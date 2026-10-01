package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributedcache/internal/cache"
	"distributedcache/internal/replication"
	"distributedcache/internal/version"
)

type replicaLab struct {
	stores  map[string]*cache.DurableStore
	servers map[string]*httptest.Server
	blocked map[string]*atomic.Bool
	delay   map[string]*atomic.Int64
}

func newReplicaLab(t *testing.T, options replication.Options) *replicaLab {
	t.Helper()
	lab := &replicaLab{stores: map[string]*cache.DurableStore{}, servers: map[string]*httptest.Server{}, blocked: map[string]*atomic.Bool{}, delay: map[string]*atomic.Int64{}}
	peers := map[string]string{}
	for _, id := range []string{"a", "b", "c"} {
		server := httptest.NewUnstartedServer(nil)
		lab.servers[id] = server
		peers[id] = "http://" + server.Listener.Addr().String()
		store, err := cache.OpenDurableStore(filepath.Join(t.TempDir(), "replica.wal"), 100)
		if err != nil {
			t.Fatal(err)
		}
		lab.stores[id] = store
		lab.blocked[id] = &atomic.Bool{}
		lab.delay[id] = &atomic.Int64{}
		t.Cleanup(func() { server.Close(); store.Close() })
	}
	for _, id := range []string{"a", "b", "c"} {
		handler, err := NewReplicatedServer(lab.stores[id], id, peers, 64, options)
		if err != nil {
			t.Fatal(err)
		}
		lab.servers[id].Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if lab.blocked[id].Load() {
				http.Error(w, "offline", 503)
				return
			}
			if delay := lab.delay[id].Load(); delay > 0 {
				select {
				case <-time.After(time.Duration(delay)):
				case <-r.Context().Done():
					return
				}
			}
			handler.ServeHTTP(w, r)
		})
		lab.servers[id].Start()
	}
	return lab
}

func (l *replicaLab) request(t *testing.T, node, method, key string, body any, want int) map[string]json.RawMessage {
	t.Helper()
	var input io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		input = bytes.NewReader(data)
	}
	r, err := http.NewRequest(method, l.servers[node].URL+"/v1/kv/"+key, input)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want {
		t.Fatalf("%s via %s: got %d want %d: %s", method, node, response.StatusCode, want, data)
	}
	result := map[string]json.RawMessage{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("replicas did not converge")
}

func TestReplicatedQuorumsRepairAndTTL(t *testing.T) {
	lab := newReplicaLab(t, replication.Options{N: 3, R: 2, W: 2, Timeout: 300 * time.Millisecond})
	lab.request(t, "a", "PUT", "key", map[string]any{"value": "first"}, 204)
	eventually(t, func() bool {
		for _, store := range lab.stores {
			r, e := store.Records("key")
			if e != nil || len(r) != 1 || r[0].Value != "first" {
				return false
			}
		}
		return true
	})
	lab.blocked["c"].Store(true)
	lab.request(t, "a", "PUT", "key", map[string]any{"value": "second"}, 204)
	got := lab.request(t, "b", "GET", "key", nil, 200)
	if string(got["value"]) != `"second"` {
		t.Fatal("quorum missed acknowledged write")
	}
	lab.blocked["c"].Store(false)
	lab.request(t, "b", "GET", "key", nil, 200)
	eventually(t, func() bool {
		r, e := lab.stores["c"].Records("key")
		return e == nil && len(r) == 1 && r[0].Value == "second"
	})
	// Expiry is selected once, then persisted identically by every replica.
	lab.request(t, "a", "PUT", "ttl", map[string]any{"value": "short", "ttl_ms": 50}, 204)
	eventually(t, func() bool {
		var expiry time.Time
		for _, s := range lab.stores {
			r, e := s.Records("ttl")
			if e != nil || len(r) != 1 {
				return false
			}
			if !expiry.IsZero() && !expiry.Equal(r[0].Expires) {
				return false
			}
			expiry = r[0].Expires
		}
		return true
	})
	time.Sleep(60 * time.Millisecond)
	lab.request(t, "b", "GET", "ttl", nil, 404)
	lab.blocked["c"].Store(true)
	lab.request(t, "a", "DELETE", "key", nil, 204)
	lab.blocked["c"].Store(false)
	lab.request(t, "b", "GET", "key", nil, 404)
	eventually(t, func() bool { r, e := lab.stores["c"].Records("key"); return e == nil && len(r) == 1 && r[0].Deleted })
}

func TestConcurrentSiblingResolution(t *testing.T) {
	lab := newReplicaLab(t, replication.Options{N: 3, R: 3, W: 3, Timeout: time.Second})
	var wg sync.WaitGroup
	for _, node := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lab.request(t, node, "PUT", "key", map[string]any{"value": node, "context": map[string]uint64{}}, 204)
		}()
	}
	wg.Wait()
	got := lab.request(t, "c", "GET", "key", nil, 409)
	var siblings []version.Record
	if err := json.Unmarshal(got["siblings"], &siblings); err != nil {
		t.Fatal(err)
	}
	if len(siblings) != 2 {
		t.Fatalf("siblings: %+v", siblings)
	}
	// No-context writes must not silently collapse observed conflicts.
	lab.request(t, "a", "PUT", "key", map[string]any{"value": "accidental"}, 409)
	lab.request(t, "b", "PUT", "key", map[string]any{"value": "resolved", "context": json.RawMessage(got["context"])}, 204)
	got = lab.request(t, "a", "GET", "key", nil, 200)
	if string(got["value"]) != `"resolved"` {
		t.Fatal("resolution failed")
	}
}

func TestQuorumFailureAndSlowReplica(t *testing.T) {
	lab := newReplicaLab(t, replication.Options{N: 3, R: 2, W: 2, Timeout: 200 * time.Millisecond})
	lab.delay["c"].Store(int64(time.Second))
	start := time.Now()
	lab.request(t, "a", "PUT", "key", map[string]any{"value": "ok"}, 204)
	if time.Since(start) > 800*time.Millisecond {
		t.Fatal("quorum waited for slow replica")
	}
	lab.blocked["b"].Store(true)
	lab.request(t, "a", "GET", "key", nil, 503)
	lab.request(t, "a", "PUT", "partial", map[string]any{"value": "uncertain", "context": map[string]uint64{}}, 503)
	r, err := lab.stores["a"].Records("partial")
	if err != nil || len(r) != 1 {
		t.Fatal("expected documented partial commit on failed quorum")
	}
}

func TestReplicatedConfigurationAndInternalValidation(t *testing.T) {
	store, err := cache.OpenDurableStore(filepath.Join(t.TempDir(), "r.wal"), 10)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	peers := map[string]string{"a": "http://localhost:8080"}
	for _, options := range []replication.Options{{N: 2, R: 1, W: 1, Timeout: time.Second}, {N: 1, R: 0, W: 1, Timeout: time.Second}, {N: 1, R: 1, W: 2, Timeout: time.Second}} {
		if _, err := NewReplicatedServer(store, "a", peers, 64, options); err == nil {
			t.Fatal("invalid quorum accepted")
		}
	}
	handler, err := NewReplicatedServer(store, "a", peers, 64, replication.Defaults(1))
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("PUT", "/internal/replica/key", bytes.NewBufferString(`[]`)))
	if w.Code != 409 {
		t.Fatal("unverified internal request accepted")
	}
	for _, body := range []string{`{}`, `{"value":"x","ttl_ms":-1}`, `{"value":"x","context":{"fake":2}}`} {
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("PUT", "/v1/kv/key", bytes.NewBufferString(body)))
		if w.Code != 400 {
			t.Fatal(fmt.Sprintf("%s: %d", body, w.Code))
		}
	}
}

func TestRejectLegacyData(t *testing.T) {
	store, err := cache.OpenDurableStore(filepath.Join(t.TempDir(), "legacy.wal"), 10)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put("old", []byte("keep me"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := NewReplicatedServer(store, "a", map[string]string{"a": "http://localhost:8080"}, 64, replication.Defaults(1)); err == nil {
		t.Fatal("silently accepted legacy data")
	}
	if value, found := store.Get("old"); !found || string(value) != "keep me" {
		t.Fatal("legacy data changed")
	}
}

func TestReadRepairPreservesUnobservedSibling(t *testing.T) {
	lab := newReplicaLab(t, replication.Options{N: 3, R: 2, W: 2, Timeout: 300 * time.Millisecond})
	a, _ := version.New("a", "one", false, time.Time{}, nil)
	b, _ := version.New("b", "two", false, time.Time{}, nil)
	if err := lab.stores["a"].MergeRecords("key", []version.Record{a}); err != nil {
		t.Fatal(err)
	}
	if err := lab.stores["b"].MergeRecords("key", []version.Record{b}); err != nil {
		t.Fatal(err)
	}
	// Any first quorum can see one or both versions; late collection repairs all.
	response, err := http.Get(lab.servers["c"].URL + "/v1/kv/key")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 && response.StatusCode != 409 {
		t.Fatalf("unexpected status %d", response.StatusCode)
	}
	eventually(t, func() bool {
		for _, s := range lab.stores {
			r, e := s.Records("key")
			if e != nil || len(r) != 2 {
				return false
			}
		}
		return true
	})
}
