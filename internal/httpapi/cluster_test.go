package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"distributedcache/internal/cache"
	"distributedcache/internal/ring"
)

func TestThreeNodeRouting(t *testing.T) {
	ids := []string{"a", "b", "c"}
	peers := map[string]string{}
	servers := map[string]*httptest.Server{}
	stores := map[string]*cache.DurableStore{}
	for _, id := range ids {
		server := httptest.NewUnstartedServer(nil)
		servers[id] = server
		peers[id] = "http://" + server.Listener.Addr().String()
		t.Cleanup(server.Close)
		store, err := cache.OpenDurableStore(filepath.Join(t.TempDir(), "store.wal"), 10)
		if err != nil {
			t.Fatal(err)
		}
		stores[id] = store
		t.Cleanup(func() { store.Close() })
	}
	for _, id := range ids {
		handler, err := NewClusterServer(stores[id], id, peers, 64)
		if err != nil {
			t.Fatal(err)
		}
		servers[id].Config.Handler = handler
		servers[id].Start()
	}
	request := func(node, method, key, body string, want int) http.Header {
		t.Helper()
		r, err := http.NewRequest(method, peers[node]+"/v1/kv/"+url.PathEscape(key), strings.NewReader(body))
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
			t.Fatalf("%s via %s: %d %s", method, node, response.StatusCode, data)
		}
		if method == "GET" && want == 200 {
			var value getResponse
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatal(err)
			}
			if value.Key != key || value.Value != "hello" {
				t.Fatalf("wrong value: %+v", value)
			}
		}
		return response.Header
	}
	table, _ := ring.New(ids, 64)
	// Special characters must survive forwarding without double decoding.
	for _, key := range []string{"username:42", "path/with/slashes", "100% ready?", "日本語"} {
		owner, _ := table.Owner(key)
		for _, node := range ids {
			headers := request(node, "PUT", key, `{"value":"hello"}`, 204)
			if headers.Get("X-Receiving-Node") != node || headers.Get("X-Owner-Node") != owner {
				t.Fatal("wrong routing headers")
			}
		}
		for _, node := range ids {
			_, found := stores[node].Get(key)
			if found != (node == owner) {
				t.Fatalf("key stored on wrong node %s", node)
			}
			request(node, "GET", key, "", 200)
		}
		request("c", "DELETE", key, "", 204)
		request("a", "GET", key, "", 404)
	}
	// A missing owner must be an availability error, not a miss or local write.
	var key string
	for i := 0; ; i++ {
		key = fmt.Sprintf("owned-by-b-%d", i)
		owner, _ := table.Owner(key)
		if owner == "b" {
			break
		}
	}
	servers["b"].Close()
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		request("a", method, key, `{"value":"hello"}`, 503)
	}
	if _, found := stores["a"].Get(key); found {
		t.Fatal("unavailable owner caused local fallback")
	}
}

func TestClusterRejectsMisconfiguration(t *testing.T) {
	store := cache.NewMemoryStore(10)
	for _, peers := range []map[string]string{
		{}, {"b": "http://localhost:8081"}, {"a": "ftp://localhost"}, {"a": "http://localhost/path"},
	} {
		if _, err := NewClusterServer(store, "a", peers, 64); err == nil {
			t.Fatal("accepted invalid config")
		}
	}
	handler, err := NewClusterServer(store, "a", map[string]string{"a": "http://localhost:8080"}, 64)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("PUT", "/internal/kv/key", strings.NewReader(`{"value":"hello"}`)))
	if w.Code != 409 {
		t.Fatalf("unverified internal request: %d", w.Code)
	}
	if _, ok := store.Get("key"); ok {
		t.Fatal("rejected request mutated store")
	}
}
