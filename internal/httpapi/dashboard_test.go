package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"distributedcache/internal/cache"
	"distributedcache/internal/ring"
)

func TestDashboardAndPlacement(t *testing.T) {
	store := cache.NewMemoryStore(10)
	server := NewServer(store)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "text/html") || !strings.Contains(w.Body.String(), "Where does a key go?") {
		t.Fatal("dashboard not served")
	}
	w = httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest("POST", "/debug/placement", strings.NewReader(`{"key":"username:42","nodes":["a","b","c"],"virtual_nodes":4,"replicas":3}`)))
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var got struct {
		Mode      string          `json:"mode"`
		Positions []ring.Position `json:"positions"`
		Replicas  []string        `json:"replicas"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	r, err := ring.New([]string{"a", "b", "c"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := r.Owner("username:42")
	if got.Mode != "placement_preview" || len(got.Positions) != 12 || len(got.Replicas) != 3 || got.Replicas[0] != owner {
		t.Fatalf("unexpected placement: %+v", got)
	}
	if _, found := store.Get("username:42"); found {
		t.Fatal("preview changed storage")
	}
}

func TestPlacementValidation(t *testing.T) {
	server := NewServer(cache.NewMemoryStore(10))
	for _, body := range []string{
		`{`, `{}`, `{"nodes":["a"],"virtual_nodes":129,"replicas":1}`,
		`{"nodes":["a","a"],"virtual_nodes":1,"replicas":1}`,
		`{"nodes":["a"],"virtual_nodes":1,"replicas":0}`,
		`{"nodes":["a"],"virtual_nodes":1,"replicas":1} {}`,
	} {
		w := httptest.NewRecorder()
		server.ServeHTTP(w, httptest.NewRequest("POST", "/debug/placement", strings.NewReader(body)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: %d", body, w.Code)
		}
	}
}
