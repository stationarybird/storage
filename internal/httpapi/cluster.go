package httpapi

import (
	"net/http"

	"distributedcache/internal/cache"
	"distributedcache/internal/routing"
)

func NewClusterServer(store cache.Store, nodeID string, peers map[string]string, virtualNodes int) (http.Handler, error) {
	local := NewServer(store)
	coordinator, err := routing.New(nodeID, peers, virtualNodes, local)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		mux.HandleFunc(method+" /v1/kv/{key}", coordinator.Public)
		mux.HandleFunc(method+" /internal/kv/{key}", coordinator.Internal)
	}
	mux.HandleFunc("GET /debug/cluster", coordinator.Placement)
	mux.Handle("/", local)
	return mux, nil
}
