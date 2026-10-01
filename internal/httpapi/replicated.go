package httpapi

import (
	"distributedcache/internal/cache"
	"distributedcache/internal/replication"
	"net/http"
)

func NewReplicatedServer(store *cache.DurableStore, id string, peers map[string]string, vnodes int, options replication.Options) (http.Handler, error) {
	if err := store.CheckReplicaMode(); err != nil {
		return nil, err
	}
	c, err := replication.New(id, peers, vnodes, options, store)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		mux.HandleFunc(method+" /v1/kv/{key}", c.Public)
	}
	for _, method := range []string{"GET", "PUT"} {
		mux.HandleFunc(method+" /internal/replica/{key}", c.Internal)
	}
	// Never expose the old unversioned internal mutation route in replicated mode.
	mux.HandleFunc("/internal/", func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	mux.HandleFunc("GET /debug/cluster", c.Placement)
	mux.Handle("/", NewServer(store))
	return mux, nil
}
