package httpapi

import (
	_ "embed"
	"encoding/json"
	"net/http"

	"distributedcache/internal/ring"
)

//go:embed dashboard.html
var dashboardHTML string

func registerDashboard(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(dashboardHTML))
	})
	// This endpoint only calculates placement for a hypothetical membership.
	// It neither changes cluster membership nor routes any KV operations.
	mux.HandleFunc("POST /debug/placement", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Key          string   `json:"key"`
			Nodes        []string `json:"nodes"`
			VirtualNodes int      `json:"virtual_nodes"`
			Replicas     int      `json:"replicas"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid placement request")
			return
		}
		if err := ensureOnlyOneJSONValue(decoder); err != nil {
			writeError(w, http.StatusBadRequest, "expected one JSON object")
			return
		}
		if len(request.Nodes) < 1 || len(request.Nodes) > 32 || request.VirtualNodes < 1 || request.VirtualNodes > 128 || request.Replicas < 1 || request.Replicas > 32 {
			writeError(w, http.StatusBadRequest, "use 1–32 nodes/replicas and 1–128 virtual nodes")
			return
		}
		table, err := ring.New(request.Nodes, request.VirtualNodes)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, struct {
			Mode        string          `json:"mode"`
			KeyPosition uint64          `json:"key_position,string"`
			Positions   []ring.Position `json:"positions"`
			Replicas    []string        `json:"replicas"`
		}{"placement_preview", ring.KeyPosition(request.Key), table.Positions(), table.Replicas(request.Key, request.Replicas)})
	})
}
