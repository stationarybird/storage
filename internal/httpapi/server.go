// Package httpapi exposes the cache through the node's HTTP API.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"distributedcache/internal/cache"
)

type putRequest struct {
	Value *string `json:"value"`
	TTLMS int64   `json:"ttl_ms"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func NewServer(store cache.Store) http.Handler {
	mux := http.NewServeMux()
	registerDashboard(mux)

	// Keep this endpoint independent of storage so orchestration can distinguish
	// a running process from later readiness/membership checks.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("PUT /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		var request putRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "request body must be valid JSON")
			return
		}
		if err := ensureOnlyOneJSONValue(decoder); err != nil {
			writeError(w, http.StatusBadRequest, "request body must contain one JSON object")
			return
		}
		if request.Value == nil {
			writeError(w, http.StatusBadRequest, "value is required")
			return
		}
		if request.TTLMS < 0 || request.TTLMS > int64((time.Duration(1<<63-1))/time.Millisecond) {
			writeError(w, http.StatusBadRequest, "ttl_ms must be a non-negative duration")
			return
		}

		if err := store.Put(key, []byte(*request.Value), time.Duration(request.TTLMS)*time.Millisecond); err != nil {
			writeStoreError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		value, found := store.Get(key)
		if !found {
			writeError(w, http.StatusNotFound, "key not found")
			return
		}
		writeJSON(w, http.StatusOK, getResponse{Key: key, Value: string(value)})
	})
	mux.HandleFunc("DELETE /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if err := store.Delete(r.PathValue("key")); err != nil {
			writeStoreError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func writeStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, cache.ErrCapacity) {
		writeError(w, http.StatusInsufficientStorage, err.Error())
		return
	}
	writeError(w, http.StatusServiceUnavailable, "storage write failed")
}

func ensureOnlyOneJSONValue(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
