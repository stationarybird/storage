package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"distributedcache/internal/cache"
)

func TestHealthz(t *testing.T) {
	server := NewServer(cache.NewMemoryStore(10))
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestKVHandlers(t *testing.T) {
	server := NewServer(cache.NewMemoryStore(10))

	put := httptest.NewRequest(http.MethodPut, "/v1/kv/greeting", bytes.NewBufferString(`{"value":"hello","ttl_ms":0}`))
	putRecorder := httptest.NewRecorder()
	server.ServeHTTP(putRecorder, put)
	if putRecorder.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", putRecorder.Code, http.StatusNoContent)
	}

	getRecorder := httptest.NewRecorder()
	server.ServeHTTP(getRecorder, httptest.NewRequest(http.MethodGet, "/v1/kv/greeting", nil))
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", getRecorder.Code, http.StatusOK)
	}
	var response getResponse
	if err := json.NewDecoder(getRecorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}
	if response.Key != "greeting" || response.Value != "hello" {
		t.Fatalf("GET response = %#v, want greeting/hello", response)
	}

	deleteRecorder := httptest.NewRecorder()
	server.ServeHTTP(deleteRecorder, httptest.NewRequest(http.MethodDelete, "/v1/kv/greeting", nil))
	if deleteRecorder.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", deleteRecorder.Code, http.StatusNoContent)
	}

	missingRecorder := httptest.NewRecorder()
	server.ServeHTTP(missingRecorder, httptest.NewRequest(http.MethodGet, "/v1/kv/greeting", nil))
	if missingRecorder.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d, want %d", missingRecorder.Code, http.StatusNotFound)
	}
}

func TestPutHandlerRejectsInvalidRequests(t *testing.T) {
	server := NewServer(cache.NewMemoryStore(10))

	for _, body := range []string{
		`not json`,
		`{}`,
		`{"value":"x","ttl_ms":-1}`,
		`{"value":"x","unknown":true}`,
		`{"value":"x"} {"value":"second"}`,
	} {
		t.Run(body, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPut, "/v1/kv/key", bytes.NewBufferString(body))
			server.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("PUT status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body)
			}
		})
	}
}
