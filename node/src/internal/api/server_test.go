package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloudkvstore/node/src/internal/storage"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	store := storage.NewMemTable()
	server := NewServer(store)
	return server, func() {}
}

func TestGetKey(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Set up test data
	key := "test-key"
	value := "test-value"
	server.store.Set(key, []byte(value))

	// Create request
	req := httptest.NewRequest("GET", "/kv/"+key, nil)
	w := httptest.NewRecorder()

	// Send request
	server.router.ServeHTTP(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["key"] != key {
		t.Errorf("Expected key %s, got %s", key, response["key"])
	}
	if response["value"] != value {
		t.Errorf("Expected value %s, got %s", value, response["value"])
	}
}

func TestGetNonExistentKey(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/kv/nonexistent", nil)
	w := httptest.NewRecorder()

	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestPutKey(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create request
	key := "test-key"
	value := "test-value"
	body := map[string]string{"value": value}
	jsonBody, _ := json.Marshal(body)

	req := httptest.NewRequest("PUT", "/kv/"+key, bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Send request
	server.router.ServeHTTP(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	// Verify value was stored
	storedValue, err := server.store.Get(key)
	if err != nil {
		t.Fatalf("Failed to get stored value: %v", err)
	}
	if string(storedValue) != value {
		t.Errorf("Expected stored value %s, got %s", value, string(storedValue))
	}
}

func TestPutInvalidRequest(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create request with invalid JSON
	req := httptest.NewRequest("PUT", "/kv/test-key", bytes.NewBufferString("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestDeleteKey(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Set up test data
	key := "test-key"
	value := "test-value"
	server.store.Set(key, []byte(value))

	// Create request
	req := httptest.NewRequest("DELETE", "/kv/"+key, nil)
	w := httptest.NewRecorder()

	// Send request
	server.router.ServeHTTP(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	// Verify key was deleted
	_, err := server.store.Get(key)
	if err != storage.ErrKeyNotFound {
		t.Errorf("Expected key to be deleted, got error: %v", err)
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("DELETE", "/kv/nonexistent", nil)
	w := httptest.NewRecorder()

	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestHealthCheck(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got %s", response["status"])
	}
}
