package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	kvErr "github.com/sajjad-MoBe/CloudKVStore/node/src/internal/errors"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/storage"

	"github.com/gorilla/mux"
)

// Handler handles HTTP requests for the key-value store
type Handler struct {
	store         *storage.MemTable
	metrics       *APIMetrics
	authManager   *AuthManager
	healthManager *HealthManager
	// Add partition manager
	partitionManager *PartitionManager
}

// APIMetrics tracks API metrics
type APIMetrics struct {
	mu sync.RWMutex
	// Request counts
	GetCount    int64
	SetCount    int64
	DeleteCount int64
	ListCount   int64
	// Error counts
	ErrorCount int64
	// Latency metrics
	GetLatency    time.Duration
	SetLatency    time.Duration
	DeleteLatency time.Duration
}

// NewHandler creates a new API handler
func NewHandler(store *storage.MemTable, authManager *AuthManager, partitionManager *PartitionManager) *Handler {
	return &Handler{
		store:            store,
		metrics:          &APIMetrics{},
		authManager:      authManager,
		partitionManager: partitionManager,
	}
}

// GetValue handles GET requests for retrieving values
func (h *Handler) GetValue(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		h.metrics.mu.Lock()
		h.metrics.GetCount++
		h.metrics.GetLatency += time.Since(start)
		h.metrics.mu.Unlock()
	}()

	// Extract key from URL
	key := strings.TrimPrefix(r.URL.Path, "/api/v1/keys/")
	if key == "" {
		h.handleError(w,
			kvErr.New(kvErr.ErrorTypeInvalidInput, "key is required", nil),
			http.StatusBadRequest,
		)
		return
	}

	// Find partition for the key
	partitionID := h.partitionManager.GetPartitionForKey(key)
	partition, err := h.partitionManager.GetPartitionInfo(partitionID)
	if err != nil {
		h.handleError(w, err, http.StatusInternalServerError)
		return
	}

	// Get value from partition
	value, err := h.partitionManager.Read(partitionID, key)
	if err != nil {
		h.metrics.mu.Lock()
		h.metrics.ErrorCount++
		h.metrics.mu.Unlock()
		var notFound *storage.ErrKeyNotFound
		if errors.As(err, &notFound) {
			h.handleError(w, err, http.StatusNotFound)
			return
		}
		h.handleError(w, err, http.StatusInternalServerError)
		return
	}

	// Return response
	response := map[string]interface{}{
		"key":       key,
		"value":     string(value),
		"partition": partitionID,
		"leader":    partition.Leader,
	}
	h.writeJSON(w, response, http.StatusOK)
}

// SetValue handles PUT requests for setting values
func (h *Handler) SetValue(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		h.metrics.mu.Lock()
		h.metrics.SetCount++
		h.metrics.SetLatency += time.Since(start)
		h.metrics.mu.Unlock()
	}()

	// Extract key from URL
	key := strings.TrimPrefix(r.URL.Path, "/api/v1/keys/")
	if key == "" {
		h.handleError(w,
			kvErr.New(kvErr.ErrorTypeInvalidInput, "key is required", nil),
			http.StatusBadRequest)
		return
	}

	// Parse request body
	var request struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		h.handleError(w,
			kvErr.New(kvErr.ErrorTypeInvalidInput, "invalid request body", err),
			http.StatusBadRequest,
		)
		return
	}

	// Find partition for the key
	partitionID := h.partitionManager.GetPartitionForKey(key)
	partition, err := h.partitionManager.GetPartitionInfo(partitionID)
	if err != nil {
		h.handleError(w, err, http.StatusInternalServerError)
		return
	}

	// Check if this node is the leader
	if partition.Leader != h.healthManager.GetNodeID() {
		h.handleError(w,
			kvErr.New(kvErr.ErrorTypeNotLeader, "not the leader for this partition", nil),
			http.StatusServiceUnavailable)
		return
	}

	// Write to partition (includes WAL and replication)
	if err := h.partitionManager.Write(partitionID, key, []byte(request.Value)); err != nil {
		h.metrics.mu.Lock()
		h.metrics.ErrorCount++
		h.metrics.mu.Unlock()
		h.handleError(w, err, http.StatusInternalServerError)
		return
	}

	// Return success response
	response := map[string]interface{}{
		"success":   true,
		"partition": partitionID,
		"leader":    partition.Leader,
	}
	h.writeJSON(w, response, http.StatusOK)
}

// DeleteValue handles DELETE requests for removing values
func (h *Handler) DeleteValue(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		h.metrics.mu.Lock()
		h.metrics.DeleteCount++
		h.metrics.DeleteLatency += time.Since(start)
		h.metrics.mu.Unlock()
	}()

	// Extract key from URL
	key := strings.TrimPrefix(r.URL.Path, "/api/v1/keys/")
	if key == "" {
		h.handleError(w,
			kvErr.New(kvErr.ErrorTypeInvalidInput, "key is required", nil),
			http.StatusBadRequest,
		)
		return
	}

	// Find partition for the key
	partitionID := h.partitionManager.GetPartitionForKey(key)
	partition, err := h.partitionManager.GetPartitionInfo(partitionID)
	if err != nil {
		h.handleError(w, err, http.StatusInternalServerError)
		return
	}

	// Check if this node is the leader
	if partition.Leader != h.healthManager.GetNodeID() {
		h.handleError(w,
			kvErr.New(kvErr.ErrorTypeNotLeader, "not the leader for this partition", nil),
			http.StatusServiceUnavailable)
		return
	}

	// Delete from partition (includes WAL and replication)
	if err := h.partitionManager.Write(partitionID, key, nil); err != nil {
		h.metrics.mu.Lock()
		h.metrics.ErrorCount++
		h.metrics.mu.Unlock()
		var notFound *storage.ErrKeyNotFound
		if errors.As(err, &notFound) {
			h.handleError(w, err, http.StatusNotFound)
			return
		}
		h.handleError(w, err, http.StatusInternalServerError)
		return
	}

	// Return success response
	response := map[string]interface{}{
		"success":   true,
		"partition": partitionID,
		"leader":    partition.Leader,
	}
	h.writeJSON(w, response, http.StatusOK)
}

// ListValues handles GET requests for listing all values
func (h *Handler) ListValues(w http.ResponseWriter, r *http.Request) {
	h.metrics.mu.Lock()
	h.metrics.ListCount++
	h.metrics.mu.Unlock()

	// Get prefix from query parameter
	prefix := r.URL.Query().Get("prefix")

	// Get all key-value pairs
	pairs := h.store.GetAll()

	// Filter by prefix if specified
	filtered := make(map[string]string)
	for k, v := range pairs {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			filtered[k] = string(v)
		}
	}

	// Return response
	response := map[string]interface{}{
		"items": filtered,
	}
	h.writeJSON(w, response, http.StatusOK)
}

// GetMetrics returns the current API metrics
func (h *Handler) GetMetrics() *APIMetrics {
	h.metrics.mu.RLock()
	defer h.metrics.mu.RUnlock()
	return h.metrics
}

// handleError writes an error response
func (h *Handler) handleError(w http.ResponseWriter, err error, status int) {
	response := map[string]interface{}{
		"error": err.Error(),
	}
	h.writeJSON(w, response, status)
}

// writeJSON writes a JSON response
func (h *Handler) writeJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

// HealthCheck handles health check requests
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status": "ok",
		"time":   time.Now().UTC(),
	}
	h.writeJSON(w, response, http.StatusOK)
}

// CreateUser handles user creation requests
func (h *Handler) CreateUser(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ID    string   `json:"id"`
		Roles []string `json:"roles"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		h.handleError(w,
			kvErr.New(kvErr.ErrorTypeInvalidInput, "invalid request body", err),
			http.StatusBadRequest,
		)
		return
	}

	// Convert string roles to Role type
	roles := make([]Role, len(request.Roles))
	for i, r := range request.Roles {
		roles[i] = Role(r)
	}

	user, err := h.authManager.CreateUser(request.ID, roles)
	if err != nil {
		h.handleError(w, err, http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"id":      user.ID,
		"roles":   user.Roles,
		"api_key": user.APIKey,
	}
	h.writeJSON(w, response, http.StatusCreated)
}

// DeleteUser handles user deletion requests
func (h *Handler) DeleteUser(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		h.handleError(w,
			kvErr.New(kvErr.ErrorTypeInvalidInput, "user ID is required", nil),
			http.StatusBadRequest,
		)
		return
	}

	// TODO: Implement user deletion
	// This would involve:
	// 1. Finding the user's API key
	// 2. Deleting the API key from the store
	// 3. Removing the user from memory

	response := map[string]interface{}{
		"success": true,
	}
	h.writeJSON(w, response, http.StatusOK)
}

func (h *Handler) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	h.healthManager.HealthCheckHandler(w, r)
}

// In handler.go:
func (h *Handler) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	m := h.GetMetrics()
	h.writeJSON(w, m, http.StatusOK)
}
