package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

// MockNode represents a mock node for testing
type MockNode struct {
	address string
	server  *http.Server
	wal     map[int][]wal.LogEntry
	kvStore map[string]string
}

// NewMockNode creates a new mock node
func NewMockNode(address string) *MockNode {
	return &MockNode{
		address: address,
		wal:     make(map[int][]wal.LogEntry),
		kvStore: make(map[string]string),
	}
}

// Start starts the mock node server
func (n *MockNode) Start() {
	router := mux.NewRouter()

	// Add WAL endpoints
	router.HandleFunc("/wal/entries", func(w http.ResponseWriter, r *http.Request) {
		partitionID := r.URL.Query().Get("partition")
		id, err := strconv.Atoi(partitionID)
		if err != nil {
			http.Error(w, "invalid partition ID", http.StatusBadRequest)
			return
		}

		// Initialize empty slice if partition doesn't exist
		if _, exists := n.wal[id]; !exists {
			n.wal[id] = make([]wal.LogEntry, 0)
		}

		// Add some test entries if the partition is empty
		if len(n.wal[id]) == 0 {
			n.wal[id] = []wal.LogEntry{
				{
					Timestamp: time.Now(),
					Operation: "SET",
					Key:       "test-key-1",
					Value:     "test-value-1",
					Partition: id,
				},
				{
					Timestamp: time.Now(),
					Operation: "SET",
					Key:       "test-key-2",
					Value:     "test-value-2",
					Partition: id,
				},
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(n.wal[id])
	}).Methods("GET")

	router.HandleFunc("/wal/apply", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			PartitionID int            `json:"partition_id"`
			Entries     []wal.LogEntry `json:"entries"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		n.wal[req.PartitionID] = req.Entries
		w.WriteHeader(http.StatusOK)
	}).Methods("POST")

	// Add KV store endpoints
	router.HandleFunc("/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case "GET":
			value, exists := n.kvStore[key]
			if !exists {
				value = fmt.Sprintf("test-value-for-%s", key)
				n.kvStore[key] = value
			}
			json.NewEncoder(w).Encode(map[string]string{"value": value})
		case "PUT":
			var req struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			n.kvStore[key] = req.Value
			w.WriteHeader(http.StatusOK)
		}
	}).Methods("GET", "PUT")

	// Add keys endpoint for replication verification
	router.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		// For testing purposes, return all keys
		keys := make([]string, 0, len(n.kvStore))
		for k := range n.kvStore {
			keys = append(keys, k)
		}
		json.NewEncoder(w).Encode(keys)
	}).Methods("GET")

	// Add replication status endpoint
	router.HandleFunc("/replication/status", func(w http.ResponseWriter, r *http.Request) {
		partitionID := r.URL.Query().Get("partition")
		if partitionID == "" {
			http.Error(w, "partition parameter is required", http.StatusBadRequest)
			return
		}

		// For testing purposes, return streaming status with a consistent LastSent value
		// This ensures replication verification works correctly
		status := struct {
			Status      string        `json:"status"`
			LastSent    int64         `json:"last_sent"`
			LastAckTime time.Time     `json:"last_ack_time"`
			NeedsSync   bool          `json:"needs_sync"`
			Lag         time.Duration `json:"lag"`
		}{
			Status:      "streaming",
			LastSent:    1, // Use a consistent value for testing
			LastAckTime: time.Now(),
			NeedsSync:   false,
			Lag:         0,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}).Methods("GET")

	n.server = &http.Server{
		Addr:    strings.TrimPrefix(n.address, "http://"),
		Handler: router,
	}

	go n.server.ListenAndServe()
}

// Stop stops the mock node server
func (n *MockNode) Stop() {
	if n.server != nil {
		n.server.Shutdown(context.Background())
	}
}
