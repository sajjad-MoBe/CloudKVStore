package node

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

// Node represents a single node in the cluster
type Node struct {
	ID               string
	Address          string
	ControllerURL    string
	Status           string
	Partitions       map[int]*partition.PartitionData
	mu               sync.RWMutex
	router           *mux.Router
	stopCh           chan struct{}
	interval         time.Duration
	partitionManager *partition.PartitionManager
}

// NewNode creates a new node instance
func NewNode(id, address, controllerURL string) *Node {
	config := partition.PartitionConfig{
		MaxMemTableSize: 1024 * 1024, // 1MB
		WALConfig: wal.WALConfig{
			MaxFileSize: 10 * 1024 * 1024, // 10MB
		},
	}

	healthManager := shared.NewHealthManager(id)
	partitionManager := partition.NewPartitionManager(config, healthManager)

	n := &Node{
		ID:               id,
		Address:          address,
		ControllerURL:    controllerURL,
		Status:           "active",
		Partitions:       make(map[int]*partition.PartitionData),
		router:           mux.NewRouter(),
		stopCh:           make(chan struct{}),
		interval:         5 * time.Second,
		partitionManager: partitionManager,
	}
	n.setupRoutes()
	return n
}

// Start begins the node's background tasks and HTTP server
func (n *Node) Start() error {
	log.Printf("Starting node %s on address %s", n.ID, n.Address)

	// Create necessary directories
	if err := os.MkdirAll("wal", 0755); err != nil {
		log.Printf("Failed to create wal directory: %v", err)
		return err
	}

	go n.heartbeatLoop()
	log.Printf("HTTP server starting on %s", n.Address)

	// Create a new server with the router
	server := &http.Server{
		Addr:    n.Address,
		Handler: n.router,
	}

	log.Printf("Server configured with address: %s", server.Addr)

	// Start the server
	log.Printf("Attempting to start HTTP server...")
	err := server.ListenAndServe()
	if err != nil {
		log.Printf("HTTP server error: %v", err)
	}
	return err
}

// Stop gracefully stops the node
func (n *Node) Stop() {
	close(n.stopCh)
}

// setupRoutes configures the HTTP routes for the node
func (n *Node) setupRoutes() {
	api := n.router.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/health", n.handleHealth).Methods("GET")
	api.HandleFunc("/kv/{key}", n.handleGetValue).Methods("GET")
	api.HandleFunc("/kv/{key}", n.handleSetValue).Methods("PUT")
	api.HandleFunc("/kv/{key}", n.handleDeleteValue).Methods("DELETE")
	api.HandleFunc("/wal/entries", n.handleWALEntries).Methods("GET")
	api.HandleFunc("/wal/apply", n.handleWALApply).Methods("POST")
}

// heartbeatLoop sends periodic heartbeats to the controller
func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.sendHeartbeat()
		case <-n.stopCh:
			return
		}
	}
}

// sendHeartbeat sends a heartbeat to the controller
func (n *Node) sendHeartbeat() {
	// TODO: Implement heartbeat logic
	// This should send a POST request to the controller's heartbeat endpoint
	// with the node's status and partition information
}

// handleHealth handles health check requests
func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	status := struct {
		Status     string `json:"status"`
		Partitions int    `json:"partitions"`
	}{
		Status:     n.Status,
		Partitions: len(n.Partitions),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleGetValue handles GET requests for key-value pairs
func (n *Node) handleGetValue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	log.Printf("GET request received for key: %s", key)

	value, err := n.partitionManager.Get(key)
	if err != nil {
		log.Printf("Error retrieving key %s: %v", key, err)
		response := struct {
			Success bool   `json:"success"`
			Error   string `json:"error"`
		}{
			Success: false,
			Error:   err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response)
		return
	}

	log.Printf("Successfully retrieved key %s with value: %s", key, value)
	response := struct {
		Success bool   `json:"success"`
		Value   string `json:"value"`
	}{
		Success: true,
		Value:   value,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleSetValue handles PUT requests for key-value pairs
func (n *Node) handleSetValue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	log.Printf("PUT request received for key: %s", key)

	var data struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		log.Printf("Error decoding request body for key %s: %v", key, err)
		response := struct {
			Success bool   `json:"success"`
			Error   string `json:"error"`
		}{
			Success: false,
			Error:   "Invalid request body",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	if err := n.partitionManager.Set(key, data.Value); err != nil {
		log.Printf("Error setting key %s: %v", key, err)
		response := struct {
			Success bool   `json:"success"`
			Error   string `json:"error"`
		}{
			Success: false,
			Error:   err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	log.Printf("Successfully set key %s with value: %s", key, data.Value)
	response := struct {
		Success bool `json:"success"`
	}{
		Success: true,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleDeleteValue handles DELETE requests for key-value pairs
func (n *Node) handleDeleteValue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	log.Printf("DELETE request received for key: %s", key)

	if err := n.partitionManager.Delete(key); err != nil {
		log.Printf("Error deleting key %s: %v", key, err)
		response := struct {
			Success bool   `json:"success"`
			Error   string `json:"error"`
		}{
			Success: false,
			Error:   err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	log.Printf("Successfully deleted key: %s", key)
	response := struct {
		Success bool `json:"success"`
	}{
		Success: true,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleWALEntries handles GET requests for WAL entries for a specific partition
func (n *Node) handleWALEntries(w http.ResponseWriter, r *http.Request) {
	partitionID := r.URL.Query().Get("partition")
	id, err := strconv.Atoi(partitionID)
	if err != nil {
		http.Error(w, "invalid partition ID", http.StatusBadRequest)
		return
	}

	// Get WAL entries for the specified partition
	entries := n.partitionManager.GetWALEntries(id)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

// handleWALApply handles POST requests to apply WAL entries to a partition
func (n *Node) handleWALApply(w http.ResponseWriter, r *http.Request) {
	type applyRequest struct {
		PartitionID int            `json:"partition_id"`
		Entries     []wal.LogEntry `json:"entries"`
	}
	var req applyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Apply entries to the partition
	// (You may want to add logic to actually apply these to the partition's WAL and state)
	// For now, just log and return OK
	// TODO: Implement actual application logic
	_ = req.PartitionID
	_ = req.Entries

	w.WriteHeader(http.StatusOK)
}

// AddPartition adds a partition to the node
func (n *Node) AddPartition(p *partition.PartitionData) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Partitions[p.ID] = p
}

// RemovePartition removes a partition from the node
func (n *Node) RemovePartition(partitionID int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.Partitions, partitionID)
}
