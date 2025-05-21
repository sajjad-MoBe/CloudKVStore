package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// Node represents a node in the cluster
type Node struct {
	ID         string      `json:"id"`
	Address    string      `json:"address"`
	Status     string      `json:"status"` // "active", "failed", "joining"
	LastSeen   time.Time   `json:"last_seen"`
	Partitions []Partition `json:"partitions"`
}

// Partition represents a data partition
type Partition struct {
	ID       int      `json:"id"`
	Leader   string   `json:"leader"`   // Node ID of the leader
	Replicas []string `json:"replicas"` // Node IDs of replicas
	Status   string   `json:"status"`   // "healthy", "rebalancing", "failed"
}

// ClusterState represents the current state of the cluster
type ClusterState struct {
	Nodes      map[string]*Node
	Partitions []Partition
	mu         sync.RWMutex
}

// Controller manages the cluster state
type Controller struct {
	state struct {
		mu         sync.RWMutex
		Nodes      map[string]*Node
		Partitions map[int]*Partition
	}
	// Add fields for partition and health management
	partitionManager *PartitionManager
	healthManager    *HealthManager
	router           *mux.Router
	stopCh           chan struct{}
	interval         time.Duration
}

// NewController creates a new controller
func NewController(partitionManager *PartitionManager, healthManager *HealthManager) *Controller {
	c := &Controller{
		partitionManager: partitionManager,
		healthManager:    healthManager,
		stopCh:           make(chan struct{}),
		router:           mux.NewRouter(),
		interval:         5 * time.Second,
	}
	c.state.Nodes = make(map[string]*Node)
	c.state.Partitions = make(map[int]*Partition)

	// Start failover monitor
	go c.startFailoverMonitor()

	c.setupRoutes()
	return c
}

// setupRoutes configures the controller's HTTP endpoints
func (c *Controller) setupRoutes() {
	// Node management
	c.router.HandleFunc("/nodes", c.handleListNodes).Methods("GET")
	c.router.HandleFunc("/nodes", c.handleAddNode).Methods("POST")
	c.router.HandleFunc("/nodes/{id}", c.handleRemoveNode).Methods("DELETE")
	c.router.HandleFunc("/nodes/{id}/status", c.handleGetNodeStatus).Methods("GET")

	// Partition management
	c.router.HandleFunc("/partitions", c.handleListPartitions).Methods("GET")
	c.router.HandleFunc("/partitions", c.handleCreatePartition).Methods("POST")
	c.router.HandleFunc("/partitions/{id}", c.handleDeletePartition).Methods("DELETE")
	c.router.HandleFunc("/partitions/{id}/leader", c.handleChangeLeader).Methods("PUT")
	c.router.HandleFunc("/partitions/{id}/replicas", c.handleUpdateReplicas).Methods("PUT")

	// Cluster operations
	c.router.HandleFunc("/cluster/rebalance", c.handleRebalance).Methods("POST")
	c.router.HandleFunc("/cluster/status", c.handleClusterStatus).Methods("GET")
}

// Start begins the controller's background tasks
func (c *Controller) Start(addr string) error {
	go c.healthCheckLoop()
	return http.ListenAndServe(addr, c.router)
}

// Stop gracefully stops the controller
func (c *Controller) Stop() {
	close(c.stopCh)
}

// healthCheckLoop periodically checks node health
func (c *Controller) healthCheckLoop() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.checkNodeHealth()
		case <-c.stopCh:
			return
		}
	}
}

// checkNodeHealth verifies the health of all nodes
func (c *Controller) checkNodeHealth() {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	for id, node := range c.state.Nodes {
		// Check if node is responsive
		if time.Since(node.LastSeen) > 30*time.Second {
			node.Status = "failed"
			c.handleNodeFailure(id)
		}
	}
}

// handleNodeFailure handles a node failure
func (c *Controller) handleNodeFailure(nodeID string) {
	// Find partitions where this node was leader
	for i, partition := range c.state.Partitions {
		if partition.Leader == nodeID {
			// Select new leader from replicas
			if len(partition.Replicas) > 0 {
				newLeader := partition.Replicas[0]
				c.state.Partitions[i].Leader = newLeader
				c.state.Partitions[i].Replicas = partition.Replicas[1:]
				c.state.Partitions[i].Status = "rebalancing"

				// TODO: Notify new leader and trigger re-replication
			} else {
				c.state.Partitions[i].Status = "failed"
			}
		}
	}
}

// HTTP Handlers

func (c *Controller) handleListNodes(w http.ResponseWriter, r *http.Request) {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()

	nodes := make([]*Node, 0, len(c.state.Nodes))
	for _, node := range c.state.Nodes {
		nodes = append(nodes, node)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}

func (c *Controller) handleAddNode(w http.ResponseWriter, r *http.Request) {
	var node Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	if _, exists := c.state.Nodes[node.ID]; exists {
		http.Error(w, "Node already exists", http.StatusConflict)
		return
	}

	node.Status = "joining"
	node.LastSeen = time.Now()
	c.state.Nodes[node.ID] = &node

	// TODO: Trigger rebalancing of partitions

	w.WriteHeader(http.StatusCreated)
}

func (c *Controller) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	if _, exists := c.state.Nodes[nodeID]; !exists {
		http.Error(w, "Node not found", http.StatusNotFound)
		return
	}

	delete(c.state.Nodes, nodeID)

	// TODO: Trigger rebalancing of partitions

	w.WriteHeader(http.StatusOK)
}

func (c *Controller) handleListPartitions(w http.ResponseWriter, r *http.Request) {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(c.state.Partitions)
}

func (c *Controller) handleCreatePartition(w http.ResponseWriter, r *http.Request) {
	var partition Partition
	if err := json.NewDecoder(r.Body).Decode(&partition); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Verify leader exists
	if _, exists := c.state.Nodes[partition.Leader]; !exists {
		http.Error(w, "Leader node not found", http.StatusBadRequest)
		return
	}

	// Verify all replicas exist
	for _, replica := range partition.Replicas {
		if _, exists := c.state.Nodes[replica]; !exists {
			http.Error(w, fmt.Sprintf("Replica node %s not found", replica), http.StatusBadRequest)
			return
		}
	}

	// Create new partition pointer
	newPartition := &Partition{
		ID:       partition.ID,
		Leader:   partition.Leader,
		Replicas: partition.Replicas,
		Status:   partition.Status,
	}
	c.state.Partitions[partition.ID] = newPartition
	w.WriteHeader(http.StatusCreated)
}

func (c *Controller) handleDeletePartition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	partitionIDStr := vars["id"]
	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		http.Error(w, "Invalid partition ID", http.StatusBadRequest)
		return
	}

	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	// Find and remove partition
	if _, exists := c.state.Partitions[partitionID]; !exists {
		http.Error(w, "Partition not found", http.StatusNotFound)
		return
	}

	delete(c.state.Partitions, partitionID)

	w.WriteHeader(http.StatusOK)
}

func (c *Controller) handleChangeLeader(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	partitionIDStr := vars["id"]
	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		http.Error(w, "Invalid partition ID", http.StatusBadRequest)
		return
	}

	var req struct {
		NewLeader string `json:"new_leader"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	// Find partition and update leader
	if _, exists := c.state.Nodes[req.NewLeader]; !exists {
		http.Error(w, "New leader node not found", http.StatusBadRequest)
		return
	}
	c.state.Partitions[partitionID].Leader = req.NewLeader
	c.state.Partitions[partitionID].Status = "rebalancing"
	w.WriteHeader(http.StatusOK)
}

func (c *Controller) handleUpdateReplicas(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	partitionIDStr := vars["id"]
	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		http.Error(w, "Invalid partition ID", http.StatusBadRequest)
		return
	}

	var req struct {
		Replicas []string `json:"replicas"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	// Find partition and update replicas
	if _, exists := c.state.Partitions[partitionID]; !exists {
		http.Error(w, "Partition not found", http.StatusNotFound)
		return
	}

	// Verify all replica nodes exist
	for _, replica := range req.Replicas {
		if _, exists := c.state.Nodes[replica]; !exists {
			http.Error(w, "Replica node not found: "+replica, http.StatusBadRequest)
			return
		}
	}
	c.state.Partitions[partitionID].Replicas = req.Replicas
	c.state.Partitions[partitionID].Status = "rebalancing"
	w.WriteHeader(http.StatusOK)
}

func (c *Controller) handleRebalance(w http.ResponseWriter, r *http.Request) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	// TODO: Implement partition rebalancing logic
	// This should redistribute partitions across nodes based on:
	// - Current node load
	// - Partition sizes
	// - Replication factor
	// - Node health

	w.WriteHeader(http.StatusOK)
}

func (c *Controller) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()

	status := struct {
		TotalNodes            int `json:"total_nodes"`
		ActiveNodes           int `json:"active_nodes"`
		FailedNodes           int `json:"failed_nodes"`
		TotalPartitions       int `json:"total_partitions"`
		HealthyPartitions     int `json:"healthy_partitions"`
		RebalancingPartitions int `json:"rebalancing_partitions"`
	}{
		TotalNodes:      len(c.state.Nodes),
		TotalPartitions: len(c.state.Partitions),
	}

	for _, node := range c.state.Nodes {
		if node.Status == "active" {
			status.ActiveNodes++
		} else if node.Status == "failed" {
			status.FailedNodes++
		}
	}

	for _, partition := range c.state.Partitions {
		if partition.Status == "healthy" {
			status.HealthyPartitions++
		} else if partition.Status == "rebalancing" {
			status.RebalancingPartitions++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// Helper functions

func (c *Controller) assignPartition(partition *Partition) error {
	// Find available nodes
	var availableNodes []string
	for id, node := range c.state.Nodes {
		if node.Status == "active" {
			availableNodes = append(availableNodes, id)
		}
	}

	if len(availableNodes) < 2 {
		return errors.New("not enough available nodes for replication")
	}

	// Assign leader and replicas
	partition.Leader = availableNodes[0]
	partition.Replicas = availableNodes[1:]
	partition.Status = "healthy"

	return nil
}

// handleGetNodeStatus handles GET /nodes/{id}/status requests
func (c *Controller) handleGetNodeStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	c.state.mu.RLock()
	defer c.state.mu.RUnlock()

	node, exists := c.state.Nodes[nodeID]
	if !exists {
		http.Error(w, "Node not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    node.Status,
		"last_seen": node.LastSeen,
	})
}

// handleFailover handles failover for a partition
func (c *Controller) handleFailover(partitionID int) error {
	// Get partition info
	partition, err := c.partitionManager.GetPartitionInfo(partitionID)
	if err != nil {
		return fmt.Errorf("failed to get partition info: %v", err)
	}

	// Check if current leader is healthy
	leaderStatus := c.healthManager.GetStatus()[partition.Leader]
	if leaderStatus.Status == "ok" {
		return nil // Leader is healthy, no need for failover
	}

	// Find healthy replica to promote
	var newLeader string
	for _, replica := range partition.Replicas {
		replicaStatus := c.healthManager.GetStatus()[replica]
		if replicaStatus.Status == "ok" {
			newLeader = replica
			break
		}
	}

	if newLeader == "" {
		return errors.New("no healthy replica available for failover")
	}

	// Update partition leader
	if err := c.partitionManager.HandleFailover(partitionID, newLeader); err != nil {
		return fmt.Errorf("failed to handle failover: %v", err)
	}

	// Notify load balancer of leader change
	if err := c.notifyLoadBalancer(partitionID, newLeader); err != nil {
		return fmt.Errorf("failed to notify load balancer: %v", err)
	}

	// Notify other nodes of leader change
	if err := c.notifyNodes(partitionID, newLeader); err != nil {
		return fmt.Errorf("failed to notify nodes: %v", err)
	}

	return nil
}

// notifyLoadBalancer notifies the load balancer of a leader change
func (c *Controller) notifyLoadBalancer(partitionID int, newLeader string) error {
	// In a real implementation, this would make an HTTP request to the load balancer
	// to update its routing table. For now, we'll just log it.
	fmt.Printf("Notifying load balancer: partition %d leader changed to %s\n", partitionID, newLeader)
	return nil
}

// notifyNodes notifies other nodes of a leader change
func (c *Controller) notifyNodes(partitionID int, newLeader string) error {
	// In a real implementation, this would make HTTP requests to all nodes
	// to update their partition information. For now, we'll just log it.
	fmt.Printf("Notifying nodes: partition %d leader changed to %s\n", partitionID, newLeader)
	return nil
}

// startFailoverMonitor starts monitoring for failover conditions
func (c *Controller) startFailoverMonitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check all partitions for failover conditions
			for partitionID := range c.state.Partitions {
				if err := c.handleFailover(partitionID); err != nil {
					fmt.Printf("Failed to handle failover for partition %d: %v\n", partitionID, err)
				}
			}
		case <-c.stopCh:
			return
		}
	}
}

// GetPartitions returns all partition IDs
func (c *Controller) GetPartitions() map[int]*PartitionData {
	return c.partitionManager.GetPartitions()
}
