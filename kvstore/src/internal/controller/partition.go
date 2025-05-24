package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
)

// Partition represents a data partition
type Partition struct {
	ID       int      `json:"id"`
	Leader   string   `json:"leader"`   // Node ID of the leader
	Replicas []string `json:"replicas"` // Node IDs of replicas
	Status   string   `json:"status"`   // "healthy", "rebalancing", "failed"
}

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

// handleFailover handles failover for a partition
func (c *Controller) handleFailover(partitionID int) error {
	shared.DefaultLogger.Info("Handling failover for partition %d", partitionID)

	// Get partition info
	partition, exists := c.state.Partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// Check if current leader is healthy
	leaderStatus := c.healthManager.GetStatus()[partition.Leader]
	if leaderStatus.Status == "ok" {
		shared.DefaultLogger.Info("Leader %s is healthy for partition %d, no failover needed", partition.Leader, partitionID)
		return nil // Leader is healthy, no need for failover
	}

	shared.DefaultLogger.Warn("Leader %s is unhealthy for partition %d, initiating failover", partition.Leader, partitionID)

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
		shared.DefaultLogger.Error("No healthy replica available for failover of partition %d", partitionID)
		return errors.New("no healthy replica available for failover")
	}

	shared.DefaultLogger.Info("Promoting replica %s to leader for partition %d", newLeader, partitionID)

	// Update partition leader
	partition.Leader = newLeader
	partition.Status = "rebalancing"

	// Remove the old leader from replicas if it's still there
	newReplicas := make([]string, 0, len(partition.Replicas))
	for _, replica := range partition.Replicas {
		if replica != partition.Leader {
			newReplicas = append(newReplicas, replica)
		}
	}
	partition.Replicas = newReplicas

	// Notify load balancer of leader change
	if err := c.notifyLoadBalancer(partitionID, newLeader); err != nil {
		shared.DefaultLogger.Error("Failed to notify load balancer of leader change: %v", err)
		return fmt.Errorf("failed to notify load balancer: %v", err)
	}

	// Notify other nodes of leader change
	if err := c.notifyNodes(partitionID, newLeader); err != nil {
		shared.DefaultLogger.Error("Failed to notify nodes of leader change: %v", err)
		return fmt.Errorf("failed to notify nodes: %v", err)
	}

	// Start replication for the new leader
	replicationManager := NewReplicationManager(c)
	for _, replica := range partition.Replicas {
		if err := replicationManager.StartReplication(partitionID, newLeader, replica); err != nil {
			shared.DefaultLogger.Error("Failed to start replication for partition %d: %v", partitionID, err)
			// Continue with other replicas even if one fails
		}
	}

	shared.DefaultLogger.Info("Failover completed for partition %d, new leader is %s", partitionID, newLeader)
	return nil
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
		Status:   "active", // Set status to active by default
	}
	c.state.Partitions[partition.ID] = newPartition

	// Start replication for replicas
	replicationManager := NewReplicationManager(c)
	for _, replica := range partition.Replicas {
		if err := replicationManager.StartReplication(partition.ID, partition.Leader, replica); err != nil {
			shared.DefaultLogger.Error("Failed to start replication for partition %d: %v", partition.ID, err)
			// Continue with other replicas even if one fails
		}
	}

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

// handleGetPartition returns info for a specific partition by ID
func (c *Controller) handleGetPartition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	partitionIDStr := vars["id"]
	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		http.Error(w, "Invalid partition ID", http.StatusBadRequest)
		return
	}

	c.state.mu.RLock()
	defer c.state.mu.RUnlock()

	partition, exists := c.state.Partitions[partitionID]
	if !exists {
		http.Error(w, "Partition not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(partition)
}
