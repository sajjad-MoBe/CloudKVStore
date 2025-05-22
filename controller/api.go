package controller

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/shared"
	"net/http"
)

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

// HTTP Handlers
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
