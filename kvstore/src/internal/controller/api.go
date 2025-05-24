package controller

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// setupRoutes is defined in health-helper.go

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
	err := json.NewEncoder(w).Encode(status)
	if err != nil {
		return
	}
}

// handleGetValue handles GET requests for key-value pairs
func (c *Controller) handleGetValue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := c.partitionManager.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"value": value})
}

// handleSetValue handles PUT requests for key-value pairs
func (c *Controller) handleSetValue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	var data struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := c.partitionManager.Set(key, data.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleDeleteValue handles DELETE requests for key-value pairs
func (c *Controller) handleDeleteValue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	if err := c.partitionManager.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
