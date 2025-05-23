package controller

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// ClusterState represents the current state of the cluster
type ClusterState struct {
	Nodes      map[string]*Node
	Partitions map[int]*Partition
	mu         sync.RWMutex
}

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

	node.Status = "active"
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

	partitions := make([]*Partition, 0, len(c.state.Partitions))
	for _, partition := range c.state.Partitions {
		partitions = append(partitions, partition)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(partitions)
}
