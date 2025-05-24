package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
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

	// Set initial status as joining
	node.Status = "joining"
	node.LastSeen = time.Now()
	c.state.Nodes[node.ID] = &node

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

	// Handle partitions before removing node
	for _, partition := range c.state.Partitions {
		if partition.Leader == nodeID {
			// If this node was a leader, promote a replica
			if len(partition.Replicas) > 0 {
				newLeader := partition.Replicas[0]
				partition.Leader = newLeader
				partition.Replicas = partition.Replicas[1:]
				partition.Status = "rebalancing"
			} else {
				partition.Status = "failed"
			}
		} else {
			// Remove this node from replicas
			newReplicas := make([]string, 0, len(partition.Replicas))
			for _, replica := range partition.Replicas {
				if replica != nodeID {
					newReplicas = append(newReplicas, replica)
				}
			}
			partition.Replicas = newReplicas
		}
	}

	delete(c.state.Nodes, nodeID)

	// Trigger rebalancing in a separate goroutine with timeout
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		rebalanceManager := NewRebalanceManager(c)
		done := make(chan error, 1)
		go func() {
			done <- rebalanceManager.RebalancePartitions()
		}()

		select {
		case err := <-done:
			if err != nil {
				shared.DefaultLogger.Error("Failed to rebalance partitions after removing node: %v", err)
			}
		case <-ctx.Done():
			shared.DefaultLogger.Error("Rebalancing timed out after removing node")
		}
	}()

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
