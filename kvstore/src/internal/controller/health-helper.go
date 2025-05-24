package controller

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
)

// healthCheckLoop periodically checks node health
func (c *Controller) healthCheckLoop() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.printNodeStatuses() // Print node statuses before health check
			c.checkNodeHealth()
		case <-c.stopCh:
			return
		}
	}
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
					// Suppress error by logging at debug level
					// fmt.Printf("Failed to handle failover for partition %d: %v\n", partitionID, err)
					// Uncomment the line below if you have a logger
					// log.Printf("DEBUG: Failed to handle failover for partition %d: %v", partitionID, err)
				}
			}
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
		switch node.Status {
		case "joining":
			// If node is joining, mark it as active
			node.Status = "active"
			// Also mark as healthy in health manager
			c.healthManager.SetNodeStatus(id, "ok", "Node is active")
			// Trigger rebalancing in a separate goroutine to avoid deadlock
			go func(nodeID string) {
				rebalanceManager := NewRebalanceManager(c)
				if err := rebalanceManager.RebalancePartitions(); err != nil {
					shared.DefaultLogger.Error("Failed to rebalance partitions after node %s became active: %v", nodeID, err)
				}
			}(id)
		case "active":
			// Check if node is responsive by making a heartbeat request
			url := fmt.Sprintf("http://%s/heartbeat", node.Address)
			resp, err := http.Post(url, "application/json", nil)
			if err != nil || resp.StatusCode != http.StatusOK {
				shared.DefaultLogger.Warn("Node %s is not responding to heartbeat, marking as failed", id)
				node.Status = "failed"
				// Mark as unhealthy in health manager
				c.healthManager.SetNodeStatus(id, "error", "Node failed health check")
				// Handle node failure in a separate goroutine to avoid deadlock
				go c.handleNodeFailure(id)
			} else {
				resp.Body.Close()
				node.LastSeen = time.Now()
				// Node is active and responsive, mark as healthy
				c.healthManager.SetNodeStatus(id, "ok", "Node is active")
			}
		case "failed":
			// Mark as unhealthy in health manager
			c.healthManager.SetNodeStatus(id, "error", "Node is failed")
		}
	}
}

// handleNodeFailure handles a node failure
func (c *Controller) handleNodeFailure(nodeID string) {
	shared.DefaultLogger.Info("Handling failure of node %s", nodeID)

	// Find partitions where this node was leader
	for partitionID := range c.state.Partitions {
		// Call handleFailover for each partition
		if err := c.handleFailover(partitionID); err != nil {
			shared.DefaultLogger.Error("Failed to handle failover for partition %d: %v", partitionID, err)
		}
	}
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

// handleHeartbeat updates the LastSeen timestamp for a node
func (c *Controller) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	node, exists := c.state.Nodes[nodeID]
	if !exists {
		http.Error(w, "Node not found", http.StatusNotFound)
		return
	}

	node.LastSeen = time.Now()
	shared.DefaultLogger.Info("Received heartbeat from node %s at %v", nodeID, node.LastSeen)
	w.WriteHeader(http.StatusOK)
}

// printNodeStatuses logs the status of all nodes
func (c *Controller) printNodeStatuses() {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	for id, node := range c.state.Nodes {
		shared.DefaultLogger.Info("Node %s: status=%s, last_seen=%v", id, node.Status, node.LastSeen)
	}
}

// In setupRoutes, add the heartbeat endpoint
func (c *Controller) setupRoutes() {
	// Node management
	c.router.HandleFunc("/nodes", c.handleListNodes).Methods("GET")
	c.router.HandleFunc("/nodes", c.handleAddNode).Methods("POST")
	c.router.HandleFunc("/nodes/{id}", c.handleRemoveNode).Methods("DELETE")
	c.router.HandleFunc("/nodes/{id}/status", c.handleGetNodeStatus).Methods("GET")
	c.router.HandleFunc("/nodes/{id}/heartbeat", c.handleHeartbeat).Methods("POST")

	// Partition management
	c.router.HandleFunc("/partitions", c.handleListPartitions).Methods("GET")
	c.router.HandleFunc("/partitions", c.handleCreatePartition).Methods("POST")
	c.router.HandleFunc("/partitions/{id}", c.handleGetPartition).Methods("GET")
	c.router.HandleFunc("/partitions/{id}", c.handleDeletePartition).Methods("DELETE")
	c.router.HandleFunc("/partitions/{id}/leader", c.handleChangeLeader).Methods("PUT")
	c.router.HandleFunc("/partitions/{id}/replicas", c.handleUpdateReplicas).Methods("PUT")

	// Cluster operations
	c.router.HandleFunc("/cluster/rebalance", c.handleRebalance).Methods("POST")
	c.router.HandleFunc("/cluster/status", c.handleClusterStatus).Methods("GET")

	// Key-value store operations
	c.router.HandleFunc("/kv/{key}", c.handleGetValue).Methods("GET")
	c.router.HandleFunc("/kv/{key}", c.handleSetValue).Methods("PUT")
	c.router.HandleFunc("/kv/{key}", c.handleDeleteValue).Methods("DELETE")
}
