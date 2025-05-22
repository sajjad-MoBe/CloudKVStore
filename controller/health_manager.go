package controller

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// HealthStatus represents the health status of a component
type HealthStatus struct {
	Status    string    `json:"status"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Details   any       `json:"details,omitempty"`
}

// HealthChecker defines the interface for health checks
type HealthChecker interface {
	Check(ctx context.Context) HealthStatus
}

// HealthManager manages health checks
type HealthManager struct {
	mu       sync.RWMutex
	checkers map[string]HealthChecker
	status   map[string]HealthStatus
	nodeID   string
	// Add fields for leader tracking
	leaderStatus map[int]string // partitionID -> leaderID
	stopCh       chan struct{}
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
