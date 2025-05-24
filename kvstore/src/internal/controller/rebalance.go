package controller

import (
	"fmt"
	"sort"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
)

// NodeLoad represents the load on a node
type NodeLoad struct {
	NodeID    string
	Load      float64
	Partition int
}

// RebalanceManager handles partition rebalancing
type RebalanceManager struct {
	controller *Controller
	logger     *shared.Logger
	metrics    *shared.MetricsCollector
}

// NewRebalanceManager creates a new rebalance manager
func NewRebalanceManager(controller *Controller) *RebalanceManager {
	return &RebalanceManager{
		controller: controller,
		logger:     shared.DefaultLogger,
		metrics:    shared.DefaultCollector,
	}
}

// RebalancePartitions rebalances partitions across nodes
func (rm *RebalanceManager) RebalancePartitions() error {
	rm.logger.Info("Starting partition rebalancing")
	startTime := time.Now()

	// Get current node loads
	nodeLoads := rm.calculateNodeLoads()

	// Sort nodes by load
	sort.Slice(nodeLoads, func(i, j int) bool {
		return nodeLoads[i].Load < nodeLoads[j].Load
	})

	// Calculate target load per node
	totalLoad := 0.0
	for _, load := range nodeLoads {
		totalLoad += load.Load
	}
	targetLoadPerNode := totalLoad / float64(len(nodeLoads))

	// Find overloaded and underloaded nodes
	var overloaded, underloaded []NodeLoad
	for _, load := range nodeLoads {
		if load.Load > targetLoadPerNode*1.1 { // 10% threshold
			overloaded = append(overloaded, load)
		} else if load.Load < targetLoadPerNode*0.9 { // 10% threshold
			underloaded = append(underloaded, load)
		}
	}

	// Move partitions from overloaded to underloaded nodes
	for _, over := range overloaded {
		for _, under := range underloaded {
			if over.Load <= targetLoadPerNode {
				break
			}

			// Calculate how much load to move
			loadToMove := min(over.Load-targetLoadPerNode, targetLoadPerNode-under.Load)
			if loadToMove <= 0 {
				continue
			}

			// Find a partition to move
			partition := rm.findPartitionToMove(over.NodeID, loadToMove)
			if partition == nil {
				continue
			}

			// Move the partition
			if err := rm.movePartition(partition.ID, over.NodeID, under.NodeID); err != nil {
				rm.logger.Error("Failed to move partition %d: %v", partition.ID, err)
				continue
			}

			// Update loads
			over.Load -= loadToMove
			under.Load += loadToMove
		}
	}

	// Record metrics
	duration := time.Since(startTime).Seconds()
	rm.metrics.RecordMetric(shared.MetricRebalanceCount, shared.Counter, 1, nil)
	rm.metrics.RecordMetric(shared.MetricOperationLatency, shared.Histogram, duration, map[string]string{
		"operation": "rebalance",
	})

	rm.logger.Info("Partition rebalancing completed in %v", duration)
	return nil
}

// calculateNodeLoads calculates the current load on each node
func (rm *RebalanceManager) calculateNodeLoads() []NodeLoad {
	var loads []NodeLoad

	rm.controller.state.mu.RLock()
	defer rm.controller.state.mu.RUnlock()

	for nodeID, node := range rm.controller.state.Nodes {
		if node.Status != "active" {
			continue
		}

		// Calculate load based on:
		// 1. Number of partitions
		// 2. Size of partitions
		// 3. Number of operations
		load := 0.0
		for _, partition := range rm.controller.state.Partitions {
			if partition.Leader == nodeID {
				load += 2.0 // Leader has higher load
			} else {
				for _, replica := range partition.Replicas {
					if replica == nodeID {
						load += 1.0 // Replica has lower load
						break
					}
				}
			}
		}

		loads = append(loads, NodeLoad{
			NodeID: nodeID,
			Load:   load,
		})
	}

	return loads
}

// findPartitionToMove finds a suitable partition to move
func (rm *RebalanceManager) findPartitionToMove(nodeID string, targetLoad float64) *Partition {
	rm.controller.state.mu.RLock()
	defer rm.controller.state.mu.RUnlock()

	// Find a partition where this node is the leader
	for _, partition := range rm.controller.state.Partitions {
		if partition.Leader == nodeID {
			return partition
		}
	}

	return nil
}

// movePartition moves a partition from one node to another
func (rm *RebalanceManager) movePartition(partitionID int, fromNode, toNode string) error {
	rm.logger.Info("Moving partition %d from %s to %s", partitionID, fromNode, toNode)

	rm.controller.state.mu.Lock()
	defer rm.controller.state.mu.Unlock()

	partition, exists := rm.controller.state.Partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// Update partition leader
	partition.Leader = toNode
	partition.Status = "rebalancing"

	// Start replication
	replicationManager := NewReplicationManager(rm.controller)
	if err := replicationManager.StartReplication(partitionID, fromNode, toNode); err != nil {
		return fmt.Errorf("failed to start replication: %v", err)
	}

	return nil
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
