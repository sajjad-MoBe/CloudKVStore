package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

// ReplicationStatus represents the state of replication for a partition
type ReplicationStatus struct {
	Status      string        `json:"status"`        // "syncing", "streaming", "failed"
	LastSent    int64         `json:"last_sent"`     // Timestamp of last sent entry
	LastAckTime time.Time     `json:"last_ack_time"` // Time of last acknowledgment
	NeedsSync   bool          `json:"needs_sync"`    // Whether full sync is needed
	Lag         time.Duration `json:"lag"`           // Current replication lag
}

// ReplicationManager handles data replication between nodes
type ReplicationManager struct {
	controller *Controller
	logger     *shared.Logger
	metrics    *shared.MetricsCollector
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(controller *Controller) *ReplicationManager {
	return &ReplicationManager{
		controller: controller,
		logger:     shared.DefaultLogger,
		metrics:    shared.DefaultCollector,
	}
}

// StartReplication starts replication for a partition
func (rm *ReplicationManager) StartReplication(partitionID int, sourceNode, targetNode string) error {
	rm.logger.Info("Starting replication for partition %d from %s to %s", partitionID, sourceNode, targetNode)

	// Get partition info
	partition, exists := rm.controller.state.Partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// Verify source and target nodes
	if _, exists := rm.controller.state.Nodes[sourceNode]; !exists {
		return fmt.Errorf("source node %s not found", sourceNode)
	}
	if _, exists := rm.controller.state.Nodes[targetNode]; !exists {
		return fmt.Errorf("target node %s not found", targetNode)
	}

	// If partition is not active, try to activate it
	if partition.Status != "active" {
		rm.logger.Info("Activating partition %d", partitionID)
		partition.Status = "active"
	}

	// Start replication in a goroutine
	go rm.replicatePartition(partitionID, sourceNode, targetNode)

	return nil
}

// replicatePartition performs the actual replication
func (rm *ReplicationManager) replicatePartition(partitionID int, sourceNode, targetNode string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rm.logger.Info("Replicating partition %d from %s to %s", partitionID, sourceNode, targetNode)
	startTime := time.Now()

	// Get WAL entries from source node
	sourceWAL, err := rm.getWALEntries(ctx, sourceNode, partitionID)
	if err != nil {
		rm.logger.Error("Failed to get WAL entries from source node: %v", err)
		return
	}

	// Apply WAL entries to target node
	if err := rm.applyWALEntries(ctx, targetNode, partitionID, sourceWAL); err != nil {
		rm.logger.Error("Failed to apply WAL entries to target node: %v", err)
		return
	}

	// Verify replication
	if err := rm.VerifyReplication(partitionID, sourceNode, targetNode); err != nil {
		rm.logger.Error("Replication verification failed: %v", err)
		return
	}

	// Record metrics
	duration := time.Since(startTime).Seconds()
	rm.metrics.RecordMetric(shared.MetricReplicationLag, shared.Gauge, duration, map[string]string{
		"partition_id": fmt.Sprintf("%d", partitionID),
		"source_node":  sourceNode,
		"target_node":  targetNode,
	})

	rm.logger.Info("Replication completed for partition %d from %s to %s in %v",
		partitionID, sourceNode, targetNode, duration)
}

// getWALEntries retrieves WAL entries from a node
func (rm *ReplicationManager) getWALEntries(ctx context.Context, nodeID string, partitionID int) ([]wal.LogEntry, error) {
	// Get node address
	node, exists := rm.controller.state.Nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Create HTTP request to get WAL entries
	url := fmt.Sprintf("http://%s/wal/entries?partition=%d", node.Address, partitionID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get WAL entries: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Decode response
	var entries []wal.LogEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to decode WAL entries: %v", err)
	}

	return entries, nil
}

// applyWALEntries applies WAL entries to a node
func (rm *ReplicationManager) applyWALEntries(ctx context.Context, nodeID string, partitionID int, entries []wal.LogEntry) error {
	// Get node address
	node, exists := rm.controller.state.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// Create HTTP request to apply WAL entries
	url := fmt.Sprintf("http://%s/wal/apply", node.Address)
	data, err := json.Marshal(map[string]interface{}{
		"partition_id": partitionID,
		"entries":      entries,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal WAL entries: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to apply WAL entries: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// VerifyReplication verifies that replication was successful
func (rm *ReplicationManager) VerifyReplication(partitionID int, sourceNode, targetNode string) error {
	rm.logger.Info("Verifying replication for partition %d between %s and %s",
		partitionID, sourceNode, targetNode)

	maxRetries := 3
	retryDelay := 2 * time.Second
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			rm.logger.Info("Retrying verification (attempt %d/%d) after error: %v",
				attempt+1, maxRetries, lastErr)
			time.Sleep(retryDelay)
		}

		// Wait for replication to complete
		if err := rm.waitForReplication(partitionID, sourceNode, targetNode); err != nil {
			lastErr = fmt.Errorf("failed to wait for replication: %v", err)
			continue
		}

		// Get WAL entries from both nodes
		sourceEntries, err := rm.getWALEntries(context.Background(), sourceNode, partitionID)
		if err != nil {
			lastErr = fmt.Errorf("failed to get source WAL entries: %v", err)
			continue
		}

		targetEntries, err := rm.getWALEntries(context.Background(), targetNode, partitionID)
		if err != nil {
			lastErr = fmt.Errorf("failed to get target WAL entries: %v", err)
			continue
		}

		// Compare WAL entries
		if len(sourceEntries) != len(targetEntries) {
			lastErr = fmt.Errorf("WAL entry count mismatch: source=%d, target=%d",
				len(sourceEntries), len(targetEntries))
			continue
		}

		// Compare each entry with detailed error reporting
		for i, sourceEntry := range sourceEntries {
			targetEntry := targetEntries[i]
			if !compareWALEntries(sourceEntry, targetEntry) {
				lastErr = fmt.Errorf("WAL entry mismatch at index %d: source=%+v, target=%+v",
					i, sourceEntry, targetEntry)
				continue
			}
		}

		// Verify data consistency
		if err := rm.verifyDataConsistency(partitionID, sourceNode, targetNode); err != nil {
			lastErr = fmt.Errorf("data consistency check failed: %v", err)
			continue
		}

		// If we get here, verification was successful
		rm.logger.Info("Replication verification successful for partition %d between %s and %s",
			partitionID, sourceNode, targetNode)
		return nil
	}

	return fmt.Errorf("replication verification failed after %d attempts: %v", maxRetries, lastErr)
}

// waitForReplication waits for replication to complete
func (rm *ReplicationManager) waitForReplication(partitionID int, sourceNode, targetNode string) error {
	timeout := 30 * time.Second
	deadline := time.Now().Add(timeout)
	checkInterval := 500 * time.Millisecond

	for time.Now().Before(deadline) {
		// Get replication status
		sourceStatus, err := rm.getReplicationStatus(sourceNode, partitionID)
		if err != nil {
			// If we get a 404, it might mean replication hasn't started yet
			if strings.Contains(err.Error(), "404") {
				time.Sleep(checkInterval)
				continue
			}
			return fmt.Errorf("failed to get source replication status: %v", err)
		}

		targetStatus, err := rm.getReplicationStatus(targetNode, partitionID)
		if err != nil {
			// If we get a 404, it might mean replication hasn't started yet
			if strings.Contains(err.Error(), "404") {
				time.Sleep(checkInterval)
				continue
			}
			return fmt.Errorf("failed to get target replication status: %v", err)
		}

		// Check if replication is complete or in progress
		if sourceStatus.Status == "streaming" && targetStatus.Status == "streaming" {
			// Verify that the last sent entry matches
			if sourceStatus.LastSent == targetStatus.LastSent {
				return nil
			}
		} else if sourceStatus.Status == "syncing" || targetStatus.Status == "syncing" {
			// If either node is still syncing, wait
			time.Sleep(checkInterval)
			continue
		}

		time.Sleep(checkInterval)
	}

	return fmt.Errorf("replication did not complete within %v", timeout)
}

// getReplicationStatus gets the replication status for a node
func (rm *ReplicationManager) getReplicationStatus(nodeID string, partitionID int) (*ReplicationStatus, error) {
	node, exists := rm.controller.state.Nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	url := fmt.Sprintf("http://%s/replication/status?partition=%d", node.Address, partitionID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get replication status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var status ReplicationStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode replication status: %v", err)
	}

	return &status, nil
}

// compareWALEntries compares two WAL entries for equality
func compareWALEntries(a, b wal.LogEntry) bool {
	return a.Timestamp.Equal(b.Timestamp) &&
		a.Operation == b.Operation &&
		a.Key == b.Key &&
		a.Value == b.Value &&
		a.Partition == b.Partition
}

// verifyDataConsistency verifies that the data is consistent between nodes
func (rm *ReplicationManager) verifyDataConsistency(partitionID int, sourceNode, targetNode string) error {
	// Get sample keys from source node
	sourceKeys, err := rm.getSampleKeys(sourceNode, partitionID)
	if err != nil {
		return fmt.Errorf("failed to get source keys: %v", err)
	}

	// Compare values for each key
	for _, key := range sourceKeys {
		sourceValue, err := rm.getValue(sourceNode, key)
		if err != nil {
			return fmt.Errorf("failed to get source value for key %s: %v", key, err)
		}

		targetValue, err := rm.getValue(targetNode, key)
		if err != nil {
			return fmt.Errorf("failed to get target value for key %s: %v", key, err)
		}

		if sourceValue != targetValue {
			return fmt.Errorf("value mismatch for key %s: source=%v, target=%v",
				key, sourceValue, targetValue)
		}
	}

	return nil
}

// getSampleKeys gets a sample of keys from a node
func (rm *ReplicationManager) getSampleKeys(nodeID string, partitionID int) ([]string, error) {
	node, exists := rm.controller.state.Nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	url := fmt.Sprintf("http://%s/keys?partition=%d", node.Address, partitionID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var keys []string
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return nil, fmt.Errorf("failed to decode keys: %v", err)
	}

	return keys, nil
}

// getValue gets a value from a node
func (rm *ReplicationManager) getValue(nodeID, key string) (string, error) {
	node, exists := rm.controller.state.Nodes[nodeID]
	if !exists {
		return "", fmt.Errorf("node %s not found", nodeID)
	}

	url := fmt.Sprintf("http://%s/kv/%s", node.Address, key)
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to get value: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode value: %v", err)
	}

	return result.Value, nil
}

// HandleNodeRecovery handles the recovery of a failed node
func (rm *ReplicationManager) HandleNodeRecovery(nodeID string) error {
	rm.logger.Info("Handling recovery for node %s", nodeID)

	// Find all partitions where this node is a replica
	for partitionID, partition := range rm.controller.state.Partitions {
		if partition.Leader == nodeID {
			// If this node was a leader, we need to re-replicate from the new leader
			if len(partition.Replicas) > 0 {
				newLeader := partition.Replicas[0]
				if err := rm.StartReplication(partitionID, newLeader, nodeID); err != nil {
					rm.logger.Error("Failed to start replication for partition %d: %v", partitionID, err)
					return err
				}
			}
		} else {
			// If this node was a replica, we need to re-replicate from the leader
			if err := rm.StartReplication(partitionID, partition.Leader, nodeID); err != nil {
				rm.logger.Error("Failed to start replication for partition %d: %v", partitionID, err)
				return err
			}
		}
	}

	return nil
}
