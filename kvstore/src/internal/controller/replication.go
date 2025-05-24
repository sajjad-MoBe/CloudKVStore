package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
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
	mu          sync.RWMutex
	LastError   string    `json:"last_error"`
	StartTime   time.Time `json:"start_time"`
}

// ReplicationManager handles data replication between nodes
type ReplicationManager struct {
	controller    *Controller
	logger        *shared.Logger
	metrics       *shared.MetricsCollector
	mu            sync.RWMutex
	status        map[string]map[int]*ReplicationStatus
	httpClient    *http.Client
	replicationMu sync.Mutex // Mutex to protect the replication process
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(controller *Controller) *ReplicationManager {
	return &ReplicationManager{
		controller: controller,
		logger:     shared.DefaultLogger,
		metrics:    shared.DefaultCollector,
		status:     make(map[string]map[int]*ReplicationStatus),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// StartReplication starts replication for a partition
func (rm *ReplicationManager) StartReplication(partitionID int, sourceNode, targetNode string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Initialize status tracking
	if _, exists := rm.status[sourceNode]; !exists {
		rm.status[sourceNode] = make(map[int]*ReplicationStatus)
	}
	if _, exists := rm.status[targetNode]; !exists {
		rm.status[targetNode] = make(map[int]*ReplicationStatus)
	}

	rm.status[sourceNode][partitionID] = &ReplicationStatus{
		Status:    "streaming",
		StartTime: time.Now(),
	}
	rm.status[targetNode][partitionID] = &ReplicationStatus{
		Status:    "streaming",
		StartTime: time.Now(),
	}

	// Start replication in a goroutine
	go rm.replicatePartition(partitionID, sourceNode, targetNode)

	return nil
}

// replicatePartition performs the actual replication
func (rm *ReplicationManager) replicatePartition(partitionID int, sourceNode, targetNode string) {
	rm.replicationMu.Lock()         // Lock the replication process
	defer rm.replicationMu.Unlock() // Unlock the replication process

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rm.logger.Info("Replicating partition %d from %s to %s", partitionID, sourceNode, targetNode)

	// Get WAL entries from source node
	sourceWAL, err := rm.getWALEntries(ctx, sourceNode, partitionID)
	if err != nil {
		rm.updateStatus(sourceNode, partitionID, "failed", 0, err.Error())
		rm.updateStatus(targetNode, partitionID, "failed", 0, err.Error())
		rm.logger.Error("Failed to get WAL entries from source node: %v", err)
		return
	}

	// Apply WAL entries to target node with retries
	if err := rm.applyWALEntriesWithRetry(ctx, targetNode, partitionID, sourceWAL); err != nil {
		rm.updateStatus(sourceNode, partitionID, "failed", 0, err.Error())
		rm.updateStatus(targetNode, partitionID, "failed", 0, err.Error())
		rm.logger.Error("Failed to apply WAL entries to target node: %v", err)
		return
	}

	// Verify data consistency with retries
	if err := rm.verifyDataConsistencyWithRetry(ctx, partitionID, sourceNode, targetNode); err != nil {
		rm.updateStatus(sourceNode, partitionID, "failed", 0, err.Error())
		rm.updateStatus(targetNode, partitionID, "failed", 0, err.Error())
		rm.logger.Error("Failed to verify data consistency: %v", err)
		return
	}

	// Update status to completed
	rm.updateStatus(sourceNode, partitionID, "completed", int64(len(sourceWAL)), "")
	rm.updateStatus(targetNode, partitionID, "completed", int64(len(sourceWAL)), "")

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

// applyWALEntriesWithRetry applies WAL entries with exponential backoff
func (rm *ReplicationManager) applyWALEntriesWithRetry(ctx context.Context, nodeID string, partitionID int, entries []wal.LogEntry) error {
	maxRetries := 5
	backoff := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		err := rm.applyWALEntries(ctx, nodeID, partitionID, entries)
		if err == nil {
			return nil
		}

		rm.logger.Warn("Failed to apply WAL entries (attempt %d/%d): %v", i+1, maxRetries, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}

	return fmt.Errorf("failed to apply WAL entries after %d retries", maxRetries)
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
		sourceValue, err := rm.getValue(sourceNode, partitionID, key)
		if err != nil {
			return fmt.Errorf("failed to get source value for key %s: %v", key, err)
		}

		targetValue, err := rm.getValue(targetNode, partitionID, key)
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

// verifyDataConsistencyWithRetry verifies data consistency with retries
func (rm *ReplicationManager) verifyDataConsistencyWithRetry(ctx context.Context, partitionID int, sourceNode, targetNode string) error {
	maxRetries := 3
	backoff := 200 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		err := rm.verifyDataConsistency(partitionID, sourceNode, targetNode)
		if err == nil {
			return nil
		}

		rm.logger.Warn("Failed to verify data consistency (attempt %d/%d): %v", i+1, maxRetries, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}

	return fmt.Errorf("failed to verify data consistency after %d retries", maxRetries)
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

// getValue gets a value for a key from a node
func (rm *ReplicationManager) getValue(nodeID string, partitionID int, key string) (string, error) {
	url := fmt.Sprintf("http://%s/get?key=%s&partition=%d", nodeID, key, partitionID)
	resp, err := rm.httpClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get value: %s", resp.Status)
	}

	var result struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.Value, nil
}

// updateStatus updates the replication status for a node and partition
func (rm *ReplicationManager) updateStatus(nodeID string, partitionID int, status string, lastSent int64, lastError string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.status[nodeID]; !exists {
		rm.status[nodeID] = make(map[int]*ReplicationStatus)
	}

	if _, exists := rm.status[nodeID][partitionID]; !exists {
		rm.status[nodeID][partitionID] = &ReplicationStatus{}
	}

	rm.status[nodeID][partitionID].mu.Lock()
	rm.status[nodeID][partitionID].Status = status
	rm.status[nodeID][partitionID].LastSent = lastSent
	rm.status[nodeID][partitionID].LastError = lastError
	rm.status[nodeID][partitionID].mu.Unlock()
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
