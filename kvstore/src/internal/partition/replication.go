package partition

import (
	"errors"
	"fmt"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/wal"
	"io"
	"time"
)

// ReplicationStream manages replication to a replica
type ReplicationStream struct {
	replicaID string
	stream    io.Writer
	stopCh    chan struct{}
	lastSent  int64
	// Track replication lag
	lastAckTime time.Time
	// Track if replica needs full sync
	needsFullSync bool
	// Track replication status
	status string // "syncing", "streaming", "failed"
}

// ReplicationStatus represents the state of replication
type ReplicationStatus struct {
	ReplicaID   string
	Status      string
	LastSent    int64
	LastAckTime time.Time
	NeedsSync   bool
	Lag         time.Duration
}

func (pm *PartitionManager) startReplicationStream(partitionID int, replicaID string) error {
	stream := &ReplicationStream{
		replicaID: replicaID,
		stopCh:    make(chan struct{}),
	}

	pm.replicationStreams[partitionID][replicaID] = stream

	// Start background replication
	go pm.replicationLoop(partitionID, replicaID)

	return nil
}

func (pm *PartitionManager) replicationLoop(partitionID int, replicaID string) {
	stream := pm.replicationStreams[partitionID][replicaID]
	partition := pm.partitions[partitionID]

	// If replica needs full sync, send snapshot first
	if stream.needsFullSync {
		snapshot, err := pm.createSnapshot(partition)
		if err != nil {
			stream.status = "failed"
			fmt.Printf("Failed to create snapshot for replica %s: %v\n", replicaID, err)
			return
		}

		if err := pm.sendSnapshot(partitionID, replicaID, snapshot); err != nil {
			stream.status = "failed"
			fmt.Printf("Failed to send snapshot to replica %s: %v\n", replicaID, err)
			return
		}

		stream.lastSent = snapshot.WALOffset
		stream.needsFullSync = false
		stream.status = "streaming"
	}

	// Regular replication loop
	for {
		select {
		case <-stream.stopCh:
			return
		default:
			// Get new WAL entries
			var entries []*wal.WALEntry
			err := partition.activeMemTable.wal.Recover(func(entry *wal.WALEntry) error {
				if entry.Timestamp > stream.lastSent {
					entries = append(entries, entry)
				}
				return nil
			})
			if err != nil {
				stream.status = "failed"
				fmt.Printf("Failed to read WAL entries: %v\n", err)
				time.Sleep(pm.config.ReplicationLag)
				continue
			}

			// If lag is too high, switch to full sync
			if time.Since(stream.lastAckTime) > pm.config.ReplicationLag*2 {
				stream.needsFullSync = true
				stream.status = "syncing"
				return
			}

			// Stream entries to replica
			for _, entry := range entries {
				if err := pm.streamToReplica(partitionID, replicaID, entry); err != nil {
					stream.status = "failed"
					fmt.Printf("Failed to stream to replica %s: %v\n", replicaID, err)
					time.Sleep(pm.config.ReplicationLag)
					continue
				}
				stream.lastSent = entry.Timestamp
				stream.lastAckTime = time.Now()
			}

			time.Sleep(pm.config.ReplicationLag)
		}
	}
}

func (pm *PartitionManager) streamToReplicas(partitionID int, entry *wal.WALEntry) error {
	for replicaID := range pm.replicationStreams[partitionID] {
		if err := pm.streamToReplica(partitionID, replicaID, entry); err != nil {
			return fmt.Errorf("failed to stream to replica %s: %v", replicaID, err)
		}
	}
	return nil
}

func (pm *PartitionManager) streamToReplica(partitionID int, replicaID string, entry *wal.WALEntry) error {
	// In a real implementation, this would send the entry to the replica
	// For now, we'll just simulate it
	return nil
}

// GetReplicationStatus returns the replication status for a partition
func (pm *PartitionManager) GetReplicationStatus(partitionID int) ([]ReplicationStatus, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return nil, errors.New("partition not found")
	}

	// Check if partition is healthy
	if partition.Status != "healthy" {
		return nil, fmt.Errorf("partition is not healthy: %s", partition.Status)
	}

	status := make([]ReplicationStatus, 0)
	for replicaID, stream := range pm.replicationStreams[partitionID] {
		status = append(status, ReplicationStatus{
			ReplicaID:   replicaID,
			Status:      stream.status,
			LastSent:    stream.lastSent,
			LastAckTime: stream.lastAckTime,
			NeedsSync:   stream.needsFullSync,
			Lag:         time.Since(stream.lastAckTime),
		})
	}

	return status, nil
}

// syncFromNewLeader syncs data from the new leader after failover
func (pm *PartitionManager) syncFromNewLeader(partitionID int, newLeader string) {
	// Wait for a short time to ensure new leader is ready
	time.Sleep(2 * time.Second)

	// Get snapshot from new leader
	snapshot, err := pm.getSnapshotFromLeader(partitionID, newLeader)
	if err != nil {
		fmt.Printf("Failed to get snapshot from new leader: %v\n", err)
		return
	}

	// Apply snapshot
	pm.mu.Lock()
	partition := pm.partitions[partitionID]
	partition.activeMemTable.mu.Lock()
	partition.activeMemTable.data = make(map[string][]byte)
	for k, v := range snapshot.Data {
		partition.activeMemTable.data[k] = v
	}
	partition.activeMemTable.mu.Unlock()
	pm.mu.Unlock()

	// Start replication stream from new leader
	if err := pm.startReplicationStream(partitionID, newLeader); err != nil {
		fmt.Printf("Failed to start replication from new leader: %v\n", err)
		return
	}
}

// ConnectNewReplica handles connecting a new replica to a partition
func (pm *PartitionManager) ConnectNewReplica(partitionID int, replicaID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return errors.New("partition not found")
	}

	// Send latest snapshot
	if partition.latestSnapshot == nil {
		snapshot, err := pm.createSnapshot(partition)
		if err != nil {
			return fmt.Errorf("failed to create snapshot: %v", err)
		}
		partition.latestSnapshot = snapshot
	}

	// Send snapshot to new replica
	if err := pm.sendSnapshot(partitionID, replicaID, partition.latestSnapshot); err != nil {
		return fmt.Errorf("failed to send snapshot: %v", err)
	}

	// Start replication stream
	if err := pm.startReplicationStream(partitionID, replicaID); err != nil {
		return fmt.Errorf("failed to start replication stream: %v", err)
	}

	partition.Replicas = append(partition.Replicas, replicaID)
	return nil
}
