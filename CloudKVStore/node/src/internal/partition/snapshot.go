package partition

import (
	"sync/atomic"
	"time"
)

// Snapshot represents a point-in-time view of a partition
type Snapshot struct {
	Data      map[string][]byte
	Timestamp time.Time
	WALOffset int64
	Version   int64
	// Reference to memtables used in snapshot
	memtables []*MemTable
}

func (pm *PartitionManager) sendSnapshot(partitionID int, replicaID string, snapshot *Snapshot) error {
	// In a real implementation, this would send the snapshot to the replica
	// For now, we'll just simulate it
	return nil
}

func (pm *PartitionManager) createSnapshot(partition *PartitionData) (*Snapshot, error) {
	// Lock all memtables to ensure consistency
	for _, level := range partition.levels {
		level.mu.Lock()
		defer level.mu.Unlock()
	}

	// Create new snapshot
	snapshot := &Snapshot{
		Data:      make(map[string][]byte),
		Timestamp: time.Now(),
		Version:   time.Now().UnixNano(),
		memtables: make([]*MemTable, len(partition.levels)),
	}

	// Copy data from all levels, maintaining version order
	for i, level := range partition.levels {
		// Increment reference count
		atomic.AddInt32(&level.refCount, 1)
		snapshot.memtables[i] = level

		// Copy data, newer versions override older ones
		for k, v := range level.data {
			snapshot.Data[k] = v
		}
	}

	// Get WAL offset
	metrics := partition.activeMemTable.wal.GetMetrics()
	snapshot.WALOffset = metrics.TotalEntries

	return snapshot, nil
}

// releaseSnapshot decrements reference counts for memtables in a snapshot
func (pm *PartitionManager) releaseSnapshot(snapshot *Snapshot) {
	for _, memtable := range snapshot.memtables {
		atomic.AddInt32(&memtable.refCount, -1)
	}
}

// getSnapshotFromLeader gets a snapshot from the leader
func (pm *PartitionManager) getSnapshotFromLeader(partitionID int, leaderID string) (*Snapshot, error) {
	// In a real implementation, this would make an HTTP request to the leader
	// to get the snapshot. For now, we'll return a mock snapshot.
	return &Snapshot{
		Data:      make(map[string][]byte),
		Timestamp: time.Now(),
		Version:   time.Now().UnixNano(),
	}, nil
}
