package partition

import (
	"github.com/sajjad-MoBe/CloudKVStore/shared"
	"sync"
	"sync/atomic"
	"time"
)

// PartitionData represents the state of a partition
type PartitionData struct {
	ID       int
	Leader   string
	Replicas []string
	Status   string
	// LSM levels (0 is the active memtable)
	levels []*MemTable
	// Current active memtable
	activeMemTable *MemTable
	// Snapshot for new replicas
	latestSnapshot *Snapshot
	// Key-level locks for concurrent access
	keyLocks sync.Map
}

func (pm *PartitionManager) shouldCompact(partition *PartitionData) bool {
	partition.activeMemTable.mu.RLock()
	size := int64(len(partition.activeMemTable.data))
	partition.activeMemTable.mu.RUnlock()
	return size >= pm.config.MaxMemTableSize
}

func (pm *PartitionManager) compactMemTable(partition *PartitionData) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Create new memtable
	newMemTable := &MemTable{
		data:       make(map[string][]byte),
		wal:        partition.activeMemTable.wal,
		level:      len(partition.levels),
		maxSize:    pm.config.MaxMemTableSize,
		version:    time.Now().UnixNano(),
		createdAt:  time.Now(),
		refCount:   0,
		isSnapshot: false,
	}

	// Move data to new level, maintaining version order
	partition.activeMemTable.mu.Lock()
	for k, v := range partition.activeMemTable.data {
		newMemTable.data[k] = v
	}
	partition.activeMemTable.mu.Unlock()

	// Update partition state
	partition.levels = append(partition.levels, newMemTable)
	partition.activeMemTable = newMemTable

	// Clean up old memtables that are no longer referenced
	go pm.cleanupOldMemtables(partition)
}

func (pm *PartitionManager) cleanupOldMemtables(partition *PartitionData) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Keep at least one memtable
	if len(partition.levels) <= 1 {
		return
	}

	// Check each memtable except the active one
	for i := 0; i < len(partition.levels)-1; i++ {
		level := partition.levels[i]
		// If no snapshots are using this memtable and it's old enough
		if atomic.LoadInt32(&level.refCount) == 0 &&
			time.Since(level.createdAt) > pm.config.ReplicationLag*2 {
			// Remove from levels
			partition.levels = append(partition.levels[:i], partition.levels[i+1:]...)
			i-- // Adjust index since we removed an element
		}
	}
}
