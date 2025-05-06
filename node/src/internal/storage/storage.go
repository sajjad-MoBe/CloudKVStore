package internal

import (
    "github.com/sajjad-MoBe/CloudKVStore/node/src/internal/shared"
    "sync"
)


type SinglePartitionStore struct {
	mutex sync.RWMutex
	data map[string]string
	wal []shared.OperationLogEntry // simple in memory write-ahead log
	// partition and stuff will be added later
}

func NewSinglePartitionStore() *SinglePartitionStore {
	return &SinglePartitionStore{
		data: make(map[string]string),
		wal:  make([]shared.OperationLogEntry, 0),
	}
}

// Add Set, Get, Delete methods in the next step...


// Helper (optional for debugging)
func (s *SinglePartitionStore) GetWalEntries() []shared.OperationLogEntry {
	s.mutex.RLock() // Use RLock as we are only reading the WAL slice pointer
	defer s.mutex.RUnlock()
	return s.wal
}