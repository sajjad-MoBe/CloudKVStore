package shared

import (
	"errors"
	"sync"

	"time"
)

type SinglePartitionStore struct {
	mutex sync.RWMutex
	data  map[string]string
	wal   []OperationLogEntry // simple in memory write-ahead log
	// partition and stuff will be added later
}

func NewSinglePartitionStore() *SinglePartitionStore {
	return &SinglePartitionStore{
		data: make(map[string]string),
		wal:  make([]OperationLogEntry, 0),
	}
}

func (s *SinglePartitionStore) GetWalEntries() []OperationLogEntry {
	s.mutex.RLock() // Use RLock as we are only reading the WAL slice pointer
	defer s.mutex.RUnlock()
	return s.wal
}

// Add Set, Get, Delete methods in the next step...

func (s *SinglePartitionStore) Set(key, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	entry := OperationLogEntry{
		Operation: "SET",
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
		Partition: 0, // Default partition ID
	}

	s.wal = append(s.wal, entry)
	// fmt.Printf("WAL: %+v\n", s.wal)

	s.data[key] = value

	return nil // no error
}

func (s *SinglePartitionStore) Get(key string) (string, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	value, found := s.data[key]

	return value, found
}

func (s *SinglePartitionStore) Delete(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	entry := OperationLogEntry{
		Operation: "DELETE",
		Key:       key,
		Value:     "",
		Timestamp: time.Now(),
		Partition: 0, // Default partition ID
	}
	_, found := s.data[key]
	if !found {
		return errors.New("key not found")
	}

	s.wal = append(s.wal, entry)
	// fmt.Printf("WAL: %+v\n", s.wal)
	delete(s.data, key)

	return nil // no error
}

func (s *SinglePartitionStore) GetWAL() []OperationLogEntry {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Return a copy to prevent external modifications
	result := make([]OperationLogEntry, len(s.wal))
	copy(result, s.wal)
	return result
}

// StorageEngine defines the interface for the key-value store
type StorageEngine interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	CreateSnapshot() (string, error)
	RestoreSnapshot(path string) error
	SetWithTTL(key string, value []byte, ttl time.Duration) error
	Cleanup() error
}
