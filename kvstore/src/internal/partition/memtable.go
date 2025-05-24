package partition

import (
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
	"sync"
	"time"
)

// MemTable represents an LSM-like in-memory table
type MemTable struct {
	data    map[string][]byte
	wal     *wal.WALManager
	mu      sync.RWMutex
	level   int
	maxSize int64
	// Version tracking
	version   int64
	createdAt time.Time
	// Reference count for snapshot
	refCount int32
	// Snapshot flag
	isSnapshot bool
}
