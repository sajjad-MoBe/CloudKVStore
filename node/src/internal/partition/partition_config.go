package partition

import (
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/wal"
	"github.com/sajjad-MoBe/CloudKVStore/shared"
	"time"
)

// PartitionConfig holds configuration for partitions
type PartitionConfig struct {
	MaxMemTableSize int64
	MaxLevels       int
	WALConfig       wal.WALConfig
	ReplicationLag  time.Duration
}
