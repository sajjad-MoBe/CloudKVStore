package partition

import "time"

// PartitionConfig holds configuration for partitions
type PartitionConfig struct {
	MaxMemTableSize int64
	MaxLevels       int
	WALConfig       wal.WALConfig
	ReplicationLag  time.Duration
}
