package partition

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

// PartitionManager handles partition-level operations and replication
type PartitionManager struct {
	mu sync.RWMutex
	// partition-specific data
	partitions map[int]*PartitionData
	// WAL managers for each partition
	walManagers map[int]*wal.WALManager
	// Replication streams
	replicationStreams map[int]map[string]*ReplicationStream
	// LSM-like in-memory structure
	memTables map[int]*MemTable
	// Configuration
	config PartitionConfig
	// Health manager for leader tracking
	healthManager *shared.HealthManager
	// In-memory key-value store for testing
	store map[string]string
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(config PartitionConfig, healthManager *shared.HealthManager) *PartitionManager {
	return &PartitionManager{
		partitions:         make(map[int]*PartitionData),
		walManagers:        make(map[int]*wal.WALManager),
		replicationStreams: make(map[int]map[string]*ReplicationStream),
		memTables:          make(map[int]*MemTable),
		config:             config,
		healthManager:      healthManager,
		store:              make(map[string]string),
	}
}

// CreatePartition creates a new partition with its WAL and memtable
func (pm *PartitionManager) CreatePartition(id int, leader string, replicas []string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[id]; exists {
		return errors.New("partition already exists")
	}

	// Create WAL for the partition
	walInstance, err := wal.NewWAL(fmt.Sprintf("wal/partition_%d", id), pm.config.WALConfig.MaxFileSize)
	if err != nil {
		return fmt.Errorf("failed to create WAL: %v", err)
	}

	// Create initial memtable
	memTable := &MemTable{
		data:    make(map[string][]byte),
		wal:     walInstance,
		level:   0,
		maxSize: pm.config.MaxMemTableSize,
	}

	partition := &PartitionData{
		ID:             id,
		Leader:         leader,
		Replicas:       replicas,
		Status:         "healthy",
		levels:         []*MemTable{memTable},
		activeMemTable: memTable,
	}

	pm.partitions[id] = partition
	pm.memTables[id] = memTable
	pm.replicationStreams[id] = make(map[string]*ReplicationStream)

	// Start replication streams
	for _, replica := range replicas {
		if err := pm.startReplicationStream(id, replica); err != nil {
			return fmt.Errorf("failed to start replication stream: %v", err)
		}
	}

	return nil
}

// Write handles writes to a partition with eventual consistency
func (pm *PartitionManager) Write(partitionID int, key string, value []byte) error {
	pm.mu.RLock()
	partition, exists := pm.partitions[partitionID]
	pm.mu.RUnlock()

	if !exists {
		return errors.New("partition not found")
	}

	// Acquire key-level lock
	lock, _ := partition.keyLocks.LoadOrStore(key, &sync.Mutex{})
	keyLock := lock.(*sync.Mutex)
	keyLock.Lock()
	defer keyLock.Unlock()

	// Write to WAL first
	walEntry := &wal.WALEntry{
		Operation: "SET",
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	if err := partition.activeMemTable.wal.Append(walEntry); err != nil {
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	// Write to memtable
	partition.activeMemTable.mu.Lock()
	partition.activeMemTable.data[key] = value
	partition.activeMemTable.mu.Unlock()

	// Check if memtable needs compaction
	if pm.shouldCompact(partition) {
		go pm.compactMemTable(partition)
	}

	// Stream to replicas asynchronously
	go func() {
		if err := pm.streamToReplicas(partitionID, walEntry); err != nil {
			fmt.Printf("Failed to stream to replicas: %v\n", err)
		}
	}()

	return nil
}

// Read handles reads from a partition with versioning
func (pm *PartitionManager) Read(partitionID int, key string) ([]byte, error) {
	pm.mu.RLock()
	partition, exists := pm.partitions[partitionID]
	pm.mu.RUnlock()

	if !exists {
		return nil, errors.New("partition not found")
	}

	// Acquire key-level read lock
	lock, _ := partition.keyLocks.LoadOrStore(key, &sync.Mutex{})
	keyLock := lock.(*sync.Mutex)
	keyLock.Lock()
	defer keyLock.Unlock()

	// Search through all levels in version order (newest first)
	for i := len(partition.levels) - 1; i >= 0; i-- {
		level := partition.levels[i]
		level.mu.RLock()
		value, exists := level.data[key]
		level.mu.RUnlock()
		if exists {
			return value, nil
		}
	}

	return nil, errors.New("key not found")
}

// HandleFailover handles leader failover for a partition
func (pm *PartitionManager) HandleFailover(partitionID int, newLeader string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return errors.New("partition not found")
	}

	// Stop all replication streams
	for _, stream := range pm.replicationStreams[partitionID] {
		close(stream.stopCh)
	}
	pm.replicationStreams[partitionID] = make(map[string]*ReplicationStream)

	// Update partition state
	oldLeader := partition.Leader
	partition.Leader = newLeader
	partition.Status = "rebalancing"

	// Create new snapshot for replication
	snapshot, err := pm.createSnapshot(partition)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	partition.latestSnapshot = snapshot

	// Start new replication streams
	for _, replica := range partition.Replicas {
		if replica != newLeader {
			if err := pm.startReplicationStream(partitionID, replica); err != nil {
				return fmt.Errorf("failed to start replication stream: %v", err)
			}
		}
	}

	// If this node is the new leader, start accepting writes
	if newLeader == pm.healthManager.GetNodeID() {
		// Start accepting writes for this partition
		partition.Status = "healthy"
	} else if oldLeader == pm.healthManager.GetNodeID() {
		// If this node was the old leader, discard data and update from new leader
		go pm.syncFromNewLeader(partitionID, newLeader)
	}

	return nil
}

// GetPartitionForKey determines which partition a key belongs to
func (pm *PartitionManager) GetPartitionForKey(key string) int {
	hash := sha256.Sum256([]byte(key))
	partitionID := int(hash[0]) % len(pm.partitions)
	return partitionID
}

// RebalancePartitions redistributes data when partition count changes
func (pm *PartitionManager) RebalancePartitions() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Get all active nodes
	activeNodes := make([]string, 0)
	for _, partition := range pm.partitions {
		if partition.Status == "healthy" {
			activeNodes = append(activeNodes, partition.Leader)
			activeNodes = append(activeNodes, partition.Replicas...)
		}
	}

	if len(activeNodes) == 0 {
		return errors.New("no active nodes available for rebalancing")
	}

	// Calculate keys per partition
	totalKeys := int64(0)
	for _, partition := range pm.partitions {
		partition.activeMemTable.mu.RLock()
		totalKeys += int64(len(partition.activeMemTable.data))
		partition.activeMemTable.mu.RUnlock()
	}
	keysPerPartition := totalKeys / int64(len(pm.partitions))

	// Calculate current load per node
	nodeLoad := make(map[string]int64)
	for _, node := range activeNodes {
		nodeLoad[node] = 0
	}
	for _, partition := range pm.partitions {
		partition.activeMemTable.mu.RLock()
		load := int64(len(partition.activeMemTable.data))
		partition.activeMemTable.mu.RUnlock()
		nodeLoad[partition.Leader] += load
	}

	// Redistribute partitions among nodes
	for _, partition := range pm.partitions {
		// Find node with least load
		minLoad := int64(1<<63 - 1)
		selectedNode := activeNodes[0]
		for _, node := range activeNodes {
			// Consider both current load and target load per partition
			adjustedLoad := nodeLoad[node] + keysPerPartition
			if adjustedLoad < minLoad {
				minLoad = adjustedLoad
				selectedNode = node
			}
		}

		// Update node load
		partition.activeMemTable.mu.RLock()
		oldLoad := int64(len(partition.activeMemTable.data))
		partition.activeMemTable.mu.RUnlock()
		nodeLoad[partition.Leader] -= oldLoad
		nodeLoad[selectedNode] += oldLoad

		// Assign leader
		partition.Leader = selectedNode

		// Assign replicas
		partition.Replicas = make([]string, 0)
		for i := 0; i < pm.config.MaxLevels; i++ {
			replicaNode := activeNodes[i%len(activeNodes)]
			if replicaNode != partition.Leader {
				partition.Replicas = append(partition.Replicas, replicaNode)
			}
		}

		// Start replication
		go pm.startReplicationStream(partition.ID, selectedNode)
	}

	return nil
}

// AddPartition adds a new partition to the cluster
func (pm *PartitionManager) AddPartition() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	newPartitionID := len(pm.partitions)

	// Create WAL for the partition
	walInstance, err := wal.NewWAL(fmt.Sprintf("wal/partition_%d", newPartitionID), pm.config.WALConfig.MaxFileSize)
	if err != nil {
		return fmt.Errorf("failed to create WAL: %v", err)
	}

	// Create initial memtable
	memTable := &MemTable{
		data:    make(map[string][]byte),
		wal:     walInstance,
		level:   0,
		maxSize: pm.config.MaxMemTableSize,
	}

	partition := &PartitionData{
		ID:             newPartitionID,
		Status:         "rebalancing",
		levels:         []*MemTable{memTable},
		activeMemTable: memTable,
	}

	pm.partitions[newPartitionID] = partition
	pm.memTables[newPartitionID] = memTable
	pm.replicationStreams[newPartitionID] = make(map[string]*ReplicationStream)

	// Trigger rebalancing
	go pm.RebalancePartitions()

	return nil
}

// RemovePartition removes a partition from the cluster
func (pm *PartitionManager) RemovePartition(partitionID int) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[partitionID]; !exists {
		return errors.New("partition does not exist")
	}

	// Stop all replication streams
	for _, stream := range pm.replicationStreams[partitionID] {
		close(stream.stopCh)
	}

	// Remove partition data
	delete(pm.partitions, partitionID)
	delete(pm.memTables, partitionID)
	delete(pm.replicationStreams, partitionID)

	// Trigger rebalancing
	go pm.RebalancePartitions()

	return nil
}

// HandleReplicaReconnect handles a replica reconnecting after being unavailable
func (pm *PartitionManager) HandleReplicaReconnect(partitionID int, replicaID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return errors.New("partition not found")
	}

	// Check if replica is part of this partition
	isReplica := false
	for _, r := range partition.Replicas {
		if r == replicaID {
			isReplica = true
			break
		}
	}
	if !isReplica {
		return errors.New("replica not part of this partition")
	}

	// Stop existing replication stream if any
	if stream, exists := pm.replicationStreams[partitionID][replicaID]; exists {
		close(stream.stopCh)
	}

	// Create new replication stream with full sync flag
	stream := &ReplicationStream{
		replicaID:     replicaID,
		stopCh:        make(chan struct{}),
		needsFullSync: true,
		status:        "syncing",
	}
	pm.replicationStreams[partitionID][replicaID] = stream

	// Start replication with full sync
	go pm.replicationLoop(partitionID, replicaID)

	// Update partition status
	partition.Status = "rebalancing"

	return nil
}

// GetPartitionInfo returns information about a partition
func (pm *PartitionManager) GetPartitionInfo(partitionID int) (*PartitionData, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return nil, errors.New("partition not found")
	}

	return partition, nil
}

// GetPartitions returns all partitions
func (pm *PartitionManager) GetPartitions() map[int]*PartitionData {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partitions := make(map[int]*PartitionData)
	for id, partition := range pm.partitions {
		partitions[id] = partition
	}
	return partitions
}

// Get retrieves a value for a given key
func (pm *PartitionManager) Get(key string) (string, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	value, ok := pm.store[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}
	return value, nil
}

// Set stores a value for a given key
func (pm *PartitionManager) Set(key, value string) error {
	if key == "" || value == "" {
		return fmt.Errorf("key and value must not be empty")
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.store[key] = value
	return nil
}

// Delete removes a key-value pair
func (pm *PartitionManager) Delete(key string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if _, ok := pm.store[key]; !ok {
		return fmt.Errorf("key not found")
	}
	delete(pm.store, key)
	return nil
}

// GetWALEntries retrieves WAL entries for a specific partition
func (pm *PartitionManager) GetWALEntries(partitionID int) []wal.LogEntry {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return nil
	}

	// Retrieve WAL entries from the partition's WAL
	entries, err := partition.activeMemTable.wal.Read()
	if err != nil {
		return nil
	}

	return entries
}
