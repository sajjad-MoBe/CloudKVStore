package api

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/wal"
)

// PartitionManager handles partition-level operations and replication
type PartitionManager struct {
	mu sync.RWMutex
	// Partition-specific data
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
	healthManager *HealthManager
}

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

// Snapshot represents a point-in-time view of a partition
type Snapshot struct {
	Data      map[string][]byte
	Timestamp time.Time
	WALOffset int64
	Version   int64
	// Reference to memtables used in snapshot
	memtables []*MemTable
}

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

// PartitionConfig holds configuration for partitions
type PartitionConfig struct {
	MaxMemTableSize int64
	MaxLevels       int
	WALConfig       wal.WALConfig
	ReplicationLag  time.Duration
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(config PartitionConfig, healthManager *HealthManager) *PartitionManager {
	return &PartitionManager{
		partitions:         make(map[int]*PartitionData),
		walManagers:        make(map[int]*wal.WALManager),
		replicationStreams: make(map[int]map[string]*ReplicationStream),
		memTables:          make(map[int]*MemTable),
		config:             config,
		healthManager:      healthManager,
	}
}

// CreatePartition creates a new partition with its WAL and memtable
func (pm *PartitionManager) CreatePartition(id int, leader string, replicas []string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[id]; exists {
		return errors.New("partition already exists")
	}

	// Create WAL manager for the partition
	walManager, err := wal.NewWALManager(fmt.Sprintf("wal/partition_%d", id), pm.config.WALConfig)
	if err != nil {
		return fmt.Errorf("failed to create WAL manager: %v", err)
	}

	// Create initial memtable
	memTable := &MemTable{
		data:    make(map[string][]byte),
		wal:     walManager,
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
	pm.walManagers[id] = walManager
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

// Helper functions

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

func (pm *PartitionManager) sendSnapshot(partitionID int, replicaID string, snapshot *Snapshot) error {
	// In a real implementation, this would send the snapshot to the replica
	// For now, we'll just simulate it
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

	// Create WAL manager for the partition
	walManager, err := wal.NewWALManager(fmt.Sprintf("wal/partition_%d", newPartitionID), pm.config.WALConfig)
	if err != nil {
		return fmt.Errorf("failed to create WAL manager: %v", err)
	}

	// Create initial memtable
	memTable := &MemTable{
		data:    make(map[string][]byte),
		wal:     walManager,
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
	pm.walManagers[newPartitionID] = walManager
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
	delete(pm.walManagers, partitionID)
	delete(pm.memTables, partitionID)
	delete(pm.replicationStreams, partitionID)

	// Trigger rebalancing
	go pm.RebalancePartitions()

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

// releaseSnapshot decrements reference counts for memtables in a snapshot
func (pm *PartitionManager) releaseSnapshot(snapshot *Snapshot) {
	for _, memtable := range snapshot.memtables {
		atomic.AddInt32(&memtable.refCount, -1)
	}
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
