package main_test

import (
	"testing"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/api"
	"github.com/stretchr/testify/assert"
)

func TestPartitionCreation(t *testing.T) {
	// Create a config for testing
	config := api.PartitionConfig{
		MaxMemTableSize: 1024 * 1024, // 1MB
		MaxLevels:       3,
	}

	// Create a mock health manager or use nil if not needed
	var healthManager *api.HealthManager = nil

	// Initialize the PartitionManager with the config
	pm := api.NewPartitionManager(config, healthManager)

	// Test creating a new partition
	err := pm.CreatePartition("p1", 3) // 3 replicas
	assert.NoError(t, err)

	// Verify partition exists
	partition, exists := pm.GetPartition("p1")
	assert.True(t, exists)
	assert.Equal(t, 3, partition.ReplicaCount)
}

func TestKeyAssignment(t *testing.T) {
	pm := partitions.NewPartitionManager()

	// Create multiple partitions
	pm.CreatePartition("p1", 2)
	pm.CreatePartition("p2", 2)

	// Test key assignment
	key1 := "test-key-1"
	key2 := "test-key-2"

	partition1 := pm.GetPartitionForKey(key1)
	partition2 := pm.GetPartitionForKey(key2)

	// Keys should be assigned to different partitions
	assert.NotEqual(t, partition1, partition2)
}

func TestReplicaManagement(t *testing.T) {
	pm := partitions.NewPartitionManager()

	// Create partition with 3 replicas
	err := pm.CreatePartition("p1", 3)
	assert.NoError(t, err)

	// Add replicas
	node1 := "node-1"
	node2 := "node-2"
	node3 := "node-3"

	pm.AddReplica("p1", node1)
	pm.AddReplica("p1", node2)
	pm.AddReplica("p1", node3)

	// Verify replica count
	partition, _ := pm.GetPartition("p1")
	assert.Equal(t, 3, len(partition.Replicas))

	// Test replica removal
	pm.RemoveReplica("p1", node2)
	partition, _ = pm.GetPartition("p1")
	assert.Equal(t, 2, len(partition.Replicas))
}

func TestPartitionReassignment(t *testing.T) {
	pm := partitions.NewPartitionManager()

	// Create initial partition
	pm.CreatePartition("p1", 2)
	pm.AddReplica("p1", "node-1")
	pm.AddReplica("p1", "node-2")

	// Test reassignment
	err := pm.ReassignPartition("p1", "node-3")
	assert.NoError(t, err)

	// Verify new assignment
	partition, _ := pm.GetPartition("p1")
	assert.Contains(t, partition.Replicas, "node-3")
}

func TestPartitionDeletion(t *testing.T) {
	pm := partitions.NewPartitionManager()

	// Create and populate partition
	pm.CreatePartition("p1", 2)
	pm.AddReplica("p1", "node-1")
	pm.AddReplica("p1", "node-2")

	// Delete partition
	err := pm.DeletePartition("p1")
	assert.NoError(t, err)

	// Verify partition is gone
	_, exists := pm.GetPartition("p1")
	assert.False(t, exists)
}
