package main_test

import (
	"testing"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/api"
	"github.com/stretchr/testify/assert"
)

type mockPartitionManager struct {
	partitions map[string]int
}

func (m *mockPartitionManager) CreatePartition(id string, replicas int) error {
	m.partitions[id] = replicas
	return nil
}

func (m *mockPartitionManager) GetPartitionForKey(key string) string {
	// Simple hash function for testing
	return "p1"
}

type mockHealthManager struct{}

func (m *mockHealthManager) IsHealthy(nodeID string) bool {
	return true
}

func TestPartitionAssignment(t *testing.T) {
	pm := &mockPartitionManager{
		partitions: make(map[string]int),
	}
	c := api.NewController(pm, &mockHealthManager{})

	// Test partition creation
	err := c.HandleCreatePartition("p1", 3) // 3 replicas
	assert.NoError(t, err)

	// Test key-to-partition mapping
	partitionID := pm.GetPartitionForKey("test-key")
	assert.Equal(t, "p1", partitionID)
}
