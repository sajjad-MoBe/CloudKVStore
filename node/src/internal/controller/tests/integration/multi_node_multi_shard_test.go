package integration

import (
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller/tests/helpers"
	"github.com/stretchr/testify/assert"
)

func TestMultiNodeMultiShardSetup(t *testing.T) {
	// Initialize controller
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl)

	// Define multiple partitions
	partitions := []controller.Partition{
		{ID: 1, Leader: "node-1", Replicas: []string{"node-2", "node-3"}, Status: "healthy"},
		{ID: 2, Leader: "node-2", Replicas: []string{"node-1", "node-3"}, Status: "healthy"},
		{ID: 3, Leader: "node-3", Replicas: []string{"node-1", "node-2"}, Status: "healthy"},
	}

	// Register nodes
	nodes := map[string]string{
		"node-1": "localhost:8081",
		"node-2": "localhost:8082",
		"node-3": "localhost:8083",
	}

	for nodeID, address := range nodes {
		err := helpers.RegisterNode(ctrl, nodeID, address)
		assert.NoError(t, err)
	}

	// Create partitions
	for _, partition := range partitions {
		err := helpers.CreatePartition(ctrl, partition)
		assert.NoError(t, err)
	}

	// Wait for nodes to become active
	time.Sleep(2 * time.Second)

	// Measure RPS with initial setup
	initialRPS := helpers.MeasureMixedOperationsRPS(helpers.NewTestClient("http://localhost"+ctrl.GetTestPort()), 1000)
	t.Logf("Initial RPS: %f", initialRPS)

	// Register a new node before updating replicas
	err := helpers.RegisterNode(ctrl, "node-4", "localhost:8084")
	assert.NoError(t, err)

	// Wait for node-4 to become active
	time.Sleep(5 * time.Second)

	// Add more replicas to each partition
	for i := range partitions {
		partitions[i].Replicas = append(partitions[i].Replicas, "node-4")
		err := helpers.UpdatePartitionReplicas(ctrl, partitions[i].ID, partitions[i].Replicas)
		assert.NoError(t, err)
	}

	// Wait for nodes to become active
	time.Sleep(5 * time.Second)

	// Measure RPS after adding more replicas
	finalRPS := helpers.MeasureMixedOperationsRPS(helpers.NewTestClient("http://localhost"+ctrl.GetTestPort()), 1000)
	t.Logf("Final RPS: %f", finalRPS)

	// Assert that RPS increases
	assert.Greater(t, finalRPS, initialRPS, "RPS should increase as more replicas are added")
}
