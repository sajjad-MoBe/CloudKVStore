package integration

import (
	"strconv"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller/tests/helpers"
	"github.com/stretchr/testify/assert"
)

func TestNodeRestartOneByOne(t *testing.T) {
	// Initialize controller
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl)

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

	// Create a partition with all nodes as replicas
	partition := controller.Partition{
		ID:       1,
		Leader:   "node-1",
		Replicas: []string{"node-2", "node-3"},
		Status:   "healthy",
	}
	err := helpers.CreatePartition(ctrl, partition)
	assert.NoError(t, err)

	// Wait for nodes to become active
	time.Sleep(3 * time.Second)

	// Insert test data
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())
	for i := 0; i < 10; i++ {
		err := client.Set("test-key-"+strconv.Itoa(i), "test-value-"+strconv.Itoa(i))
		assert.NoError(t, err)
	}

	// Restart nodes one by one
	for nodeID := range nodes {
		t.Run("Restart_"+nodeID, func(t *testing.T) {
			// Remove node
			err := helpers.RemoveNode(ctrl, nodeID)
			assert.NoError(t, err)

			// Wait for failover to occur
			time.Sleep(5 * time.Second)

			// Re-register node
			err = helpers.RegisterNode(ctrl, nodeID, nodes[nodeID])
			assert.NoError(t, err)

			// Wait for node to become active
			time.Sleep(5 * time.Second)

			// Verify data is preserved
			for i := 0; i < 10; i++ {
				value, err := client.Get("test-key-" + strconv.Itoa(i))
				assert.NoError(t, err)
				assert.Equal(t, "test-value-"+strconv.Itoa(i), value)
			}
		})
	}
}
