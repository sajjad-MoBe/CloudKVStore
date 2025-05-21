package e2e_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/client"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/cmd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCluster struct {
	Nodes      []*node.Node
	Clients    []*client.Client
	Partitions int
	Replicas   int
}

func setupTestCluster(t *testing.T, nodeCount, partitions, replicas int) *TestCluster {
	cluster := &TestCluster{
		Nodes:      make([]*node.Node, nodeCount),
		Clients:    make([]*client.Client, nodeCount),
		Partitions: partitions,
		Replicas:   replicas,
	}

	// Start nodes
	for i := 0; i < nodeCount; i++ {
		config := node.Config{
			ID:            fmt.Sprintf("node-%d", i),
			Port:          8000 + i,
			PartitionPort: 9000 + i,
		}
		n := node.NewNode(config)
		err := n.Start()
		require.NoError(t, err)
		cluster.Nodes[i] = n

		// Create client for this node
		c := client.NewClient(fmt.Sprintf("localhost:%d", config.Port), client.DefaultRetryConfig())
		cluster.Clients[i] = c
	}

	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)
	return cluster
}

func (c *TestCluster) shutdown() {
	for _, n := range c.Nodes {
		n.Shutdown()
	}
}

func TestBasicOperations(t *testing.T) {
	cluster := setupTestCluster(t, 3, 2, 2)
	defer cluster.shutdown()

	// Test basic write and read
	client := cluster.Clients[0]
	err := client.Set("test-key", []byte("test-value"))
	require.NoError(t, err)

	value, err := client.Get("test-key")
	require.NoError(t, err)
	assert.Equal(t, []byte("test-value"), value)
}

func TestClusterExpansion(t *testing.T) {
	// Start with 3 nodes
	cluster := setupTestCluster(t, 3, 2, 2)
	defer cluster.shutdown()

	// Add 2 more nodes
	for i := 3; i < 5; i++ {
		config := node.Config{
			ID:            fmt.Sprintf("node-%d", i),
			Port:          8000 + i,
			PartitionPort: 9000 + i,
		}
		n := node.NewNode(config)
		err := n.Start()
		require.NoError(t, err)
		cluster.Nodes = append(cluster.Nodes, n)

		c := client.NewClient(fmt.Sprintf("localhost:%d", config.Port), client.DefaultRetryConfig())
		cluster.Clients = append(cluster.Clients, c)
	}

	// Wait for rebalancing
	time.Sleep(5 * time.Second)

	// Verify data consistency after expansion
	client := cluster.Clients[0]
	value, err := client.Get("test-key")
	require.NoError(t, err)
	assert.Equal(t, []byte("test-value"), value)
}

func TestNodeFailure(t *testing.T) {
	cluster := setupTestCluster(t, 5, 3, 2)
	defer cluster.shutdown()

	// Write some data
	client := cluster.Clients[0]
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		err := client.Set(key, []byte(fmt.Sprintf("value-%d", i)))
		require.NoError(t, err)
	}

	// Kill a node
	cluster.Nodes[1].Shutdown()

	// Wait for failover
	time.Sleep(2 * time.Second)

	// Verify data is still accessible
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value, err := client.Get(key)
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}

func TestConcurrentOperations(t *testing.T) {
	cluster := setupTestCluster(t, 5, 3, 2)
	defer cluster.shutdown()

	// Create multiple clients
	clients := cluster.Clients

	// Run concurrent operations
	done := make(chan bool)
	for i := 0; i < 5; i++ {
		go func(clientIndex int) {
			c := clients[clientIndex]
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("client-%d-key-%d", clientIndex, j)
				err := c.Set(key, []byte(fmt.Sprintf("value-%d", j)))
				require.NoError(t, err)

				value, err := c.Get(key)
				require.NoError(t, err)
				assert.Equal(t, []byte(fmt.Sprintf("value-%d", j)), value)
			}
			done <- true
		}(i)
	}

	// Wait for all operations to complete
	for i := 0; i < 5; i++ {
		<-done
	}
}

func TestPartitionReassignment(t *testing.T) {
	cluster := setupTestCluster(t, 5, 3, 2)
	defer cluster.shutdown()

	// Write data to specific partition
	client := cluster.Clients[0]
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("partition-key-%d", i)
		err := client.Set(key, []byte(fmt.Sprintf("value-%d", i)))
		require.NoError(t, err)
	}

	// Trigger partition reassignment
	err := cluster.Nodes[0].ReassignPartition("p1", "node-4")
	require.NoError(t, err)

	// Wait for reassignment
	time.Sleep(2 * time.Second)

	// Verify data is still accessible
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("partition-key-%d", i)
		value, err := client.Get(key)
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}
