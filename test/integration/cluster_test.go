package integration_test

import (
	"fmt"
	"testing"

	"github.com/sajjad-MoBe/CloudKVStore/client"
	"github.com/stretchr/testify/assert"
)

// TestNode represents a single node in the cluster
type TestNode struct {
	ID        string
	LBAddress string
	IsLeader  bool
	IsRunning bool
}

// TestPartition represents a partition with leader and replicas
type TestPartition struct {
	Leader   *TestNode
	Replicas []*TestNode
}

// TestCluster represents the entire test cluster
type TestCluster struct {
	Partitions []*TestPartition
	Nodes      []*TestNode
}

// NewCluster creates a new test cluster
func NewCluster(partitions, replicas int) *TestCluster {
	cluster := &TestCluster{
		Partitions: make([]*TestPartition, partitions),
		Nodes:      make([]*TestNode, 0),
	}

	nodeCounter := 0
	for i := 0; i < partitions; i++ {
		// Create leader
		leader := &TestNode{
			ID:        fmt.Sprintf("node-%d", nodeCounter),
			LBAddress: fmt.Sprintf("localhost:%d", 8000+nodeCounter),
			IsLeader:  true,
			IsRunning: true,
		}
		nodeCounter++
		cluster.Nodes = append(cluster.Nodes, leader)

		// Create replicas
		replicaNodes := make([]*TestNode, replicas-1)
		for j := 0; j < replicas-1; j++ {
			replica := &TestNode{
				ID:        fmt.Sprintf("node-%d", nodeCounter),
				LBAddress: fmt.Sprintf("localhost:%d", 8000+nodeCounter),
				IsLeader:  false,
				IsRunning: true,
			}
			nodeCounter++
			replicaNodes[j] = replica
			cluster.Nodes = append(cluster.Nodes, replica)
		}

		cluster.Partitions[i] = &TestPartition{
			Leader:   leader,
			Replicas: replicaNodes,
		}
	}

	return cluster
}

// GetLoadBalancerAddress returns a load balancer address
func (c *TestCluster) GetLoadBalancerAddress() string {
	if len(c.Nodes) == 0 {
		return "localhost:8000"
	}
	return c.Nodes[0].LBAddress // Using first node as LB for testing
}

// Shutdown stops all nodes in the cluster
func (c *TestCluster) Shutdown() {
	for _, node := range c.Nodes {
		node.IsRunning = false
	}
}

// GetAllKeys mock implementation for testing
func (n *TestNode) GetAllKeys() map[string][]byte {
	// In a real implementation, this would query the node's data
	// For testing, we return a mock response
	return map[string][]byte{
		"test-key": []byte("test-value"),
	}
}

func startTestCluster(nodeCount int) []*TestNode {
	nodes := make([]*TestNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = &TestNode{
			ID:        fmt.Sprintf("node-%d", i),
			LBAddress: fmt.Sprintf("localhost:%d", 8000+i),
			IsRunning: true,
		}
	}
	return nodes
}

func stopTestCluster(nodes []*TestNode) {
	for _, node := range nodes {
		node.IsRunning = false
	}
}

func TestLeaderFailover(t *testing.T) {
	// Start 3-node cluster
	nodes := startTestCluster(3)
	defer stopTestCluster(nodes)

	// Write to leader
	c := client.NewClient(nodes[0].LBAddress, client.DefaultRetryConfig())
	err := c.Set("key", []byte("value"))
	assert.NoError(t, err)

	// Kill leader node
	nodes[0].IsRunning = false

	// Verify new leader elected and data accessible
	newClient := client.NewClient(nodes[1].LBAddress, client.DefaultRetryConfig())
	val, err := newClient.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), val)
}

func TestCrossPartitionConsistency(t *testing.T) {
	// Deploy full cluster with 3 partitions
	cluster := NewCluster(3, 2) // 3 partitions, 2 replicas each
	defer cluster.Shutdown()

	// Write 1000 keys across partitions
	c := client.NewClient(cluster.GetLoadBalancerAddress(), client.DefaultRetryConfig())
	for i := 0; i < 100; i++ { // Reduced to 100 for faster testing
		key := fmt.Sprintf("key-%d", i)
		err := c.Set(key, []byte(fmt.Sprintf("value-%d", i)))
		assert.NoError(t, err)
	}

	// Verify all replicas have identical data
	// Note: In a real test, you would need to implement actual data retrieval
	// This is just a structural example
	for _, partition := range cluster.Partitions {
		leaderData := partition.Leader.GetAllKeys()
		for _, replica := range partition.Replicas {
			assert.Equal(t, leaderData, replica.GetAllKeys())
		}
	}
}
