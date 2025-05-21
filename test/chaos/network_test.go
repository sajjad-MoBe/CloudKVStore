package chaos_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/sajjad-MoBe/CloudKVStore/client"
	"github.com/stretchr/testify/assert"
)

type TestCluster struct {
	Partitions []*TestPartition
	GroupA     *TestNodeGroup
	GroupB     *TestNodeGroup
}

type TestPartition struct {
	Leader   *TestNode
	Replicas []*TestNode
}

type TestNodeGroup struct {
	Nodes      []*TestNode
	LBAddress  string
	IsIsolated bool
}

type TestNode struct {
	ID        string
	Address   string
	IsRunning bool
}

func NewCluster(partitions, replicas int) *TestCluster {
	cluster := &TestCluster{
		Partitions: make([]*TestPartition, partitions),
		GroupA: &TestNodeGroup{
			LBAddress: "localhost:8000",
			Nodes:     make([]*TestNode, 0),
		},
		GroupB: &TestNodeGroup{
			LBAddress: "localhost:8001",
			Nodes:     make([]*TestNode, 0),
		},
	}

	// Initialize partitions and nodes
	for i := 0; i < partitions; i++ {
		leader := &TestNode{
			ID:        fmt.Sprintf("leader-%d", i),
			Address:   fmt.Sprintf("localhost:%d", 9000+i),
			IsRunning: true,
		}
		cluster.GroupA.Nodes = append(cluster.GroupA.Nodes, leader)

		replicaNodes := make([]*TestNode, replicas-1)
		for j := 0; j < replicas-1; j++ {
			replica := &TestNode{
				ID:        fmt.Sprintf("replica-%d-%d", i, j),
				Address:   fmt.Sprintf("localhost:%d", 9100+i*10+j),
				IsRunning: true,
			}
			replicaNodes[j] = replica
			cluster.GroupB.Nodes = append(cluster.GroupB.Nodes, replica)
		}

		cluster.Partitions[i] = &TestPartition{
			Leader:   leader,
			Replicas: replicaNodes,
		}
	}

	return cluster
}

func (g *TestNodeGroup) Isolate() {
	g.IsIsolated = true
	for _, node := range g.Nodes {
		node.IsRunning = false
	}
}

func (g *TestNodeGroup) Heal() {
	g.IsIsolated = false
	for _, node := range g.Nodes {
		node.IsRunning = true
	}
}

func (c *TestCluster) IsolateGroup(groupA, groupB string) {
	c.GroupA.Isolate()
	c.GroupB.Isolate()
}

func (c *TestCluster) HealNetwork() {
	c.GroupA.Heal()
	c.GroupB.Heal()
}

func (c *TestCluster) Shutdown() {
	for _, partition := range c.Partitions {
		partition.Leader.IsRunning = false
		for _, replica := range partition.Replicas {
			replica.IsRunning = false
		}
	}
}

func TestNetworkPartition(t *testing.T) {
	cluster := NewCluster(2, 3)
	defer cluster.Shutdown()

	// Simulate network partition between 2 node groups
	cluster.IsolateGroup("group-a", "group-b")

	// Write to both partitions (split-brain scenario)
	clientA := client.NewClient(cluster.GroupA.LBAddress, client.DefaultRetryConfig())
	err := clientA.Set("key", []byte("value-a"))
	assert.NoError(t, err)

	clientB := client.NewClient(cluster.GroupB.LBAddress, client.DefaultRetryConfig())
	err = clientB.Set("key", []byte("value-b"))
	assert.NoError(t, err)

	// Heal network
	cluster.HealNetwork()

	// Verify conflict resolution
	val, err := clientA.Get("key")
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(val, []byte("value-a")) ||
		bytes.Equal(val, []byte("value-b")))
}
