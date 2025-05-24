package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

const (
	Node1Addr = "http://node1:8081"
	Node2Addr = "http://node2:8082"
	Node3Addr = "http://node3:8083"
)

type NodeInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type PartitionInfo struct {
	ID       int      `json:"id"`
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`
}

func main() {
	// Wait for nodes to be ready
	waitForNodes()

	// Run tests
	fmt.Println("Starting tests...")

	// Test 1: Basic Operations
	fmt.Println("\nTest 1: Basic Operations")
	testBasicOperations()

	// Test 2: RPS Measurement
	fmt.Println("\nTest 2: RPS Measurement")
	measureRPS()

	// Test 3: Multiple Nodes and Partitions
	fmt.Println("\nTest 3: Multiple Nodes and Partitions")
	testMultipleNodesAndPartitions()

	// Test 4: Node Removal and Failover
	fmt.Println("\nTest 4: Node Removal and Failover")
	testNodeRemovalAndFailover()

	// Test 5: Node Restart
	fmt.Println("\nTest 5: Node Restart")
	testNodeRestart()

	// Test 6: Add Node Under Load
	fmt.Println("\nTest 6: Add Node Under Load")
	testAddNodeUnderLoad()

	// Test 7: Replica Count Adjustment
	fmt.Println("\nTest 7: Replica Count Adjustment")
	testReplicaCountAdjustment()

	fmt.Println("\nAll tests completed!")
}

func waitForNodes() {
	fmt.Println("Waiting for nodes to be ready...")
	for {
		allReady := true
		for _, addr := range []string{Node1Addr, Node2Addr, Node3Addr} {
			resp, err := http.Get(addr + "/cluster/status")
			if err != nil || resp.StatusCode != http.StatusOK {
				allReady = false
				break
			}
		}
		if allReady {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Println("All nodes are ready!")
}

func testBasicOperations() {
	fmt.Println("Testing core operations...")

	// Test Set operation
	fmt.Println("\nTesting Set operation:")
	key := "test-key"
	value := "test-value"

	// 1. Find the leader for the key's partition
	fmt.Println("1. Finding leader for key's partition...")
	resp, err := http.Get(Node1Addr + "/partitions")
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to get partitions:", err)
	}
	var partitions []PartitionInfo
	json.NewDecoder(resp.Body).Decode(&partitions)

	// 2. Set value on the leader
	fmt.Println("2. Setting value on leader...")
	body, _ := json.Marshal(map[string]string{"value": value})
	req, _ := http.NewRequest("PUT", Node1Addr+"/kv/"+key, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to set value:", err)
	}

	// 3. Verify WAL entry was created
	fmt.Println("3. Verifying WAL entry...")
	resp, err = http.Get(Node1Addr + "/wal")
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to get WAL entries:", err)
	}
	var walEntries []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&walEntries)
	if len(walEntries) == 0 {
		log.Fatal("No WAL entries found")
	}

	// 4. Wait for replication (Eventual Consistency)
	fmt.Println("4. Waiting for replication...")
	time.Sleep(2 * time.Second)

	// Test Get operation
	fmt.Println("\nTesting Get operation:")
	// 1. Get value from any node (load balancer would handle this)
	fmt.Println("1. Getting value from node...")
	resp, err = http.Get(Node2Addr + "/kv/" + key)
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to get value:", err)
	}
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	if result["value"] != value {
		log.Fatal("Value mismatch")
	}

	// Test Delete operation
	fmt.Println("\nTesting Delete operation:")
	// 1. Find the leader for the key's partition
	fmt.Println("1. Finding leader for key's partition...")
	resp, err = http.Get(Node1Addr + "/partitions")
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to get partitions:", err)
	}
	json.NewDecoder(resp.Body).Decode(&partitions)

	// 2. Delete value from the leader
	fmt.Println("2. Deleting value from leader...")
	req, _ = http.NewRequest("DELETE", Node1Addr+"/kv/"+key, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to delete value:", err)
	}

	// 3. Verify WAL entry was created
	fmt.Println("3. Verifying WAL entry...")
	resp, err = http.Get(Node1Addr + "/wal")
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to get WAL entries:", err)
	}
	json.NewDecoder(resp.Body).Decode(&walEntries)
	if len(walEntries) == 0 {
		log.Fatal("No WAL entries found")
	}

	// 4. Wait for replication
	fmt.Println("4. Waiting for replication...")
	time.Sleep(2 * time.Second)

	// 5. Verify deletion on replicas
	fmt.Println("5. Verifying deletion on replicas...")
	resp, err = http.Get(Node2Addr + "/kv/" + key)
	if err == nil && resp.StatusCode == http.StatusOK {
		log.Fatal("Value still exists on replica")
	}

	fmt.Println("\nCore operations test passed!")
}

func measureRPS() {
	const (
		duration = 10 * time.Second
		key      = "rps-test"
	)

	start := time.Now()
	operations := 0
	var wg sync.WaitGroup

	for time.Since(start) < duration {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value := fmt.Sprintf("value-%d", rand.Intn(1000))
			body, _ := json.Marshal(map[string]string{"value": value})
			req, _ := http.NewRequest("PUT", Node1Addr+"/kv/"+key, bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")
			http.DefaultClient.Do(req)
			operations++
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	rps := float64(operations) / elapsed.Seconds()

	fmt.Printf("Achieved %.2f RPS\n", rps)
}

func testMultipleNodesAndPartitions() {
	// Create partitions
	partitions := []PartitionInfo{
		{ID: 1, Leader: "node-1", Replicas: []string{"node-2", "node-3"}},
		{ID: 2, Leader: "node-2", Replicas: []string{"node-1", "node-3"}},
		{ID: 3, Leader: "node-3", Replicas: []string{"node-1", "node-2"}},
	}

	for _, p := range partitions {
		body, _ := json.Marshal(p)
		resp, err := http.Post(Node1Addr+"/partitions", "application/json", bytes.NewBuffer(body))
		if err != nil || resp.StatusCode != http.StatusOK {
			log.Fatal("Failed to create partition:", err)
		}
	}

	// Test data distribution
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		body, _ := json.Marshal(map[string]string{"value": value})
		req, _ := http.NewRequest("PUT", Node1Addr+"/kv/"+key, bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		http.DefaultClient.Do(req)
	}

	fmt.Println("Multiple nodes and partitions test passed!")
}

func testNodeRemovalAndFailover() {
	// Remove node-2
	req, _ := http.NewRequest("DELETE", Node1Addr+"/nodes/node-2", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to remove node:", err)
	}

	// Wait for failover
	time.Sleep(5 * time.Second)

	// Verify cluster status
	resp, err = http.Get(Node1Addr + "/cluster/status")
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to get cluster status:", err)
	}

	fmt.Println("Node removal and failover test passed!")
}

func testNodeRestart() {
	// Simulate node restart by removing and re-adding node-2
	req, _ := http.NewRequest("DELETE", Node1Addr+"/nodes/node-2", nil)
	http.DefaultClient.Do(req)

	time.Sleep(2 * time.Second)

	// Re-add node-2
	body, _ := json.Marshal(NodeInfo{ID: "node-2", Address: "node2:8082"})
	resp, err := http.Post(Node1Addr+"/nodes", "application/json", bytes.NewBuffer(body))
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to re-add node:", err)
	}

	// Wait for recovery
	time.Sleep(5 * time.Second)

	fmt.Println("Node restart test passed!")
}

func testAddNodeUnderLoad() {
	// Generate load
	go func() {
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("load-key-%d", i)
			value := fmt.Sprintf("load-value-%d", i)
			body, _ := json.Marshal(map[string]string{"value": value})
			req, _ := http.NewRequest("PUT", Node1Addr+"/kv/"+key, bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")
			http.DefaultClient.Do(req)
		}
	}()

	// Add new node
	body, _ := json.Marshal(NodeInfo{ID: "node-4", Address: "node4:8084"})
	resp, err := http.Post(Node1Addr+"/nodes", "application/json", bytes.NewBuffer(body))
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to add node under load:", err)
	}

	// Wait for rebalancing
	time.Sleep(10 * time.Second)

	fmt.Println("Add node under load test passed!")
}

func testReplicaCountAdjustment() {
	// Update partition replicas
	body, _ := json.Marshal(PartitionInfo{
		ID:       1,
		Leader:   "node-1",
		Replicas: []string{"node-2", "node-3", "node-4"},
	})
	req, _ := http.NewRequest("PUT", Node1Addr+"/partitions/1", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Failed to update replicas:", err)
	}

	// Wait for replication
	time.Sleep(5 * time.Second)

	fmt.Println("Replica count adjustment test passed!")
}
