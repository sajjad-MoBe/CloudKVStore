package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/cmd/node"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

const (
	numPartitions = 4
)

type testNode struct {
	node   *node.Node
	port   string
	stopCh chan struct{}
}

func TestMultiNodeSetup(t *testing.T) {
	testCases := []struct {
		name     string
		numNodes int
	}{
		{"Single Node", 1},
		{"Two Nodes", 2},
		{"Three Nodes", 3},
	}

	for testIndex, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			controllerPort := 8080 + testIndex*10
			baseNodePort := 8081 + testIndex*10

			config := partition.PartitionConfig{
				MaxMemTableSize: 1024 * 1024, // 1MB
				WALConfig: wal.WALConfig{
					MaxFileSize: 10 * 1024 * 1024, // 10MB
				},
			}
			healthManager := shared.NewHealthManager("controller-1")
			partitionManager := partition.NewPartitionManager(config, healthManager)
			ctrl := controller.NewController(partitionManager, healthManager)

			controllerStopCh := make(chan struct{})
			go func() {
				if err := ctrl.Start(fmt.Sprintf(":%d", controllerPort)); err != nil {
					t.Errorf("Failed to start controller: %v", err)
				}
			}()

			time.Sleep(time.Second)

			nodes := startNodes(t, tc.numNodes, baseNodePort, controllerPort)
			defer func() {
				stopNodes(nodes)
				ctrl.Stop()
				close(controllerStopCh)
				time.Sleep(time.Second)
			}()

			// Register nodes with the controller
			for i, n := range nodes {
				nodeID := fmt.Sprintf("node-%d", i+1)
				address := fmt.Sprintf("localhost:%s", n.port)

				req := struct {
					ID      string `json:"id"`
					Address string `json:"address"`
				}{
					ID:      nodeID,
					Address: address,
				}

				jsonData, err := json.Marshal(req)
				if err != nil {
					t.Fatalf("Failed to marshal node registration request: %v", err)
				}

				url := fmt.Sprintf("http://localhost:%d/nodes", controllerPort)
				resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
				if err != nil {
					t.Fatalf("Failed to register node: %v", err)
				}
				resp.Body.Close()

				if resp.StatusCode != http.StatusCreated {
					t.Fatalf("Failed to register node: unexpected status code %d", resp.StatusCode)
				}
			}

			// Wait for nodes to become active
			timeout := time.After(5 * time.Second)
			tick := time.Tick(100 * time.Millisecond)
			for {
				select {
				case <-timeout:
					t.Fatal("Timeout waiting for nodes to become active")
				case <-tick:
					allActive := true
					for i := range nodes {
						nodeID := fmt.Sprintf("node-%d", i+1)
						url := fmt.Sprintf("http://localhost:%d/nodes/%s/status", controllerPort, nodeID)
						resp, err := http.Get(url)
						if err != nil {
							allActive = false
							continue
						}
						var result struct {
							Status string `json:"status"`
						}
						if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
							resp.Body.Close()
							allActive = false
							continue
						}
						resp.Body.Close()
						if result.Status != "active" {
							allActive = false
							continue
						}
					}
					if allActive {
						goto NodesActive
					}
				}
			}
		NodesActive:

			if err := createPartitions(t, tc.numNodes, controllerPort, testIndex); err != nil {
				t.Fatalf("Failed to create partitions: %v", err)
			}

			time.Sleep(2 * time.Second)

			measureMultiNodePerformance(t, nodes)
		})
	}
}

func startNodes(t *testing.T, numNodes, baseNodePort, controllerPort int) []*testNode {
	nodes := make([]*testNode, numNodes)
	for i := 0; i < numNodes; i++ {
		port := fmt.Sprintf("%d", baseNodePort+i)
		n := node.NewNode(
			fmt.Sprintf("node-%d", i+1),
			":"+port,
			fmt.Sprintf("http://localhost:%d", controllerPort),
		)

		stopCh := make(chan struct{})
		go func() {
			if err := n.Start(); err != nil {
				t.Errorf("Failed to start node: %v", err)
			}
		}()

		nodes[i] = &testNode{
			node:   n,
			port:   port,
			stopCh: stopCh,
		}

		time.Sleep(time.Second)
	}
	return nodes
}

func stopNodes(nodes []*testNode) {
	for _, n := range nodes {
		n.node.Stop()
		close(n.stopCh)
	}
}

func createPartitions(t *testing.T, numNodes, controllerPort, testIndex int) error {
	for i := 0; i < numPartitions; i++ {
		nodeIDs := make([]string, numNodes)
		for j := 0; j < numNodes; j++ {
			nodeIDs[j] = fmt.Sprintf("node-%d", j+1)
		}

		partitionID := i + testIndex*100 // unique partition ID per test run
		req := struct {
			PartitionID int      `json:"partition_id"`
			Leader      string   `json:"leader"`
			Replicas    []string `json:"replicas"`
		}{
			PartitionID: partitionID,
			Leader:      nodeIDs[0],
			Replicas:    nodeIDs[1:],
		}

		jsonData, err := json.Marshal(req)
		if err != nil {
			return err
		}

		url := fmt.Sprintf("http://localhost:%d/partitions", controllerPort)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to create partition: %d, body: %s", resp.StatusCode, string(body))
		}
	}
	return nil
}

func measureMultiNodePerformance(t *testing.T, nodes []*testNode) {
	const numOperations = 1000
	var wg sync.WaitGroup
	results := make(chan float64, len(nodes))

	for _, n := range nodes {
		wg.Add(1)
		go func(node *testNode) {
			defer wg.Done()
			rps := measureNodePerformance(t, node, numOperations)
			results <- rps
		}(n)
	}

	wg.Wait()
	close(results)

	var totalRPS float64
	for rps := range results {
		totalRPS += rps
	}

	t.Logf("Total RPS across %d nodes: %.2f", len(nodes), totalRPS)
	t.Logf("Average RPS per node: %.2f", totalRPS/float64(len(nodes)))
}

func measureNodePerformance(t *testing.T, n *testNode, count int) float64 {
	start := time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("perf-key-%d", i)
		value := fmt.Sprintf("perf-value-%d", i)

		if err := setValueOnNode(n.port, key, value); err != nil {
			t.Errorf("Set operation failed on node %s: %v", n.port, err)
			return 0
		}

		if _, err := getValueFromNode(n.port, key); err != nil {
			t.Errorf("Get operation failed on node %s: %v", n.port, err)
			return 0
		}

		if err := deleteValueOnNode(n.port, key); err != nil {
			t.Errorf("Delete operation failed on node %s: %v", n.port, err)
			return 0
		}
	}
	duration := time.Since(start)
	return float64(count*3) / duration.Seconds()
}

func setValueOnNode(port, key, value string) error {
	data := map[string]string{"value": value}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://localhost:%s/kv/%s", port, key), bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func getValueFromNode(port, key string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/kv/%s", port, key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("key not found")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result["value"], nil
}

func deleteValueOnNode(port, key string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://localhost:%s/kv/%s", port, key), nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
