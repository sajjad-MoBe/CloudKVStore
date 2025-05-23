package helpers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/wal"
)

// TestClient represents a test client for the key-value store
type TestClient struct {
	client    *http.Client
	serverURL string
}

// NewTestClient creates a new test client
func NewTestClient(serverURL string) *TestClient {
	return &TestClient{
		client:    &http.Client{},
		serverURL: serverURL,
	}
}

// Set sets a key-value pair
func (c *TestClient) Set(key, value string) error {
	url := fmt.Sprintf("%s/kv/%s", c.serverURL, key)

	// Create request body with value
	reqBody := struct {
		Value string `json:"value"`
	}{
		Value: value,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("error marshaling request body: %v", err)
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// Get retrieves a value for a key
func (c *TestClient) Get(key string) (string, error) {
	url := fmt.Sprintf("%s/kv/%s", c.serverURL, key)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("error decoding response: %v", err)
	}

	return result.Value, nil
}

// Delete removes a key-value pair
func (c *TestClient) Delete(key string) error {
	url := fmt.Sprintf("%s/kv/%s", c.serverURL, key)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	// Ignore 404 errors (key not found) during performance tests
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// SetupTestController creates a new controller instance for testing
func SetupTestController(t *testing.T) *controller.Controller {
	// Create WAL config
	walConfig := wal.WALConfig{
		MaxFileSize:    1024 * 1024, // 1MB
		MaxFiles:       5,
		RotationPeriod: 1 * time.Hour,
		CompressFiles:  true,
	}

	// Create partition config
	partitionConfig := partition.PartitionConfig{
		MaxMemTableSize: 1024 * 1024, // 1MB
		MaxLevels:       3,
		WALConfig:       walConfig,
		ReplicationLag:  5 * time.Second,
	}

	// Create health manager with test node ID
	healthManager := shared.NewHealthManager("test-node-1")

	// Create partition manager
	partitionManager := partition.NewPartitionManager(partitionConfig, healthManager)

	// Create controller
	controller := controller.NewController(partitionManager, healthManager)

	// Start controller in a goroutine
	portCh := make(chan string)
	go func() {
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Errorf("Failed to create listener: %v", err)
			return
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close() // Free the port so the controller can use it
		portCh <- fmt.Sprintf(":%d", port)
		if err := controller.Start(fmt.Sprintf(":%d", port)); err != nil {
			t.Errorf("Failed to start controller: %v", err)
		}
	}()

	// Get the port and wait for controller to start
	port := <-portCh
	time.Sleep(100 * time.Millisecond)

	// Store the port in the controller for test helpers to use
	controller.SetTestPort(port)

	return controller
}

// MeasureOperationRPS measures the requests per second for a given operation
func MeasureOperationRPS(client *TestClient, operation string, iterations int) float64 {
	start := time.Now()

	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		value := fmt.Sprintf("test-value-%d", i)

		var err error
		switch operation {
		case "set":
			err = client.Set(key, value)
		case "get":
			_, err = client.Get(key)
		case "delete":
			err = client.Delete(key)
		}

		if err != nil {
			// Log error but continue
			fmt.Printf("Error during %s operation: %v\n", operation, err)
		}
	}

	duration := time.Since(start)
	return float64(iterations) / duration.Seconds()
}

// MeasureMixedOperationsRPS measures RPS for a mix of operations
func MeasureMixedOperationsRPS(client *TestClient, iterations int) float64 {
	start := time.Now()

	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		value := fmt.Sprintf("test-value-%d", i)

		// Perform a mix of operations
		_ = client.Set(key, value)
		_, _ = client.Get(key)
		_ = client.Delete(key)
	}

	duration := time.Since(start)
	return float64(iterations*3) / duration.Seconds() // Multiply by 3 because we do 3 operations per iteration
}

// RegisterNode registers a new node with the controller
func RegisterNode(ctrl *controller.Controller, nodeID string, address string) error {
	// Create HTTP request to register node
	url := fmt.Sprintf("http://localhost%s/nodes", ctrl.GetTestPort())
	node := controller.Node{
		ID:      nodeID,
		Address: address,
		Status:  "joining",
	}

	jsonData, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("error marshaling node data: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// GetNodeStatus gets the status of a node
func GetNodeStatus(ctrl *controller.Controller, nodeID string) (string, error) {
	url := fmt.Sprintf("http://localhost%s/nodes/%s/status", ctrl.GetTestPort(), nodeID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("error decoding response: %v", err)
	}

	return result.Status, nil
}

// RemoveNode removes a node from the controller
func RemoveNode(ctrl *controller.Controller, nodeID string) error {
	url := fmt.Sprintf("http://localhost%s/nodes/%s", ctrl.GetTestPort(), nodeID)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// GetNodes gets all registered nodes
func GetNodes(ctrl *controller.Controller) (map[string]string, error) {
	url := fmt.Sprintf("http://localhost%s/nodes", ctrl.GetTestPort())
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var nodes []controller.Node
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	result := make(map[string]string)
	for _, node := range nodes {
		result[node.ID] = node.Address
	}

	return result, nil
}

// GetLoadBalancerConfig retrieves the load balancer configuration from the controller
func GetLoadBalancerConfig(ctrl *controller.Controller) (map[string]interface{}, error) {
	url := fmt.Sprintf("http://localhost%s/cluster/status", ctrl.GetTestPort())
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	return result, nil
}
