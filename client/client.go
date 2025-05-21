package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Client represents a client for interacting with the key-value store cluster
type Client struct {
	// Load balancer address
	lbAddress string
	// HTTP client for making requests
	httpClient *http.Client
	// Retry configuration
	retryConfig RetryConfig
	// Mutex for concurrent operations
	mu sync.RWMutex
}

// RetryConfig defines retry behavior
type RetryConfig struct {
	MaxRetries int
	RetryDelay time.Duration
	Timeout    time.Duration
}

// DefaultRetryConfig returns default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 3,
		RetryDelay: 100 * time.Millisecond,
		Timeout:    5 * time.Second,
	}
}

// NewClient creates a new client instance
func NewClient(lbAddress string, retryConfig RetryConfig) *Client {
	return &Client{
		lbAddress: lbAddress,
		httpClient: &http.Client{
			Timeout: retryConfig.Timeout,
		},
		retryConfig: retryConfig,
	}
}

// Set stores a key-value pair in the cluster
func (c *Client) Set(key string, value []byte) error {
	// First, get partition info from load balancer
	partitionInfo, err := c.getPartitionInfo(key)
	if err != nil {
		return fmt.Errorf("failed to get partition info: %v", err)
	}

	// Send request to leader
	urlStr := fmt.Sprintf("http://%s/api/v1/keys/%s", partitionInfo.Leader, key)
	req, err := http.NewRequest(http.MethodPut, urlStr, bytes.NewReader(value))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Try request with retry
	var lastErr error
	for i := 0; i < c.retryConfig.MaxRetries; i++ {
		resp, err := c.httpClient.Do(req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable {
				// Not the leader, get updated partition info
				partitionInfo, err = c.getPartitionInfo(key)
				if err != nil {
					lastErr = err
					continue
				}
				// Update request URL with new leader
				urlStr = fmt.Sprintf("http://%s/api/v1/keys/%s", partitionInfo.Leader, key)
				parsedURL, err := url.Parse(urlStr)
				if err != nil {
					lastErr = err
					continue
				}
				req.URL = parsedURL
				continue
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		} else {
			lastErr = err
		}

		// Wait before retry
		time.Sleep(c.retryConfig.RetryDelay)
	}
	return fmt.Errorf("max retries exceeded: %v", lastErr)
}

// Get retrieves a value for a key from the cluster
func (c *Client) Get(key string) ([]byte, error) {
	// First, get partition info from load balancer
	partitionInfo, err := c.getPartitionInfo(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition info: %v", err)
	}

	// Try each replica in order
	for _, replica := range append([]string{partitionInfo.Leader}, partitionInfo.Replicas...) {
		url := fmt.Sprintf("http://%s/api/v1/keys/%s", replica, key)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			continue
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return io.ReadAll(resp.Body)
		}
	}

	return nil, fmt.Errorf("failed to get value from any replica")
}

// Delete removes a key from the cluster
func (c *Client) Delete(key string) error {
	// First, get partition info from load balancer
	partitionInfo, err := c.getPartitionInfo(key)
	if err != nil {
		return fmt.Errorf("failed to get partition info: %v", err)
	}

	// Send request to leader
	urlStr := fmt.Sprintf("http://%s/api/v1/keys/%s", partitionInfo.Leader, key)
	req, err := http.NewRequest(http.MethodDelete, urlStr, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Try request with retry
	var lastErr error
	for i := 0; i < c.retryConfig.MaxRetries; i++ {
		resp, err := c.httpClient.Do(req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable {
				// Not the leader, get updated partition info
				partitionInfo, err = c.getPartitionInfo(key)
				if err != nil {
					lastErr = err
					continue
				}
				// Update request URL with new leader
				urlStr = fmt.Sprintf("http://%s/api/v1/keys/%s", partitionInfo.Leader, key)
				parsedURL, err := url.Parse(urlStr)
				if err != nil {
					lastErr = err
					continue
				}
				req.URL = parsedURL
				continue
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		} else {
			lastErr = err
		}

		// Wait before retry
		time.Sleep(c.retryConfig.RetryDelay)
	}
	return fmt.Errorf("max retries exceeded: %v", lastErr)
}

// PartitionInfo represents information about a partition
type PartitionInfo struct {
	ID       int      `json:"id"`
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`
	Status   string   `json:"status"`
}

// getPartitionInfo gets partition information from the load balancer
func (c *Client) getPartitionInfo(key string) (*PartitionInfo, error) {
	url := fmt.Sprintf("http://%s/api/v1/partitions/info?key=%s", c.lbAddress, key)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var info PartitionInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return &info, nil
}

// BatchClient represents a client for batch operations
type BatchClient struct {
	client *Client
	// Channel for batch operations
	batchCh chan batchOp
	// Wait group for batch operations
	wg sync.WaitGroup
	// Results channel
	resultsCh chan BatchResult
}

// batchOp represents a batch operation
type batchOp struct {
	opType string // "set", "get", "delete"
	key    string
	value  []byte
}

// batchResult represents the result of a batch operation

type BatchResult struct { // Changed from batchResult to BatchResult (exported)
	Key   string // Exported
	Value []byte // Exported
	Err   error  // Exported
}

// NewBatchClient creates a new batch client
func NewBatchClient(client *Client, batchSize int) *BatchClient {
	bc := &BatchClient{
		client:    client,
		batchCh:   make(chan batchOp, batchSize),
		resultsCh: make(chan BatchResult, batchSize),
	}

	// Start batch processor
	go bc.processBatch()

	return bc
}

// Set adds a set operation to the batch
func (bc *BatchClient) Set(key string, value []byte) {
	bc.wg.Add(1)
	bc.batchCh <- batchOp{
		opType: "set",
		key:    key,
		value:  value,
	}
}

// Get adds a get operation to the batch
func (bc *BatchClient) Get(key string) {
	bc.wg.Add(1)
	bc.batchCh <- batchOp{
		opType: "get",
		key:    key,
	}
}

// Delete adds a delete operation to the batch
func (bc *BatchClient) Delete(key string) {
	bc.wg.Add(1)
	bc.batchCh <- batchOp{
		opType: "delete",
		key:    key,
	}
}

// Wait waits for all batch operations to complete
func (bc *BatchClient) Wait() []BatchResult {
	bc.wg.Wait()
	close(bc.batchCh)

	var results []BatchResult
	for res := range bc.resultsCh {
		results = append(results, res)
	}
	return results
}

// processBatch processes batch operations
func (bc *BatchClient) processBatch() {
	for op := range bc.batchCh {
		go func(op batchOp) {
			defer bc.wg.Done()

			var result BatchResult
			result.Key = op.key

			switch op.opType {
			case "set":
				result.Err = bc.client.Set(op.key, op.value)
			case "get":
				value, err := bc.client.Get(op.key)
				result.Value = value
				result.Err = err
			case "delete":
				result.Err = bc.client.Delete(op.key)
			}

			bc.resultsCh <- result
		}(op)
	}
	close(bc.resultsCh)
}

// LoadTest performs a load test with the given parameters
func LoadTest(client *Client, numRequests int, batchSize int) error {
	bc := NewBatchClient(client, batchSize)

	// Generate and send batch operations
	for i := 0; i < numRequests; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))

		// Alternate between set and get operations
		if i%2 == 0 {
			bc.Set(key, value)
		} else {
			bc.Get(key)
		}
	}

	// Wait for all operations to complete
	results := bc.Wait()

	// Check for errors
	for _, result := range results {
		if result.Err != nil {
			return fmt.Errorf("operation failed for key %s: %v", result.Key, result.Err)
		}
	}

	return nil
}
