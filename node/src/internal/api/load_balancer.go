package api

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// LoadBalancer handles request routing and load distribution
type LoadBalancer struct {
	mu sync.RWMutex
	// Partition routing information
	partitionRoutes map[int]*PartitionRoute
	// Number of partitions
	numPartitions int
	// Controller client for partition info
	controller *Controller
	// HTTP client for forwarding requests
	httpClient *http.Client
	// Read strategy configuration
	readStrategy ReadStrategy
	// Retry configuration
	retryConfig RetryConfig
}

// PartitionRoute holds routing information for a partition
type PartitionRoute struct {
	Leader     string
	Replicas   []string
	Status     string
	LastUpdate time.Time
}

// ReadStrategy defines how read requests are distributed
type ReadStrategy struct {
	// Type can be "leader_only", "replica_only", or "mixed"
	Type string
	// ReplicaWeight determines how often reads go to replicas (0-100)
	ReplicaWeight int
}

// RetryConfig defines retry behavior
type RetryConfig struct {
	MaxRetries int
	RetryDelay time.Duration
	Timeout    time.Duration
}

// NewLoadBalancer creates a new load balancer
func NewLoadBalancer(controller *Controller, numPartitions int, readStrategy ReadStrategy, retryConfig RetryConfig) *LoadBalancer {
	return &LoadBalancer{
		partitionRoutes: make(map[int]*PartitionRoute),
		numPartitions:   numPartitions,
		controller:      controller,
		httpClient: &http.Client{
			Timeout: retryConfig.Timeout,
		},
		readStrategy: readStrategy,
		retryConfig:  retryConfig,
	}
}

// Start initializes the load balancer
func (lb *LoadBalancer) Start() error {
	// Initial partition information fetch
	if err := lb.updatePartitionInfo(); err != nil {
		return fmt.Errorf("failed to fetch initial partition info: %v", err)
	}

	// Start background partition info updates
	go lb.periodicPartitionUpdate()

	return nil
}

// HandleRequest routes incoming requests to appropriate nodes
func (lb *LoadBalancer) HandleRequest(w http.ResponseWriter, r *http.Request) {
	// Extract key from request
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	// Determine partition for key
	partitionID := lb.getPartitionForKey(key)

	// Get target node based on operation type
	targetNode, err := lb.getTargetNode(partitionID, r.Method)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// Forward request to target node
	response, err := lb.forwardRequest(targetNode, r)
	if err != nil {
		// Handle retries if needed
		if lb.shouldRetry(r.Method) {
			response, err = lb.retryRequest(targetNode, r)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	// Copy response back to client
	for k, v := range response.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(response.StatusCode)
	io.Copy(w, response.Body)
	response.Body.Close()
}

// getPartitionForKey determines which partition a key belongs to
func (lb *LoadBalancer) getPartitionForKey(key string) int {
	hash := sha256.Sum256([]byte(key))
	partitionID := int(hash[0]) % lb.numPartitions
	return partitionID
}

// getTargetNode determines which node should handle the request
func (lb *LoadBalancer) getTargetNode(partitionID int, method string) (string, error) {
	lb.mu.RLock()
	route, exists := lb.partitionRoutes[partitionID]
	lb.mu.RUnlock()

	if !exists {
		return "", errors.New("partition not found")
	}

	// Write operations always go to leader
	if method == http.MethodPost || method == http.MethodDelete {
		return route.Leader, nil
	}

	// Read operations follow the configured strategy
	if method == http.MethodGet {
		return lb.selectReadNode(route)
	}

	return "", errors.New("unsupported method")
}

// selectReadNode chooses a node for read operations based on strategy
func (lb *LoadBalancer) selectReadNode(route *PartitionRoute) (string, error) {
	switch lb.readStrategy.Type {
	case "leader_only":
		return route.Leader, nil
	case "replica_only":
		if len(route.Replicas) == 0 {
			return route.Leader, nil
		}
		// Simple round-robin for now
		return route.Replicas[0], nil
	case "mixed":
		// Use replica weight to determine if request goes to replica
		if len(route.Replicas) > 0 && lb.readStrategy.ReplicaWeight > 0 {
			// Simple random selection for now
			return route.Replicas[0], nil
		}
		return route.Leader, nil
	default:
		return route.Leader, nil
	}
}

// forwardRequest sends the request to the target node
func (lb *LoadBalancer) forwardRequest(targetNode string, r *http.Request) (*http.Response, error) {
	// Create new request to forward
	req, err := http.NewRequest(r.Method, fmt.Sprintf("http://%s%s", targetNode, r.URL.Path), r.Body)
	if err != nil {
		return nil, err
	}

	// Copy headers
	for k, v := range r.Header {
		req.Header[k] = v
	}

	// Send request
	return lb.httpClient.Do(req)
}

// retryRequest attempts to retry a failed request
func (lb *LoadBalancer) retryRequest(targetNode string, r *http.Request) (*http.Response, error) {
	var lastErr error
	for i := 0; i < lb.retryConfig.MaxRetries; i++ {
		// Update target node in case it changed
		key := r.URL.Query().Get("key")
		partitionID := lb.getPartitionForKey(key)
		newTarget, err := lb.getTargetNode(partitionID, r.Method)
		if err != nil {
			return nil, err
		}

		// Try request with new target
		response, err := lb.forwardRequest(newTarget, r)
		if err == nil {
			return response, nil
		}
		lastErr = err

		// Wait before retry
		time.Sleep(lb.retryConfig.RetryDelay)
	}
	return nil, fmt.Errorf("max retries exceeded: %v", lastErr)
}

// shouldRetry determines if a request should be retried
func (lb *LoadBalancer) shouldRetry(method string) bool {
	// Only retry GET requests by default
	return method == http.MethodGet
}

// updatePartitionInfo fetches latest partition information from controller
func (lb *LoadBalancer) updatePartitionInfo() error {
	// Get partition info from controller
	partitions, err := lb.controller.GetPartitions()
	if err != nil {
		return err
	}

	// Update routing information
	lb.mu.Lock()
	defer lb.mu.Unlock()

	for _, p := range partitions {
		lb.partitionRoutes[p.ID] = &PartitionRoute{
			Leader:     p.Leader,
			Replicas:   p.Replicas,
			Status:     p.Status,
			LastUpdate: time.Now(),
		}
	}

	return nil
}

// periodicPartitionUpdate periodically updates partition information
func (lb *LoadBalancer) periodicPartitionUpdate() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := lb.updatePartitionInfo(); err != nil {
			// Log error but continue
			fmt.Printf("Failed to update partition info: %v\n", err)
		}
	}
}

// GetPartitionInfo returns current partition routing information
func (lb *LoadBalancer) GetPartitionInfo() map[int]*PartitionRoute {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	// Create a copy of the routes
	routes := make(map[int]*PartitionRoute)
	for k, v := range lb.partitionRoutes {
		routes[k] = v
	}
	return routes
}
