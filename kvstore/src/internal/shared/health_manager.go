package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// HealthStatus represents the health status of a component
type HealthStatus struct {
	Status    string    `json:"status"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Details   any       `json:"details,omitempty"`
}

// HealthChecker defines the interface for health checks
type HealthChecker interface {
	Check(ctx context.Context) HealthStatus
}

// HealthManager manages health checks
type HealthManager struct {
	mu       sync.RWMutex
	checkers map[string]HealthChecker
	status   map[string]HealthStatus
	nodeID   string
	// Add fields for leader tracking
	leaderStatus map[int]string // partitionID -> leaderID
	stopCh       chan struct{}
	// Add fields for health check configuration
	checkInterval time.Duration
	maxRetries    int
	retryDelay    time.Duration
	logger        *Logger
}

// HealthCheckHandler handles health check requests
func (hm *HealthManager) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hm.RunHealthChecks(ctx)
	status := hm.GetStatus()

	// Determine overall status
	overallStatus := "ok"
	for _, s := range status {
		if s.Status != "ok" {
			overallStatus = "error"
			break
		}
	}

	response := map[string]interface{}{
		"status":     overallStatus,
		"timestamp":  time.Now(),
		"components": status,
	}

	w.Header().Set("Content-Type", "application/json")
	if overallStatus == "error" {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		return
	}
}

// GetNodeID returns the ID of this node
func (h *HealthManager) GetNodeID() string {
	return h.nodeID
}

// StorageHealthChecker checks storage health
type StorageHealthChecker struct {
	store StorageEngine
}

// NewStorageHealthChecker creates a new storage health checker
func NewStorageHealthChecker(store StorageEngine) *StorageHealthChecker {
	return &StorageHealthChecker{store: store}
}

// Check implements HealthChecker
func (c *StorageHealthChecker) Check(ctx context.Context) HealthStatus {
	start := time.Now()

	// Try to perform a test write and read operation
	testKey := fmt.Sprintf("__health_check_%d__", time.Now().UnixNano())
	testValue := []byte("health_check_value")

	// Test write operation
	if err := c.store.Set(testKey, testValue); err != nil {
		return HealthStatus{
			Status:    "error",
			Message:   "Storage write health check failed",
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"error":     err.Error(),
				"duration":  time.Since(start).String(),
				"operation": "write",
			},
		}
	}

	// Test read operation
	value, err := c.store.Get(testKey)
	if err != nil {
		return HealthStatus{
			Status:    "error",
			Message:   "Storage read health check failed",
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"error":     err.Error(),
				"duration":  time.Since(start).String(),
				"operation": "read",
			},
		}
	}

	// Verify the value
	if !bytes.Equal(value, testValue) {
		return HealthStatus{
			Status:    "error",
			Message:   "Storage data integrity check failed",
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"error":     "read value does not match written value",
				"duration":  time.Since(start).String(),
				"operation": "verify",
			},
		}
	}

	// Clean up the test key
	_ = c.store.Delete(testKey)

	return HealthStatus{
		Status:    "ok",
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"duration":   time.Since(start).String(),
			"operations": []string{"write", "read", "verify"},
		},
	}
}

// AuthHealthChecker checks authentication health
type AuthHealthChecker struct {
	auth *AuthManager
}

// NewAuthHealthChecker creates a new auth health checker
func NewAuthHealthChecker(auth *AuthManager) *AuthHealthChecker {
	return &AuthHealthChecker{auth: auth}
}

// Check implements HealthChecker
func (c *AuthHealthChecker) Check(ctx context.Context) HealthStatus {
	start := time.Now()

	// Test user creation
	testUserID := fmt.Sprintf("__health_check_%d__", time.Now().UnixNano())
	user, err := c.auth.CreateUser(testUserID, []Role{RoleRead})
	if err != nil {
		return HealthStatus{
			Status:    "error",
			Message:   "Auth user creation check failed",
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"error":     err.Error(),
				"duration":  time.Since(start).String(),
				"operation": "create_user",
			},
		}
	}

	// Test API key generation
	if user.APIKey == "" {
		return HealthStatus{
			Status:    "error",
			Message:   "Auth API key generation failed",
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"error":     "empty API key generated",
				"duration":  time.Since(start).String(),
				"operation": "generate_key",
			},
		}
	}

	// Test authentication
	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", user.APIKey)

	authenticatedUser, err := c.auth.Authenticate(req)
	if err != nil {
		return HealthStatus{
			Status:    "error",
			Message:   "Auth authentication check failed",
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"error":     err.Error(),
				"duration":  time.Since(start).String(),
				"operation": "authenticate",
			},
		}
	}

	// Test authorization
	if !c.auth.Authorize(authenticatedUser, RoleRead) {
		return HealthStatus{
			Status:    "error",
			Message:   "Auth authorization check failed",
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"error":     "user not authorized for RoleRead",
				"duration":  time.Since(start).String(),
				"operation": "authorize",
			},
		}
	}

	return HealthStatus{
		Status:    "ok",
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"duration":   time.Since(start).String(),
			"operations": []string{"create_user", "generate_key", "authenticate", "authorize"},
		},
	}
}

// NewHealthManager creates a new health manager
func NewHealthManager(nodeID string) *HealthManager {
	hm := &HealthManager{
		checkers:      make(map[string]HealthChecker),
		status:        make(map[string]HealthStatus),
		nodeID:        nodeID,
		leaderStatus:  make(map[int]string),
		stopCh:        make(chan struct{}),
		checkInterval: 5 * time.Second,
		maxRetries:    3,
		retryDelay:    1 * time.Second,
		logger:        DefaultLogger,
	}
	// Start periodic health checks
	go hm.startPeriodicChecks()
	return hm
}

// startPeriodicChecks starts periodic health checks
func (hm *HealthManager) startPeriodicChecks() {
	ticker := time.NewTicker(hm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx := context.Background()
			hm.RunHealthChecks(ctx)
		case <-hm.stopCh:
			return
		}
	}
}

// Stop stops the health manager
func (hm *HealthManager) Stop() {
	close(hm.stopCh)
}

// UpdateLeaderStatus updates the leader status for a partition
func (hm *HealthManager) UpdateLeaderStatus(partitionID int, leaderID string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.leaderStatus[partitionID] = leaderID
}

// GetLeaderStatus returns the current leader for a partition
func (hm *HealthManager) GetLeaderStatus(partitionID int) string {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.leaderStatus[partitionID]
}

// IsLeader checks if this node is the leader for a partition
func (hm *HealthManager) IsLeader(partitionID int) bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.leaderStatus[partitionID] == hm.nodeID
}

// RegisterChecker registers a health checker
func (hm *HealthManager) RegisterChecker(name string, checker HealthChecker) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.checkers[name] = checker
}

// RunHealthChecks runs all registered health checks with retries
func (hm *HealthManager) RunHealthChecks(ctx context.Context) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	for name, checker := range hm.checkers {
		var lastErr error
		var status HealthStatus

		// Retry health checks
		for attempt := 0; attempt < hm.maxRetries; attempt++ {
			if attempt > 0 {
				hm.logger.Warn("Retrying health check for %s (attempt %d/%d) after error: %v",
					name, attempt+1, hm.maxRetries, lastErr)
				time.Sleep(hm.retryDelay)
			}

			status = checker.Check(ctx)
			if status.Status == "ok" {
				break
			}
			lastErr = fmt.Errorf("health check failed: %s", status.Message)
		}

		// Update status
		hm.status[name] = status

		// Log health check results
		if status.Status == "ok" {
			hm.logger.Info("Health check passed for %s", name)
		} else {
			hm.logger.Error("Health check failed for %s: %v", name, lastErr)
		}
	}
}

// GetStatus returns the current health status with additional metadata
func (hm *HealthManager) GetStatus() map[string]HealthStatus {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	status := make(map[string]HealthStatus)
	for k, v := range hm.status {
		status[k] = v
	}

	// Add metadata about the health check system
	status["_metadata"] = HealthStatus{
		Status:    "ok",
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"check_interval": hm.checkInterval.String(),
			"max_retries":    hm.maxRetries,
			"retry_delay":    hm.retryDelay.String(),
			"node_id":        hm.nodeID,
		},
	}

	return status
}

// SetCheckInterval sets the interval between health checks
func (hm *HealthManager) SetCheckInterval(interval time.Duration) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.checkInterval = interval
}

// SetMaxRetries sets the maximum number of retries for health checks
func (hm *HealthManager) SetMaxRetries(maxRetries int) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.maxRetries = maxRetries
}

// SetRetryDelay sets the delay between retries
func (hm *HealthManager) SetRetryDelay(delay time.Duration) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.retryDelay = delay
}

// SetLogger sets the logger for the health manager
func (hm *HealthManager) SetLogger(logger *Logger) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.logger = logger
}

// SetNodeStatus sets the health status for a node
func (hm *HealthManager) SetNodeStatus(nodeID string, status, message string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.status[nodeID] = HealthStatus{
		Status:    status,
		Message:   message,
		Timestamp: time.Now(),
	}
}
