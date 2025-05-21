package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/storage"
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
}

// NewHealthManager creates a new health manager
func NewHealthManager(nodeID string) *HealthManager {
	hm := &HealthManager{
		checkers:     make(map[string]HealthChecker),
		status:       make(map[string]HealthStatus),
		nodeID:       nodeID,
		leaderStatus: make(map[int]string),
		stopCh:       make(chan struct{}),
	}
	// Start periodic health checks
	go hm.startPeriodicChecks()
	return hm
}

// startPeriodicChecks starts periodic health checks
func (hm *HealthManager) startPeriodicChecks() {
	ticker := time.NewTicker(5 * time.Second)
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

// RunHealthChecks runs all registered health checks
func (hm *HealthManager) RunHealthChecks(ctx context.Context) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	for name, checker := range hm.checkers {
		hm.status[name] = checker.Check(ctx)
	}
}

// GetStatus returns the current health status
func (hm *HealthManager) GetStatus() map[string]HealthStatus {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	status := make(map[string]HealthStatus)
	for k, v := range hm.status {
		status[k] = v
	}
	return status
}

// StorageHealthChecker checks storage health
type StorageHealthChecker struct {
	store storage.StorageEngine
}

// NewStorageHealthChecker creates a new storage health checker
func NewStorageHealthChecker(store storage.StorageEngine) *StorageHealthChecker {
	return &StorageHealthChecker{store: store}
}

// Check implements HealthChecker
func (c *StorageHealthChecker) Check(ctx context.Context) HealthStatus {
	start := time.Now()
	_, err := c.store.Get("__health_check__")
	duration := time.Since(start)

	if err != nil {
		return HealthStatus{
			Status:    "error",
			Message:   "Storage health check failed",
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"error":    err.Error(),
				"duration": duration.String(),
			},
		}
	}

	return HealthStatus{
		Status:    "ok",
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"duration": duration.String(),
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
	// Check if auth manager is responsive
	// This is a simple check that could be expanded
	return HealthStatus{
		Status:    "ok",
		Timestamp: time.Now(),
	}
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
