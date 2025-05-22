package controller

import (
	"github.com/gorilla/mux"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/shared"
	"net/http"
	"sync"
	"time"
)

// Controller manages the cluster state
type Controller struct {
	state struct {
		mu         sync.RWMutex
		Nodes      map[string]*Node
		Partitions map[int]*Partition
	}
	// Add fields for partition and health management
	partitionManager *partition.PartitionManager
	healthManager    *shared.HealthManager
	router           *mux.Router
	stopCh           chan struct{}
	interval         time.Duration
}

// NewController creates a new controller
func NewController(partitionManager *partition.PartitionManager, healthManager *shared.HealthManager) *Controller {
	c := &Controller{
		partitionManager: partitionManager,
		healthManager:    healthManager,
		stopCh:           make(chan struct{}),
		router:           mux.NewRouter(),
		interval:         5 * time.Second,
	}
	c.state.Nodes = make(map[string]*Node)
	c.state.Partitions = make(map[int]*Partition)

	// Start failover monitor
	go c.startFailoverMonitor()

	c.setupRoutes()
	return c
}

// Start begins the controller's background tasks
func (c *Controller) Start(addr string) error {
	go c.healthCheckLoop()
	return http.ListenAndServe(addr, c.router)
}

// Stop gracefully stops the controller
func (c *Controller) Stop() {
	close(c.stopCh)
}
