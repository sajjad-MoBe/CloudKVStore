package shared

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/cmd/controller"
	"net/http"
	"time"
)

// Node represents a node in the cluster
type Node struct {
	ID         string                 `json:"id"`
	Address    string                 `json:"address"`
	Status     string                 `json:"status"` // "active", "failed", "joining"
	LastSeen   time.Time              `json:"last_seen"`
	Partitions []controller.Partition `json:"partitions"`
}

// handleGetNodeStatus handles GET /nodes/{id}/status requests
func (c *controller.Controller) handleGetNodeStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	c.state.mu.RLock()
	defer c.state.mu.RUnlock()

	node, exists := c.state.Nodes[nodeID]
	if !exists {
		http.Error(w, "Node not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    node.Status,
		"last_seen": node.LastSeen,
	})
}
