package main

import (
	"log"
	"os"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

func main() {
	// Get controller address from environment variable or use default
	controllerAddr := os.Getenv("CONTROLLER_ADDRESS")
	if controllerAddr == "" {
		controllerAddr = ":8080"
	}

	// Initialize partition manager with default config
	config := partition.PartitionConfig{
		MaxMemTableSize: 1024 * 1024, // 1MB
		WALConfig: wal.WALConfig{
			MaxFileSize: 10 * 1024 * 1024, // 10MB
		},
	}

	// Create health manager
	healthManager := shared.NewHealthManager("controller")

	// Create partition manager
	partitionManager := partition.NewPartitionManager(config, healthManager)

	// Create and start controller
	ctrl := controller.NewController(partitionManager, healthManager)
	log.Printf("Starting controller on %s", controllerAddr)
	if err := ctrl.Start(controllerAddr); err != nil {
		log.Fatalf("Failed to start controller: %v", err)
	}
}
