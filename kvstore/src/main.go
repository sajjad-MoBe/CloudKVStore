package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

func main() {
	// Parse command line arguments
	nodeID := flag.String("id", "node-1", "Node ID")
	port := flag.String("port", "8081", "Port to listen on")
	flag.Parse()

	// Create partition config
	config := partition.PartitionConfig{
		MaxMemTableSize: 1024 * 1024, // 1MB
		WALConfig: wal.WALConfig{
			MaxFileSize: 10 * 1024 * 1024, // 10MB
		},
	}

	// Create managers
	healthManager := shared.NewHealthManager(*nodeID)
	partitionManager := partition.NewPartitionManager(config, healthManager)

	// Create controller
	controller := controller.NewController(partitionManager, healthManager)

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the controller
	addr := ":" + *port
	log.Printf("Starting node %s on port %s...", *nodeID, *port)

	// Start controller in a goroutine
	go func() {
		if err := controller.Start(addr); err != nil {
			log.Printf("Controller error: %v", err)
			os.Exit(1)
		}
	}()

	// Start heartbeat goroutine
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Send heartbeat to controller
				heartbeatURL := fmt.Sprintf("http://localhost%s/nodes/%s/heartbeat", addr, *nodeID)
				_, err := http.Post(heartbeatURL, "application/json", nil)
				if err != nil {
					log.Printf("Failed to send heartbeat: %v", err)
				}
			case <-sigChan:
				return
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Printf("Shutting down node %s...", *nodeID)
	controller.Stop()
}
