package main

import (
	"flag"
	"log"
	"os"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/node"
)

func main() {
	// Set up logging to both file and stdout
	logFile, err := os.OpenFile("node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}
	defer logFile.Close()

	// Create multi-writer to log to both file and stdout
	log.SetOutput(os.Stdout)
	log.Println("Starting node service...")

	// Get node configuration from environment variables or flags
	nodeID := flag.String("id", os.Getenv("NODE_ID"), "Node ID")
	nodeAddr := flag.String("addr", os.Getenv("NODE_ADDRESS"), "Node address")
	controllerURL := flag.String("controller", os.Getenv("CONTROLLER_URL"), "Controller URL")
	flag.Parse()

	// Set default values if not provided
	if *nodeID == "" {
		*nodeID = "node1"
	}
	if *nodeAddr == "" {
		*nodeAddr = "localhost:8080"
	}
	if *controllerURL == "" {
		*controllerURL = "http://localhost:8080"
	}

	log.Printf("Node configuration:")
	log.Printf("  ID: %s", *nodeID)
	log.Printf("  Address: %s", *nodeAddr)
	log.Printf("  Controller URL: %s", *controllerURL)

	// Create and start node
	n := node.NewNode(*nodeID, *nodeAddr, *controllerURL)
	log.Printf("Node instance created, starting server...")

	if err := n.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	// Keep the main goroutine running
	select {}
}
