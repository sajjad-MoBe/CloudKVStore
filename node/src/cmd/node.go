package cmd

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/server"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/storage"

	"github.com/spf13/cobra"
)

var (
    nodeAddress string
)

var startNodeCmd = &cobra.Command{
    Use:   "start",
    Short: "Start the KV store node",
    Run:   runNode,
}

func init() {
    startNodeCmd.Flags().StringVarP(&nodeAddress, "address", "a", ":8080", "Address for the node server to listen on")
}

func runNode(cmd *cobra.Command, args []string) {
    // Create storage
    store := storage.NewSinglePartitionStore()

    // Create server
    srv := server.NewServer(store, nodeAddress)

    // Setup graceful shutdown
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Handle shutdown signals
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    // Start server in goroutine
    go func() {
        log.Printf("Starting server on %s\n", nodeAddress)
        if err := srv.Start(); err != nil {
            log.Printf("Server error: %v\n", err)
            cancel()
        }
    }()

    // Wait for shutdown signal
    select {
    case sig := <-sigChan:
        log.Printf("Received signal %v, initiating shutdown\n", sig)
    case <-ctx.Done():
        log.Println("Shutting down due to error")
    }

    // Perform graceful shutdown
    if err := srv.Shutdown(context.Background()); err != nil {
        log.Printf("Error during shutdown: %v\n", err)
    }
}
