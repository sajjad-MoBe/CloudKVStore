package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:   "kvstore",
    Short: "A distributed key-value store",
    Long: `A distributed in-memory key-value store with support for 
clustering, partitioning, and replication.`,
}

func ExecuteServer() {
    if err := rootCmd.Execute(); err != nil {
        fmt.Println("couldn't execute app,", err)
        os.Exit(1)
    }
}

func init() {
    rootCmd.AddCommand(startNodeCmd)
}
