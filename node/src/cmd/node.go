package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var startNodeCmd = &cobra.Command{
	Use:   "node",
	Short: "to start node",
	Run: func(cmd *cobra.Command, args []string) {

		fmt.Println("node")
	},
}
