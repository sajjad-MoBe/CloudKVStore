package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{}

func ExecuteServer() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println("couldn't execute app,", err)
		os.Exit(1)
	}
}

func init() {

	rootCmd.AddCommand(startNodeCmd)

}
