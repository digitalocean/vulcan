package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func Version(hash, version string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version number of vulcan",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("hash=%s\nversion=%s\n", hash, version)
		},
	}
}
