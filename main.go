package main

import (
	"log"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/digitalocean/vulcan/cmd"
)

var (
	hash    string
	version string
)

func main() {
	viper.SetEnvPrefix("vulcan")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// vulcan is the base command.
	var vulcan = &cobra.Command{
		Use:   "vulcan",
		Short: "vulcan is a distributed prometheus service",
	}

	vulcan.AddCommand(cmd.Indexer())
	vulcan.AddCommand(cmd.Ingester)
	vulcan.AddCommand(cmd.Querier)
	vulcan.AddCommand(cmd.Scraper)

	vulcan.AddCommand(cmd.Job)
	vulcan.AddCommand(cmd.Version(hash, version))

	if err := vulcan.Execute(); err != nil {
		log.Fatal(err)
	}
}
