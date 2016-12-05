// Copyright 2016 The Vulcan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/digitalocean/vulcan/cmd"
)

var (
	gitSummary string
	goVersion  string
)

func main() {
	viper.SetEnvPrefix("vulcan")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// vulcan is the base command.
	var vulcan = &cobra.Command{
		Use:   "vulcan",
		Short: "vulcan is a distributed prometheus service",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// viper.BindPFlags allows flags to be set by environment variables
			if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
				return err
			}
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return err
			}

			// set the global log level
			lvl, err := log.ParseLevel(viper.GetString("log-level"))
			if err != nil {
				return err
			}
			log.SetLevel(lvl)
			return nil
		},
	}
	vulcan.PersistentFlags().String("log-level", "info", "The level of logging (panic|fatal|error|warn|info|debug)")

	vulcan.AddCommand(cmd.Cacher())
	vulcan.AddCommand(cmd.Indexer())
	vulcan.AddCommand(cmd.Querier())
	vulcan.AddCommand(cmd.Forwarder())
	vulcan.AddCommand(cmd.Downsampler())

	vulcan.AddCommand(cmd.Version(&cmd.VersionConfig{
		GitSummary: gitSummary,
		GoVersion:  goVersion,
	}))

	if err := vulcan.Execute(); err != nil {
		log.Fatal(err)
	}
}
