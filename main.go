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
	vulcan.AddCommand(cmd.Querier())

	vulcan.AddCommand(cmd.Version(hash, version))

	if err := vulcan.Execute(); err != nil {
		log.Fatal(err)
	}
}
