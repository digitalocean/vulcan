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

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// VersionConfig is used to create a Version since the parameters are all
// strings and easy to order wrong.
type VersionConfig struct {
	GitSummary string
	GoVersion  string
}

// Version handles the command line option for the Vulcan version.
func Version(config *VersionConfig) *cobra.Command {
	v := &cobra.Command{
		Use:   "version",
		Short: "Version prints the Vulcan version.",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("%s\n\nGitSummary: %s\nGoVersion: %s\n", config.GitSummary, config.GitSummary, config.GoVersion)
		},
	}
	return v
}
