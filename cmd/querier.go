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
	"strings"
	"time"

	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/querier"
	"github.com/digitalocean/vulcan/storage/elasticsearch"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Querier handles parsing the command line options, initializes and starts the
// querier service accordingling.  It is the entry point for the Querier
// service.
func Querier() *cobra.Command {
	querier := &cobra.Command{
		Use:   "querier",
		Short: "runs the query service that implements PromQL and prometheus v1 api",
		RunE: func(cmd *cobra.Command, args []string) error {
			// bind pflags to viper so they are settable by env variables
			cmd.Flags().VisitAll(func(f *pflag.Flag) {
				viper.BindPFlag(f.Name, f)
			})
			// create elasticsearch metric resolver
			r, err := elasticsearch.NewResolver(&elasticsearch.ResolverConfig{
				URL:   viper.GetString("es"),
				Sniff: viper.GetBool("es-sniff"),
				Index: viper.GetString("es-index"),
			})
			if err != nil {
				return err
			}
			// create cassandra datapoint reader
			dpr, err := cassandra.NewDatapointReader(&cassandra.DatapointReaderConfig{
				CassandraAddrs: strings.Split(viper.GetString("cassandra-addrs"), ","),
				Keyspace:       viper.GetString("cassandra-keyspace"),
				Timeout:        10 * time.Second,
			})
			if err != nil {
				return err
			}
			q := querier.NewQuerier(&querier.Config{
				DatapointReader: dpr,
				Resolver:        r,
			})
			return q.Run()
		},
	}

	querier.Flags().String("cassandra-addrs", "", "cassandra01.example.com")
	querier.Flags().String("cassandra-keyspace", "vulcan", "cassandra keyspace to query")
	querier.Flags().String("es", "http://elasticsearch:9200", "elasticsearch connection url")
	querier.Flags().Bool("es-sniff", true, "whether or not to sniff additional hosts in the cluster")
	querier.Flags().String("es-index", "vulcan", "the elasticsearch index to write documents into")

	return querier
}
