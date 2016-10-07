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
	"github.com/digitalocean/vulcan/elasticsearch"
	"github.com/digitalocean/vulcan/querier"
	"github.com/gocql/gocql"
	hostpool "github.com/hailocab/go-hostpool"

	"github.com/spf13/cobra"
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
			// create elasticsearch metric resolver
			r, err := elasticsearch.NewResolver(&elasticsearch.ResolverConfig{
				URL:   viper.GetString(flagESAddrs),
				Sniff: viper.GetBool(flagESSniff),
				Index: viper.GetString(flagESIndex),
			})
			if err != nil {
				return err
			}
			cluster := gocql.NewCluster(strings.Split(viper.GetString(flagCassandraAddrs), ",")...)
			cluster.Keyspace = viper.GetString(flagCassandraKeyspace)
			cluster.Timeout = viper.GetDuration(flagCassandraTimeout)
			cluster.NumConns = viper.GetInt(flagCassandraNumConns)
			cluster.Consistency = gocql.LocalOne
			cluster.ProtoVersion = 4
			// Fallback simple host pool distributes queries and prevents sending queries to unresponsive hosts.
			fallbackHostPolicy := gocql.HostPoolHostPolicy(hostpool.New(nil))
			// Token-aware policy performs queries against a host responsible for the partition.
			// TODO in gocql make token-aware able to write to any host for a partition when the
			// replication factor is > 1.
			// https://github.com/gocql/gocql/blob/4f49cd01c8939ce7624952fe286c3d08c4be7fa1/policies.go#L331
			cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallbackHostPolicy)
			sess, err := cluster.CreateSession()
			if err != nil {
				return err
			}
			itrf := &cassandra.IteratorFactory{
				Session:  sess,
				PageSize: viper.GetInt(flagCassandraPageSize),
				Prefetch: viper.GetFloat64(flagCassandraPrefetch),
			}
			q := querier.NewQuerier(&querier.Config{
				IteratorFactory: itrf,
				Resolver:        r,
			})
			return q.Run()
		},
	}

	querier.Flags().String(flagCassandraAddrs, "", "cassandra01.example.com")
	querier.Flags().String(flagCassandraKeyspace, "vulcan", "cassandra keyspace to query")
	querier.Flags().Int(flagCassandraPageSize, magicPageSize, "number of samples to read from cassandra at a time")
	querier.Flags().Float64(flagCassandraPrefetch, magicPrefetch, "prefetch next page when there are (prefetch * pageSize) number of rows remaining")
	querier.Flags().Int(flagCassandraNumConns, 2, "number of connections to cassandra per node")
	querier.Flags().Duration(flagCassandraTimeout, time.Second*2, "cassandra timeout duration")
	querier.Flags().String(flagESAddrs, "http://elasticsearch:9200", "elasticsearch connection url")
	querier.Flags().Bool(flagESSniff, true, "whether or not to sniff additional hosts in the cluster")
	querier.Flags().String(flagESIndex, "vulcan", "the elasticsearch index to write documents into")

	return querier
}
