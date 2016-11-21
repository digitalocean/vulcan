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
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/indexer"
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
			ctx, cancel := context.WithCancel(context.Background())
			term := make(chan os.Signal, 1)
			signal.Notify(term, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-term
				logrus.Info("shutting down...")
				cancel()
				<-term
				os.Exit(1)
			}()
			listenAddr := viper.GetString(flagAddress)
			kafkaAddrs := strings.Split(viper.GetString(flagKafkaAddrs), ",")
			clientID := viper.GetString(flagKafkaClientID)
			indexerGroupID := viper.GetString(flagIndexerGroupID)
			kafkaTopic := viper.GetString(flagKafkaTopic)
			logrus.WithFields(logrus.Fields{
				"listen_addr":      listenAddr,
				"kafka_addrs":      kafkaAddrs,
				"kafka_client_id":  clientID,
				"indexer_group_id": indexerGroupID,
				"kafka_topic":      kafkaTopic,
			}).Info("starting indexer")
			cfg := sarama.NewConfig()
			cfg.Version = sarama.V0_10_0_0
			cfg.ClientID = clientID
			client, err := sarama.NewClient(kafkaAddrs, cfg)
			if err != nil {
				return err
			}
			// create resolver that queries the in-memory indexer cluster.
			rslvr, err := indexer.NewResolver(&indexer.ResolverConfig{
				Client:  client,
				Context: ctx,
				GroupID: indexerGroupID,
				Topic:   kafkaTopic,
				Refresh: time.Minute,
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
				Resolver:        rslvr,
			})
			return q.Run()
		},
	}

	querier.Flags().String(flagAddress, ":9090", "address to listen on")
	querier.Flags().String(flagCassandraAddrs, "", "cassandra01.example.com")
	querier.Flags().String(flagCassandraKeyspace, "vulcan", "cassandra keyspace to query")
	querier.Flags().Int(flagCassandraPageSize, magicPageSize, "number of samples to read from cassandra at a time")
	querier.Flags().Float64(flagCassandraPrefetch, magicPrefetch, "prefetch next page when there are (prefetch * pageSize) number of rows remaining")
	querier.Flags().Int(flagCassandraNumConns, 2, "number of connections to cassandra per node")
	querier.Flags().Duration(flagCassandraTimeout, time.Second*2, "cassandra timeout duration")
	querier.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	querier.Flags().String(flagKafkaClientID, "vulcan-querier", "set the kafka client id")
	querier.Flags().String(flagIndexerGroupID, "vulcan-indexer", "workers with the same groupID will join the same Kafka ConsumerGroup")
	querier.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")

	return querier
}
