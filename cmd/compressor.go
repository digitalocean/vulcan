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
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/compressor"
	"github.com/gocql/gocql"
	hostpool "github.com/hailocab/go-hostpool"
	cg "github.com/supershabam/sarama-cg"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Compressor handles parsing the command line options, initializes, and starts the
// compressor service accordingling.
func Compressor() *cobra.Command {
	comp := &cobra.Command{
		Use:   "compressor",
		Short: "runs the compressor service to consume metrics from kafka into cassandra",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := sarama.NewConfig()
			cfg.Version = sarama.V0_10_0_0
			cfg.ClientID = viper.GetString(flagKafkaClientID)
			addrs := strings.Split(viper.GetString(flagKafkaAddrs), ",")
			client, err := sarama.NewClient(addrs, cfg)
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
			w := cassandra.NewWriter(&cassandra.WriterConfig{
				NumWorkers:    20,
				Session:       sess,
				TTL:           time.Hour * 5,
				CompressedTTL: time.Hour * 5,
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			coord := cg.NewCoordinator(&cg.CoordinatorConfig{
				Client:  client,
				Context: ctx,
				GroupID: viper.GetString(flagKafkaGroupID),
				Protocols: []cg.ProtocolKey{
					{
						Protocol: &cg.HashRing{},
						Key:      "hashring",
					},
				},
				SessionTimeout: 30 * time.Second,
				Heartbeat:      3 * time.Second,
				Topics:         []string{"vulcan"},
			})
			c, err := compressor.NewCompressor(&compressor.CompressorConfig{
				Client:           client,
				Coordinator:      coord,
				MaxDirtyDuration: 3 * time.Hour,
				MaxSampleDelta:   3 * time.Hour,
				Window:           2 * time.Hour,
				Writer:           w,
			})
			if err != nil {
				return err
			}
			return c.Run()
		},
	}

	comp.Flags().Duration(flagCassandraTimeout, time.Second*2, "cassandra timeout duration")
	comp.Flags().Int(flagCassandraNumConns, 2, "number of connections to cassandra per node")
	comp.Flags().String(flagCassandraAddrs, "", "one.example.com:9092,two.example.com:9092")
	comp.Flags().String(flagCassandraKeyspace, "vulcan", "cassandra keyspace to use")
	comp.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	comp.Flags().String(flagKafkaClientID, "vulcan-compressor", "set the kafka client id")
	comp.Flags().String(flagKafkaGroupID, "vulcan-compressor", "workers with the same groupID will join the same Kafka ConsumerGroup")
	comp.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")

	return comp
}
