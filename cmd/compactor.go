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
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/kafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	cg "github.com/supershabam/sarama-cg"
)

// Compactor returns a cobra command to handle options for running the compactor service
func Compactor() *cobra.Command {
	cmptr := &cobra.Command{
		Use:   "compactor",
		Short: "runs the compactor service to consume metrics from kafka into cassandra as compressed blobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			// cluster := gocql.NewCluster(strings.Split(viper.GetString(flagCassandraAddrs), ",")...)
			// cluster.Keyspace = viper.GetString(flagCassandraKeyspace)
			// cluster.Timeout = viper.GetDuration(flagCassandraTimeout)
			// cluster.NumConns = viper.GetInt(flagCassandraNumConns)
			// cluster.Consistency = gocql.LocalOne
			// cluster.ProtoVersion = 4
			// // Fallback simple host pool distributes queries and prevents sending queries to unresponsive hosts.
			// fallbackHostPolicy := gocql.HostPoolHostPolicy(hostpool.New(nil))
			// // Token-aware policy performs queries against a host responsible for the partition.
			// // TODO in gocql make token-aware able to write to any host for a partition when the
			// // replication factor is > 1.
			// // https://github.com/gocql/gocql/blob/4f49cd01c8939ce7624952fe286c3d08c4be7fa1/policies.go#L331
			// cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallbackHostPolicy)
			// sess, err := cluster.CreateSession()
			// if err != nil {
			// 	return err
			// }
			// w := cassandra.NewWriter(&cassandra.WriterConfig{
			// 	NumWorkers: viper.GetInt(flagNumCassandraWorkers),
			// 	Session:    sess,
			// 	TTL:        viper.GetDuration(flagCompressedTTL),
			// })
			// err = prometheus.Register(w)
			// if err != nil {
			// 	return err
			// }
			// s, err := kafka.NewSource(&kafka.SourceConfig{
			// 	Addrs:    strings.Split(viper.GetString(flagKafkaAddrs), ","),
			// 	ClientID: viper.GetString(flagKafkaClientID),
			// 	GroupID:  viper.GetString(flagKafkaGroupID),
			// 	Topics:   []string{viper.GetString(flagKafkaTopic)},
			// })
			// if err != nil {
			// 	return err
			// }
			// c := &compactor.NewCompactor(&compactor.Config{
			// 	Source: s,
			// 	Writer: w,
			// })
			// go func() {
			// 	http.Handle("/metrics", prometheus.Handler())
			// 	http.ListenAndServe(":8080", nil)
			// }()
			// return i.Run()

			cfg := sarama.NewConfig()
			cfg.Version = sarama.V0_9_0_0
			client, err := sarama.NewClient(strings.Split(viper.GetString(flagKafkaAddrs), ","), cfg)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithCancel(context.Background())
			// create coordinator
			coord := cg.NewCoordinator(&cg.Config{
				Client:  client,
				GroupID: viper.GetString(flagKafkaGroupID),
				// Protocols are how we agree on how to assign topic-partitions to consumers.
				// As long as every consumer in the group has at least 1 common protocol (determined by the key),
				// then the group will function.
				// A protocol is an interface, so I can implement my own.
				Protocols: []cg.ProtocolKey{
					{
						Protocol: &cg.HashRing{},
						Key:      "hashring",
					},
				},
				SessionTimeout: 30 * time.Second,
				Heartbeat:      3 * time.Second,
				Topics:         []string{viper.GetString(flagKafkaTopic)},
				// Consume is called every time we become responsible for a topic-partition.
				// This let's us implement our own logic of how to consume a partition.
				Consume: func(ctx context.Context, topic string, partition int32) {
					if topic != "vulcan" || partition != 0 {
						return
					}
					fmt.Printf("creating consumer %s-%d\n", topic, partition)
					go func() {
						bc, err := kafka.NewBackfillConsumer(&kafka.BackfillConsumerConfig{
							Client:    client,
							Ctx:       ctx,
							Topic:     topic,
							Partition: partition,
						})
						if err != nil {
							logrus.WithError(err).Error("could not create backfill consumer")
							return
						}
						count := 0
						for m := range bc.Messages() {
							logrus.WithFields(logrus.Fields{
								"offset": m.Offset,
							}).Debug("received message")
							count++
							if count > 25 {
								cancel()
							}
						}
						err = bc.Err()
						if err != nil {
							logrus.WithError(err).Error("could not create backfill consumer")
							return
						}
						fmt.Printf("closing consumer %s-%d\n", topic, partition)
					}()
				},
			})

			err = coord.Run(ctx)
			if err != nil {
				return err
			}
			return nil
		},
	}

	cmptr.Flags().Duration(flagCassandraTimeout, time.Second*2, "cassandra timeout duration")
	cmptr.Flags().Int(flagCassandraNumConns, 2, "number of connections to cassandra per node")
	cmptr.Flags().Int(flagNumCassandraWorkers, 200, "number of compactor goroutines to write to cassandra")
	cmptr.Flags().Int(flagNumKafkaWorkers, 30, "number of compactor goroutines to process kafka messages")
	cmptr.Flags().String(flagCassandraAddrs, "", "one.example.com:9092,two.example.com:9092")
	cmptr.Flags().String(flagCassandraKeyspace, "vulcan", "cassandra keyspace to use")
	cmptr.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	cmptr.Flags().String(flagKafkaClientID, "vulcan-compactor", "set the kafka client id")
	cmptr.Flags().String(flagKafkaGroupID, "vulcan-compactor", "workers with the same groupID will join the same Kafka ConsumerGroup")
	cmptr.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")
	cmptr.Flags().Duration(flagCompressedTTL, time.Hour*24*365, "uncompressed sample ttl")

	return cmptr
}
