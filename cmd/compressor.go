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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/compressor"
	"github.com/gocql/gocql"
	hostpool "github.com/hailocab/go-hostpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	cg "github.com/supershabam/sarama-cg"
	"github.com/supershabam/sarama-cg/protocol"
)

func Compressor() *cobra.Command {
	cmpr := &cobra.Command{
		Use:   "compressor",
		Short: "compressor reads datapoints from the bus and persists a compressed blob of samples",
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

			cassandraAddrs := strings.Split(viper.GetString(flagCassandraAddrs), ",")
			cassandraKeyspace := viper.GetString(flagCassandraKeyspace)
			cassandraTimeout := viper.GetDuration(flagCassandraTimeout)
			cassandraConn := viper.GetInt(flagCassandraNumConns)
			webListenAddr := viper.GetString(flagWebListenAddress)
			kafkaAddrs := strings.Split(viper.GetString(flagKafkaAddrs), ",")
			clientID := viper.GetString(flagKafkaClientID)
			groupID := viper.GetString(flagKafkaGroupID)
			kafkaSessionTimeout := viper.GetDuration(flagKafkaSession)
			kafkaHeartbeat := viper.GetDuration(flagKafkaHeartbeat)
			kafkaTopic := viper.GetString(flagKafkaTopic)
			maxAge := viper.GetDuration(flagMaxAge)
			maxIdle := viper.GetDuration(flagMaxIdle)

			logrus.WithFields(logrus.Fields{
				"web_listen_addr":       webListenAddr,
				"kafka_addrs":           kafkaAddrs,
				"kafka_client_id":       clientID,
				"kafka_group_id":        groupID,
				"kafka_heartbeat":       kafkaHeartbeat,
				"kafka_session_timeout": kafkaSessionTimeout,
				"kafka_topic":           kafkaTopic,
				"cassandra_addrs":       cassandraAddrs,
				"cassandra_keyspace":    cassandraKeyspace,
				"cassandra_timeout":     cassandraTimeout,
				"max_age":               maxAge,
				"max_idle":              maxIdle,
			}).Info("starting cacher")

			// Setup Cassandra writer
			cluster := gocql.NewCluster(cassandraAddrs...)
			cluster.Keyspace = cassandraKeyspace
			cluster.Timeout = cassandraTimeout
			cluster.NumConns = cassandraConn
			cluster.Consistency = gocql.LocalOne
			cluster.ProtoVersion = 4
			// Fallback simple host pool distributes queries and prevents sending queries to unresponsive hosts.
			fallbackHostPolicy := gocql.HostPoolHostPolicy(hostpool.New(nil))
			cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallbackHostPolicy)
			sess, err := cluster.CreateSession()
			if err != nil {
				return err
			}
			cfg := sarama.NewConfig()
			cfg.Version = sarama.V0_10_0_0
			cfg.ClientID = clientID
			client, err := sarama.NewClient(kafkaAddrs, cfg)
			if err != nil {
				return err
			}
			coord := cg.NewCoordinator(&cg.CoordinatorConfig{
				Client:  client,
				Context: ctx,
				GroupID: groupID,
				Protocols: []cg.ProtocolKey{
					{
						Protocol: &protocol.RoundRobin{},
						Key:      "roundrobin",
					},
				},
				SessionTimeout: kafkaSessionTimeout,
				Heartbeat:      kafkaHeartbeat,
				Topics:         []string{kafkaTopic},
			})
			c, err := compressor.NewCompressor(&compressor.CompressorConfig{
				Client:      client,
				Coordinator: coord,
				GroupID:     groupID,
				MaxAge:      maxAge,
				MaxIdle:     maxIdle,
				Session:     sess,
				TTL:         time.Hour * 24 * 3,
			})
			prometheus.MustRegister(c)
			var outerErr error
			// run http server in goroutine and allow it to close context and record error if any.
			go func() {
				defer cancel()
				http.Handle("/metrics", prometheus.Handler())
				err := http.ListenAndServe(webListenAddr, nil)
				if err != nil {
					outerErr = err
				}
			}()
			// run cacher service until it's done (will stop when context is canceled)
			err = c.Run()
			if err != nil {
				// if cacher failed, return its error
				return err
			}
			// otherwise, the cacher stopped because the context canceled because of grpc/http error or shutdown.
			return outerErr
		},
	}

	cmpr.Flags().Duration(flagMaxIdle, time.Minute*30, "maximum duration between an identical metric before it is flushed")
	cmpr.Flags().Duration(flagMaxAge, time.Hour*2, "maximum age of a metric before it is flushed")
	cmpr.Flags().Duration(flagCassandraTimeout, time.Second*2, "cassandra timeout duration")
	cmpr.Flags().Int(flagCassandraNumConns, 5, "number of connections to cassandra per node")
	cmpr.Flags().Int(flagNumCassandraWorkers, 200, "number of ingester goroutines to write to cassandra")
	cmpr.Flags().String(flagCassandraAddrs, "", "one.example.com:9092,two.example.com:9092")
	cmpr.Flags().String(flagCassandraKeyspace, "vulcan", "cassandra keyspace to use")
	cmpr.Flags().String(flagWebListenAddress, ":8080", "web address for telemetry")
	cmpr.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	cmpr.Flags().String(flagKafkaClientID, "vulcan-cacher", "set the kafka client id")
	cmpr.Flags().String(flagKafkaGroupID, "vulcan-cacher", "workers with the same groupID will join the same Kafka ConsumerGroup")
	cmpr.Flags().Duration(flagKafkaHeartbeat, time.Second*3, "kafka consumer group heartbeat interval")
	cmpr.Flags().Duration(flagKafkaSession, time.Second*30, "kafka consumer group session duration")
	cmpr.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")

	return cmpr
}
