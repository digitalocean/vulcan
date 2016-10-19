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
	"net/http"
	"strings"
	"time"

	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/ingester"
	"github.com/digitalocean/vulcan/kafka"
	"github.com/gocql/gocql"
	hostpool "github.com/hailocab/go-hostpool"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Ingester handles parsing the command line options, initializes, and starts the
// ingester service accordingling.
func Ingester() *cobra.Command {
	ingcmd := &cobra.Command{
		Use:   "ingester",
		Short: "runs the ingester service to consume metrics from kafka into cassandra",
		RunE: func(cmd *cobra.Command, args []string) error {
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
				NumWorkers: viper.GetInt(flagNumCassandraWorkers),
				Session:    sess,
				TTL:        viper.GetDuration(flagUncompressedTTL),
				TableName:  "uncompressed",
				Keyspace:   viper.GetString(flagCassandraKeyspace),
			})
			err = prometheus.Register(w)
			if err != nil {
				return err
			}
			s, err := kafka.NewSource(&kafka.SourceConfig{
				Addrs:    strings.Split(viper.GetString(flagKafkaAddrs), ","),
				ClientID: viper.GetString(flagKafkaClientID),
				GroupID:  viper.GetString(flagKafkaGroupID),
				Topics:   []string{viper.GetString(flagKafkaTopic)},
			})
			if err != nil {
				return err
			}
			i := &ingester.Ingester{
				NumWorkers: viper.GetInt(flagNumKafkaWorkers),
				Source:     s,
				Writer:     w,
			}
			go func() {
				http.Handle("/metrics", prometheus.Handler())
				http.ListenAndServe(":8080", nil)
			}()
			return i.Run()
		},
	}

	ingcmd.Flags().Duration(flagCassandraTimeout, time.Second*2, "cassandra timeout duration")
	ingcmd.Flags().Int(flagCassandraNumConns, 2, "number of connections to cassandra per node")
	ingcmd.Flags().Int(flagNumCassandraWorkers, 200, "number of ingester goroutines to write to cassandra")
	ingcmd.Flags().Int(flagNumKafkaWorkers, 30, "number of ingester goroutines to process kafka messages")
	ingcmd.Flags().String(flagCassandraAddrs, "", "one.example.com:9092,two.example.com:9092")
	ingcmd.Flags().String(flagCassandraKeyspace, "vulcan", "cassandra keyspace to use")
	ingcmd.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	ingcmd.Flags().String(flagKafkaClientID, "vulcan-ingest", "set the kafka client id")
	ingcmd.Flags().String(flagKafkaGroupID, "vulcan-ingester", "workers with the same groupID will join the same Kafka ConsumerGroup")
	ingcmd.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")
	ingcmd.Flags().Duration(flagUncompressedTTL, time.Hour*24*7, "uncompressed sample ttl")

	return ingcmd
}
