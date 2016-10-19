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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/downsampler"
	"github.com/digitalocean/vulcan/kafka"
	"github.com/gocql/gocql"
	hostpool "github.com/hailocab/go-hostpool"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Downsampler handles parsing the command line options, initializes, and starts the
// downnsampler service accordingling.  It is the entry point for the downnsampler
// service.
func Downsampler() *cobra.Command {
	d := &cobra.Command{
		Use:   "downsampler",
		Short: "consumes timeseries sent by the forwarder and stores them at the resolution configured to the timeseries storage",
		RunE: func(cmd *cobra.Command, args []string) error {
			var (
				reg     = prometheus.NewRegistry()
				signals = make(chan os.Signal, 1)
			)

			signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

			// get kafka source
			s, err := kafka.NewSource(&kafka.SourceConfig{
				Addrs:    strings.Split(viper.GetString(flagKafkaAddrs), ","),
				ClientID: viper.GetString(flagKafkaClientID),
				GroupID:  viper.GetString(flagKafkaGroupID),
				Topics:   []string{viper.GetString(flagKafkaTopic)},
			})
			if err != nil {
				return err
			}
			//reg.MustRegister(s)
			log.WithFields(log.Fields{
				"kafka_client_id": viper.GetString(flagKafkaClientID),
				"kafka_topic":     viper.GetString(flagKafkaTopic),
				"kafka_addresses": viper.GetString(flagKafkaAddrs),
			}).Info("registered as kafka consumer")

			// Setup Cassandra writer
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

			tablename := fmt.Sprintf("downsampler_%v", viper.GetDuration(flagDownsamplerResolution))
			keyspace := viper.GetString(flagCassandraKeyspace)

			w := cassandra.NewWriter(&cassandra.WriterConfig{
				NumWorkers: viper.GetInt(flagNumCassandraWorkers),
				Session:    sess,
				TTL:        viper.GetDuration(flagDownsampledTTL),
				TableName:  tablename,
				Keyspace:   keyspace,
			})
			err = prometheus.Register(w)
			if err != nil {
				return err
			}

			// create reader
			r := cassandra.NewReader(&cassandra.ReaderConfig{
				Session:   sess,
				TableName: tablename,
				Keyspace:  keyspace,
			})

			log.WithFields(log.Fields{
				"cassandra_address":  viper.GetString(flagCassandraAddrs),
				"cassandra_keyspace": viper.GetString(flagCassandraKeyspace),
				"cassandra_ttl":      viper.GetString(flagDownsampledTTL),
			})

			ds := downsampler.NewDownsampler(&downsampler.Config{
				Consumer:   s,
				Writer:     w,
				Reader:     r,
				Resolution: viper.GetDuration(flagDownsamplerResolution),
			})
			reg.MustRegister(ds)

			// listen for signals so can gracefully stop kakfa producer
			go func() {
				<-signals
				// tell forwarder to stop handling in incoming requests
				ds.Stop()
				// tell kafka writer to stop sending out producer messages
				os.Exit(0)
			}()

			http.Handle(viper.GetString(flagTelemetryPath), promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
			go http.ListenAndServe(viper.GetString(flagAddress), nil)

			log.WithFields(log.Fields{
				"downsampler_resolution": viper.GetString(flagDownsamplerResolution),
			}).Info("downsampler started")

			// Chose 30 workers for sake of testing.
			return ds.Run(30)
		},
	}

	d.Flags().String(flagKafkaTopic, "vulcan", "kafka topic to write to")
	d.Flags().String(flagAddress, ":8888", "server listening address")
	d.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	d.Flags().String(flagKafkaClientID, "vulcan-downsampler", "set the kafka client id")
	d.Flags().String(flagKafkaGroupID, "vulcan-downsampler", "workers with the same groupID will join the same Kafka ConsumerGroup")
	d.Flags().String(flagTelemetryPath, "/metrics", "path under which to expose metrics")
	d.Flags().Bool(flagKafkaTrackWrites, false, "track kafka writes for metric scraping and logging")
	d.Flags().Int(flagKafkaBatchSize, 65536, "batch size of each send of kafka producer messages")
	d.Flags().Duration(flagCassandraTimeout, time.Second*2, "cassandra timeout duration")
	d.Flags().Int(flagCassandraNumConns, 5, "number of connections to cassandra per node")
	d.Flags().Int(flagNumCassandraWorkers, 200, "number of ingester goroutines to write to cassandra")
	d.Flags().String(flagCassandraAddrs, "", "one.example.com:9092,two.example.com:9092")
	d.Flags().String(flagCassandraKeyspace, "vulcan", "cassandra keyspace to use")
	d.Flags().Duration(flagDownsampledTTL, time.Hour*24*30, "downsampled sample ttl")
	d.Flags().Duration(flagDownsamplerResolution, time.Minute*15, "resolution duration")
	d.Flags().Int(flagNumWorkers, 30, "number of concurrent downsampler workers")

	return d
}
