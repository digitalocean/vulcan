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
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/digitalocean/vulcan/forwarder"
	"github.com/digitalocean/vulcan/kafka"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	flagAddress             = "address"
	flagCassandraAddrs      = "cassandra-addrs"
	flagCassandraKeyspace   = "cassandra-keyspace"
	flagCassandraTimeout    = "cassandra-timeout"
	flagCassandraNumConns   = "cassandra-num-conns"
	flagKafkaAddrs          = "kafka-addrs"
	flagKafkaClientID       = "kafka-client-id"
	flagKafkaGroupID        = "kafka-group-id"
	flagKafkaTopic          = "kafka-topic"
	flagNumCassandraWorkers = "num-cassandra-workers"
	flagNumKafkaWorkers     = "num-kafka-workers"
	flagNumWorkers          = "num-workers"
	flagTelemetryPath       = "telemetry-path"
	flagWebListenAddress    = "web-listen-address"
)

// Forwarder handles parsing the command line options, initializes, and starts the
// forwarder service accordingling.  It is the entry point for the forwarder
// service.
func Forwarder() *cobra.Command {
	f := &cobra.Command{
		Use:   "forwarder",
		Short: "forwards metric received from prometheus to message bus",
		RunE: func(cmd *cobra.Command, args []string) error {
			var (
				reg     = prometheus.NewRegistry()
				signals = make(chan os.Signal, 1)
			)

			signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
			// create upstream kafka writer to receive data
			w, err := kafka.NewWriter(&kafka.WriterConfig{
				ClientID: viper.GetString(flagKafkaClientID),
				Topic:    viper.GetString(flagKafkaTopic),
				Addrs:    strings.Split(viper.GetString(flagKafkaAddrs), ","),
			})
			if err != nil {
				return err
			}

			err = reg.Register(w)
			if err != nil {
				return err
			}

			log.WithFields(log.Fields{
				"kafka_client_id": viper.GetString(flagKafkaClientID),
				"kafka_topic":     viper.GetString(flagKafkaTopic),
				"kafka_addresses": viper.GetString(flagKafkaAddrs),
			}).Info("registered as kafka producer")

			fwd := forwarder.NewForwarder(&forwarder.Config{
				Writer: w,
			})

			err = reg.Register(fwd)
			if err != nil {
				return err
			}

			d := forwarder.NewDecompressor()

			lis, err := net.Listen("tcp", viper.GetString(flagAddress))
			if err != nil {
				return err
			}
			// start both gRPC and http servers and return as soon as either of them have an
			// error. It is expected that these two servers will either return an error, or run
			// indefinitely. This will be easier to handle when Prometheus moves away from gRPC
			// and to an HTTP POST for the generic write API https://github.com/prometheus/prometheus/pull/1957
			errCh := make(chan error)
			once := sync.Once{}
			go func() {
				server := grpc.NewServer(grpc.RPCDecompressor(d))
				remote.RegisterWriteServer(server, fwd)

				log.WithFields(log.Fields{
					"listening_address": viper.GetString(flagAddress),
				}).Info("starting vulcan forwarder gRPC service")
				if err := server.Serve(lis); err != nil {
					once.Do(func() {
						errCh <- err
					})
				}
			}()
			go func() {
				log.WithFields(log.Fields{
					"listening_address": viper.GetString(flagWebListenAddress),
					"telemetry_path":    viper.GetString(flagTelemetryPath),
				}).Info("starting http server for telemetry")
				http.Handle(viper.GetString(flagTelemetryPath), promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
				if err := http.ListenAndServe(viper.GetString(flagWebListenAddress), nil); err != nil {
					once.Do(func() {
						errCh <- err
					})
				}
			}()

			// listen for signals so can gracefully stop kakfa producer
			go func() {
				for _ = range signals {
					w.Stop()
					time.Sleep(2 * time.Second)
					os.Exit(0)
				}
			}()

			return <-errCh
		},
	}

	f.Flags().String(flagAddress, ":8888", "grpc server listening address")
	f.Flags().String(flagKafkaTopic, "vulcan", "kafka topic to write to")
	f.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	f.Flags().String(flagKafkaClientID, "vulcan-forwarder", "set the kafka client id")
	f.Flags().String(flagTelemetryPath, "/metrics", "path under which to expose metrics")
	f.Flags().String(flagWebListenAddress, ":9031", "address to listen on for telemetry")

	return f
}
