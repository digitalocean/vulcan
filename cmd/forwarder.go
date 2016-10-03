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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/digitalocean/vulcan/forwarder"
	"github.com/digitalocean/vulcan/kafka"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
				ClientID:    viper.GetString(flagKafkaClientID),
				Topic:       viper.GetString(flagKafkaTopic),
				Addrs:       strings.Split(viper.GetString(flagKafkaAddrs), ","),
				TrackWrites: viper.GetBool(flagKafkaTrackWrites),
				BatchSize:   viper.GetInt(flagKafkaBatchSize),
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

			// listen for signals so can gracefully stop kakfa producer
			go func() {
				<-signals
				// tell forwarder to stop handling in incoming requests
				fwd.Stop()
				// tell kafka writer to stop sending out producer messages
				w.Stop()
				os.Exit(0)
			}()

			http.Handle("/", forwarder.WriteHandler(fwd, "snappy"))
			http.Handle(viper.GetString(flagTelemetryPath), promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

			log.WithFields(log.Fields{
				"listening_address": viper.GetString(flagAddress),
			}).Info("starting vulcan forwarder http service")

			return http.ListenAndServe(viper.GetString(flagAddress), nil)
		},
	}

	f.Flags().String(flagAddress, ":8888", "server listening address")
	f.Flags().String(flagKafkaTopic, "vulcan", "kafka topic to write to")
	f.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	f.Flags().String(flagKafkaClientID, "vulcan-forwarder", "set the kafka client id")
	f.Flags().String(flagTelemetryPath, "/metrics", "path under which to expose metrics")
	f.Flags().Bool(flagKafkaTrackWrites, false, "track kafka writes for metric scraping and logging")
	f.Flags().Int(flagKafkaBatchSize, 65536, "batch size of each send of kafka producer messages")

	return f
}
