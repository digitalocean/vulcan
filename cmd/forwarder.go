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
	"strings"

	"google.golang.org/grpc"

	"github.com/digitalocean/vulcan/forwarder"
	"github.com/digitalocean/vulcan/kafka"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/prometheus/storage/remote"
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
			// create upstream kafka writer to receive data
			w, err := kafka.NewWriter(&kafka.WriterConfig{
				ClientID: viper.GetString("kafka-client-id"),
				Topic:    viper.GetString("kafka-topic"),
				Addrs:    strings.Split(viper.GetString("kafka-addrs"), ","),
			})
			if err != nil {
				return err
			}

			log.WithFields(log.Fields{
				"kafka_client_id": viper.GetString("kafka-client-id"),
				"kafka_topic":     viper.GetString("kafka-topic"),
				"kafka_addresses": viper.GetString("kafka-addrs"),
			}).Info("registered as kafka producer")

			bw := forwarder.NewForwarder(&forwarder.Config{Writer: w})

			d := forwarder.NewDecompressor()

			lis, err := net.Listen("tcp", viper.GetString("address"))
			if err != nil {
				return err
			}

			server := grpc.NewServer(grpc.RPCDecompressor(d))
			remote.RegisterWriteServer(server, bw)

			log.WithFields(log.Fields{
				"listening_address": viper.GetString("address"),
			}).Info("starting vulcan forwarder service")

			return server.Serve(lis)
		},
	}

	f.Flags().String("address", ":8888", "grpc server listening address")
	f.Flags().String("kafka-topic", "vulcan", "kafka topic to write to")
	f.Flags().String("kafka-addrs", "", "one.example.com:9092,two.example.com:9092")
	f.Flags().String("kafka-client-id", "vulcan-forwarder", "set the kafka client id")

	return f
}
