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
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/indexer"
	"github.com/digitalocean/vulcan/model"
	cg "github.com/supershabam/sarama-cg"
	"github.com/supershabam/sarama-cg/protocol"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Indexer handles parsing the command line options, initializes, and starts the
// indexer service accordingling.  It is the entry point for the Indexer
// service.
func Indexer() *cobra.Command {
	var idxr = &cobra.Command{
		Use:   "indexer",
		Short: "consumes metrics from the bus and makes them searchable",
		RunE: func(cmd *cobra.Command, args []string) error {
			advertisedAddr := viper.GetString(flagAdvertise)
			listenAddr := viper.GetString(flagAddress)
			kafkaAddrs := strings.Split(viper.GetString(flagKafkaAddrs), ",")
			clientID := viper.GetString(flagKafkaClientID)
			groupID := viper.GetString(flagKafkaGroupID)
			kafkaSessionTimeout := viper.GetDuration(flagKafkaSession)
			kafkaHeartbeat := viper.GetDuration(flagKafkaHeartbeat)
			kafkaTopic := viper.GetString(flagKafkaTopic)

			logrus.WithFields(logrus.Fields{
				"advertised_addr":       advertisedAddr,
				"listen_addr":           listenAddr,
				"kafka_addrs":           kafkaAddrs,
				"kafka_client_id":       clientID,
				"kafka_group_id":        groupID,
				"kafka_heartbeat":       kafkaHeartbeat,
				"kafka_session_timeout": kafkaSessionTimeout,
				"kafka_topic":           kafkaTopic,
			}).Info("starting indexer")

			ud, err := json.Marshal(model.UserData{
				AdvertisedAddr: advertisedAddr,
			})
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
			coord := cg.NewCoordinator(&cg.CoordinatorConfig{
				Client:  client,
				Context: ctx,
				GroupID: groupID,
				Protocols: []cg.ProtocolKey{
					{
						Protocol: &protocol.RoundRobin{
							MyUserData: ud,
						},
						Key: "roundrobin",
					},
				},
				SessionTimeout: kafkaSessionTimeout,
				Heartbeat:      kafkaHeartbeat,
				Topics:         []string{kafkaTopic},
			})
			i, err := indexer.NewIndexer(&indexer.Config{
				Client:      client,
				Coordinator: coord,
			})
			if err != nil {
				return err
			}
			lis, err := net.Listen("tcp", listenAddr)
			if err != nil {
				return err
			}
			s := grpc.NewServer()
			indexer.RegisterResolverServer(s, i)
			// run http server in goroutine and close context and record error.
			var outerErr error
			go func() {
				defer cancel()
				err := s.Serve(lis)
				if err != nil {
					outerErr = err
				}
			}()
			err = i.Run()
			if err != nil {
				return err
			}
			return outerErr
		},
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	addr := fmt.Sprintf("%s:%d", hostname, 8080)
	idxr.Flags().String(flagAddress, ":8080", "address to listen on")
	idxr.Flags().String(flagAdvertise, addr, "address to advertise to others to connect to this cacher")
	idxr.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	idxr.Flags().String(flagKafkaClientID, "vulcan-indexer", "set the kafka client id")
	idxr.Flags().String(flagKafkaGroupID, "vulcan-indexer", "workers with the same groupID will join the same Kafka ConsumerGroup")
	idxr.Flags().Duration(flagKafkaHeartbeat, time.Second*3, "kafka consumer group heartbeat interval")
	idxr.Flags().Duration(flagKafkaSession, time.Second*30, "kafka consumer group session duration")
	idxr.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")

	return idxr
}
