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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/cacher"
	"github.com/digitalocean/vulcan/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	cg "github.com/supershabam/sarama-cg"
	"github.com/supershabam/sarama-cg/protocol"
)

// Cacher is an in-memory cache for samples that the querier uses to serve queries for data
// that has not yet been compacted and persisted by the compactor.
func Cacher() *cobra.Command {
	cchr := &cobra.Command{
		Use:   "cacher",
		Short: "cacher keeps recent metrics from the bus in-memory",
		RunE: func(cmd *cobra.Command, args []string) error {
			advertisedAddr := viper.GetString(flagAdvertise)
			listenAddr := viper.GetString(flagAddress)
			kafkaAddrs := strings.Split(viper.GetString(flagKafkaAddrs), ",")
			clientID := viper.GetString(flagKafkaClientID)
			groupID := viper.GetString(flagKafkaGroupID)
			kafkaSessionTimeout := viper.GetDuration(flagKafkaSession)
			kafkaHeartbeat := viper.GetDuration(flagKafkaHeartbeat)
			kafkaTopic := viper.GetString(flagKafkaTopic)
			maxAge := viper.GetDuration(flagCacherMaxAge)
			cleanup := viper.GetDuration(flagCacherCleanup)

			logrus.WithFields(logrus.Fields{
				"advertised_addr":       advertisedAddr,
				"listen_addr":           listenAddr,
				"kafka_addrs":           kafkaAddrs,
				"kafka_client_id":       clientID,
				"kafka_group_id":        groupID,
				"kafka_heartbeat":       kafkaHeartbeat,
				"kafka_session_timeout": kafkaSessionTimeout,
				"kafka_topic":           kafkaTopic,
				"max_age":               maxAge,
			}).Info("starting cacher")

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
						Protocol: &protocol.HashRing{
							MyUserData: ud,
						},
						Key: "hashring",
					},
				},
				SessionTimeout: kafkaSessionTimeout,
				Heartbeat:      kafkaHeartbeat,
				Topics:         []string{kafkaTopic},
			})
			c, err := cacher.NewCacher(&cacher.Config{
				Cleanup:     cleanup,
				Client:      client,
				Coordinator: coord,
				MaxAge:      maxAge,
				Topic:       kafkaTopic,
			})
			if err != nil {
				return err
			}
			prometheus.MustRegister(c)
			// run http server in goroutine and close context and record error.
			var outerErr error
			go func() {
				defer cancel()
				http.Handle("/metrics", prometheus.Handler())
				http.Handle("/chunks", c)
				err = http.ListenAndServe(listenAddr, nil)
				if err != nil {
					outerErr = err
				}
			}()
			err = c.Run()
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
	cchr.Flags().String(flagAddress, ":8080", "address to listen on")
	cchr.Flags().String(flagAdvertise, addr, "address to advertise to others to connect to this cacher")
	cchr.Flags().Duration(flagCacherCleanup, time.Minute*10, "garbage collection interval for cacher")
	cchr.Flags().Duration(flagCacherMaxAge, time.Hour*4, "max age of samples to keep in-memory")
	cchr.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	cchr.Flags().String(flagKafkaClientID, "vulcan-cacher", "set the kafka client id")
	cchr.Flags().String(flagKafkaGroupID, "vulcan-cacher", "workers with the same groupID will join the same Kafka ConsumerGroup")
	cchr.Flags().Duration(flagKafkaHeartbeat, time.Second*3, "kafka consumer group heartbeat interval")
	cchr.Flags().Duration(flagKafkaSession, time.Second*30, "kafka consumer group session duration")
	cchr.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")

	return cchr
}
