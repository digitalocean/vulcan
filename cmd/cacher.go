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
			// TODO make advertised addr more configurable..
			hostname, err := os.Hostname()
			if err != nil {
				return err
			}
			port := viper.GetInt(flagCacherPort)
			addr := fmt.Sprintf("%s:%d", hostname, port)
			ud, err := json.Marshal(model.UserData{
				AdvertisedAddr: addr,
			})
			if err != nil {
				return err
			}
			kafkaAddrs := strings.Split(viper.GetString(flagKafkaAddrs), ",")
			clientID := viper.GetString(flagKafkaClientID)
			groupID := viper.GetString(flagKafkaGroupID)

			logrus.WithFields(logrus.Fields{
				"kafka_addrs":     kafkaAddrs,
				"kafka_client_id": clientID,
				"kafka_group_id":  groupID,
			}).Info("starting cacher")

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
				SessionTimeout: viper.GetDuration(flagKafkaSession),
				Heartbeat:      viper.GetDuration(flagKafkaHeartbeat),
				Topics:         []string{viper.GetString(flagKafkaTopic)},
			})
			c, err := cacher.NewCacher(&cacher.Config{
				Cleanup:     viper.GetDuration(flagCacherCleanup),
				Client:      client,
				Coordinator: coord,
				MaxAge:      viper.GetDuration(flagCacherMaxAge),
				Topic:       viper.GetString(flagKafkaTopic),
			})
			if err != nil {
				return err
			}
			prometheus.MustRegister(c)
			go func() {
				http.Handle("/metrics", prometheus.Handler())
				http.Handle("/chunks", c)
				http.ListenAndServe(addr, nil)
			}()
			logrus.Info("running")
			return c.Run()
		},
	}

	cchr.Flags().Duration(flagCacherCleanup, time.Minute*10, "garbage collection interval for cacher")
	cchr.Flags().Duration(flagCacherMaxAge, time.Hour*4, "max age of samples to keep in-memory")
	cchr.Flags().Int(flagCacherPort, 8080, "port to listen on")
	cchr.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	cchr.Flags().String(flagKafkaClientID, "vulcan-cacher", "set the kafka client id")
	cchr.Flags().String(flagKafkaGroupID, "vulcan-cacher", "workers with the same groupID will join the same Kafka ConsumerGroup")
	cchr.Flags().Duration(flagKafkaHeartbeat, time.Second*3, "kafka consumer group heartbeat interval")
	cchr.Flags().Duration(flagKafkaSession, time.Second*30, "kafka consumer group session duration")
	cchr.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")

	return cchr
}
