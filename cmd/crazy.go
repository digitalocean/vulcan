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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/crazy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	cg "github.com/supershabam/sarama-cg"
)

// Crazy is an attempt to serve "real-time" non-compressed metrics from memory instead
// of persisting all these pesky non-compressed datapoints to cassandra. Kafaka should
// provide our commit log. Compressor should be the long term store with efficient writes.
func Crazy() *cobra.Command {
	crzy := &cobra.Command{
		Use:   "crazy",
		Short: "crazy is an in-memory ttl-ed compressed metric database",
		RunE: func(cmd *cobra.Command, args []string) error {
			kafkaAddrs := strings.Split(viper.GetString(flagKafkaAddrs), ",")
			clientID := viper.GetString(flagKafkaClientID)
			groupID := viper.GetString(flagKafkaGroupID)

			logrus.WithFields(logrus.Fields{
				"kafka_addrs":     kafkaAddrs,
				"kafka_client_id": clientID,
				"kafka_group_id":  groupID,
			}).Info("starting crazy")

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
						Protocol: &cg.HashRing{},
						Key:      "hashring",
					},
				},
				SessionTimeout: 30 * time.Second,
				Heartbeat:      3 * time.Second,
				Topics:         []string{"vulcan"},
			})
			c, err := crazy.NewCrazy(&crazy.Config{
				Client:      client,
				Coordinator: coord,
				MaxAge:      time.Hour * 2,
			})
			if err != nil {
				return err
			}
			prometheus.MustRegister(c)
			go func() {
				http.Handle("/metrics", prometheus.Handler())
				http.HandleFunc("/chunks", func(w http.ResponseWriter, r *http.Request) {
					id := r.URL.Query().Get("id")
					ok, chunks := c.ChunksAfter(id, time.Now().Add(-time.Hour*24*365).UnixNano()/int64(time.Millisecond))
					if !ok {
						fmt.Fprintf(w, "id not found")
						return
					}
					w.Header().Add("Content-Type", "text/plain")
					fmt.Fprintf(w, "found n=%d chunks\n", len(chunks))
					buf := make([]byte, chunk.ChunkLen)
					for _, chnk := range chunks {
						chnk.MarshalToBuf(buf)
						fmt.Fprintf(w, "%x\n\n", buf)
					}
					return
				})
				http.ListenAndServe(":8080", nil)
			}()
			logrus.Info("running")
			return c.Run()
		},
	}

	crzy.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	crzy.Flags().String(flagKafkaClientID, "vulcan-crazy", "set the kafka client id")
	crzy.Flags().String(flagKafkaGroupID, "vulcan-crazy", "workers with the same groupID will join the same Kafka ConsumerGroup")
	crzy.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")

	return crzy
}
