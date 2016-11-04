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
	"strings"
	"time"

	"github.com/Shopify/sarama"
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
			cfg := sarama.NewConfig()
			cfg.Version = sarama.V0_10_0_0
			cfg.ClientID = viper.GetString(flagKafkaClientID)
			addrs := strings.Split(viper.GetString(flagKafkaAddrs), ",")
			client, err := sarama.NewClient(addrs, cfg)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			coord := cg.NewCoordinator(&cg.CoordinatorConfig{
				Client:  client,
				Context: ctx,
				GroupID: viper.GetString(flagKafkaGroupID),
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
			return nil
		},
	}

	crzy.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	crzy.Flags().String(flagKafkaClientID, "vulcan-crazy", "set the kafka client id")
	crzy.Flags().String(flagKafkaGroupID, "vulcan-crazy", "workers with the same groupID will join the same Kafka ConsumerGroup")
	crzy.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")

	return crzy
}
