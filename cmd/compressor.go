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
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/kafka"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func handle(source <-chan *bus.SourcePayload) error {
	logrus.Info("starting to handle source")
	rw := &sync.RWMutex{}
	partials := map[string]chunk.Chunk{}
	var counter int64
	for m := range source {
		for _, ts := range m.TimeSeriesBatch {
			id := ts.ID()
			rw.RLock()
			c, ok := partials[id]
			if !ok {
				rw.RUnlock()
				innerc, err := chunk.NewForEncoding(chunk.Varbit)
				if err != nil {
					return err
				}
				rw.Lock()
				c = innerc
				partials[id] = c
				rw.Unlock()
				rw.RLock()
			}
			for _, s := range ts.Samples {
				sp := model.SamplePair{
					Timestamp: model.Time(s.TimestampMS),
					Value:     model.SampleValue(s.Value),
				}
				chunks, err := c.Add(sp)
				if err != nil {
					return err
				}
				if len(chunks) == 2 {
					logrus.Info("this is where we write a chunk")
					c = chunks[1]
				} else {
					c = chunks[0]
				}
			}
			rw.RUnlock()
			rw.Lock()
			partials[id] = c
			rw.Unlock()
		}
		m.Ack()

		if counter%100000 == 0 {
			rw.RLock()
			var first chunk.Chunk
			var firstID string
			for id, c := range partials {
				firstID = id
				first = c
				break
			}
			if first == nil {
				continue
			}
			fc := first.Clone()
			rw.RUnlock()
			i := fc.NewIterator()
			fmt.Printf("id: %s(\n", firstID)
			for i.Scan() {
				sp := i.Value()
				fmt.Printf("[%s=%f],", time.Unix(int64(sp.Timestamp)/1000, 0), sp.Value)
			}
			fmt.Printf("\n")
		}

		if counter%100000 == 0 {
			maxUtil := 0.0
			rw.RLock()
			for _, c := range partials {
				if c.Utilization() > maxUtil {
					maxUtil = c.Utilization()
				}
			}
			rw.RUnlock()
			logrus.WithFields(logrus.Fields{
				"num_partials": len(partials),
				"max_util":     maxUtil,
			}).Info("partials stats")

		}
		counter = counter + 1
	}
	logrus.Info("done handling source")
	return nil
}

// Compressor handles parsing the command line options, initializes, and starts the
// compressor service accordingling.
func Compressor() *cobra.Command {
	comp := &cobra.Command{
		Use:   "compressor",
		Short: "runs the compressor service to consume metrics from kafka into cassandra",
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
			shard, err := kafka.NewWindowedShardSource(&kafka.WindowedShardSourceConfig{
				Client:         client,
				Ctx:            ctx,
				GroupID:        viper.GetString(flagKafkaGroupID),
				Heartbeat:      time.Second * 3,
				SessionTimeout: time.Second * 30,
				Topic:          viper.GetString(flagKafkaTopic),
				Window:         time.Hour * 2,
			})
			if err != nil {
				return err
			}
			for source := range shard.Messages() {
				go func(source <-chan *bus.SourcePayload) {
					err := handle(source)
					if err != nil {
						logrus.WithError(err).Error("while handling source")
					}
				}(source)
			}
			err = shard.Err()
			if err != nil {
				return err
			}
			return nil
		},
	}

	comp.Flags().String(flagKafkaAddrs, "", "one.example.com:9092,two.example.com:9092")
	comp.Flags().String(flagKafkaClientID, "vulcan-compressor", "set the kafka client id")
	comp.Flags().String(flagKafkaGroupID, "vulcan-compressor", "workers with the same groupID will join the same Kafka ConsumerGroup")
	comp.Flags().String(flagKafkaTopic, "vulcan", "set the kafka topic to consume")

	return comp
}
