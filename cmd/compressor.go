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

	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/kafka"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Compressor handles parsing the command line options, initializes, and starts the
// compressor service accordingling.
func Compressor() *cobra.Command {
	comp := &cobra.Command{
		Use:   "compressor",
		Short: "runs the compressor service to consume metrics from kafka into cassandra",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, err := kafka.NewWindowedSource(&kafka.WindowedSourceConfig{
				Addrs:    strings.Split(viper.GetString(flagKafkaAddrs), ","),
				Ctx:      ctx,
				ClientID: viper.GetString(flagKafkaClientID),
				GroupID:  viper.GetString(flagKafkaGroupID),
				Target:   time.Now().Add(-time.Hour * 2),
				Topics:   []string{viper.GetString(flagKafkaTopic)},
			})
			if err != nil {
				return err
			}
			partials := map[string]chunk.Chunk{}
			go func() {
				for {
					time.Sleep(time.Second)
					maxUtil := 0.0
					for _, c := range partials {
						if c.Utilization() > maxUtil {
							maxUtil = c.Utilization()
						}
					}
					logrus.WithFields(logrus.Fields{
						"num_partials": len(partials),
						"max_util":     maxUtil,
					}).Info("partials stats")
				}
			}()
			for m := range s.Messages() {
				for _, ts := range m.TimeSeriesBatch {
					id := ts.ID()
					c, ok := partials[id]
					if !ok {
						c, err = chunk.NewForEncoding(chunk.Varbit)
						if err != nil {
							return err
						}
						partials[id] = c
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
					partials[id] = c
				}
				m.Ack()
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
