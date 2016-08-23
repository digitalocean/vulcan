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
	"strings"
	"time"

	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/ingester"
	"github.com/digitalocean/vulcan/kafka"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Ingester handles parsing the command line options, initializes, and starts the
// ingester service accordingling.  It is the entry point for the Ingester
// service.
var Ingester = &cobra.Command{
	Use:   "ingester",
	Short: "runs the ingester service to consume metrics from kafka into cassandra",
	RunE: func(cmd *cobra.Command, args []string) error {
		log.SetLevel(log.DebugLevel)
		// bind pflags to viper so they are settable by env variables
		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			viper.BindPFlag(f.Name, f)
		})

		// ensure cassandra tables
		err := cassandra.SetupTables(strings.Split(viper.GetString("cassandra-addrs"), ","), viper.GetString("cassandra-keyspace"))
		if err != nil {
			return err
		}

		// create cassandra sample writer
		sw, err := cassandra.NewSampleWriter(&cassandra.SampleWriterConfig{
			CassandraAddrs: strings.Split(viper.GetString("cassandra-addrs"), ","),
			Keyspace:       viper.GetString("cassandra-keyspace"),
			Timeout:        30 * time.Second,
		})
		if err != nil {
			return err
		}

		// create kafka source
		source, err := kafka.NewAckSource(&kafka.AckSourceConfig{
			Addrs:     strings.Split(viper.GetString("kafka-addrs"), ","),
			ClientID:  viper.GetString("kafka-client-id"),
			Converter: kafka.DefaultConverter{},
			Topic:     viper.GetString("kafka-topic"),
		})
		if err != nil {
			return err
		}
		prometheus.MustRegister(source)

		// create and start ingester
		i := ingester.NewIngester(&ingester.Config{
			SampleWriter: sw,
			AckSource:    source,
		})
		prometheus.MustRegister(i)
		go func() {
			http.Handle("/metrics", prometheus.Handler())
			http.ListenAndServe(":8080", nil)
		}()
		return i.Run()
	},
}

func init() {
	Ingester.Flags().String("cassandra-addrs", "", "one.example.com:9092,two.example.com:9092")
	Ingester.Flags().String("cassandra-keyspace", "vulcan", "cassandra keyspace to use")
	Ingester.Flags().String("kafka-addrs", "", "one.example.com:9092,two.example.com:9092")
	Ingester.Flags().String("kafka-client-id", "vulcan-ingest", "set the kafka client id")
	Ingester.Flags().String("kafka-topic", "vulcan", "set the kafka topic to consume")
}
