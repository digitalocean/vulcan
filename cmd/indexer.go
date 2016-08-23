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
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/digitalocean/vulcan/elasticsearch"
	"github.com/digitalocean/vulcan/indexer"
	"github.com/digitalocean/vulcan/kafka"
	"github.com/digitalocean/vulcan/storage"

	"github.com/olivere/elastic"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Indexer handles parsing the command line options, initializes, and starts the
// indexer service accordingling.  It is the entry point for the Indexer
// service.
func Indexer() *cobra.Command {
	var Indexer = &cobra.Command{
		Use:   "indexer",
		Short: "consumes metrics from the bus and makes them searchable",
		RunE: func(cmd *cobra.Command, args []string) error {
			// bind pflags to viper so they are settable by env variables
			cmd.Flags().VisitAll(func(f *pflag.Flag) {
				viper.BindPFlag(f.Name, f)
			})

			// get kafka source
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

			// set up elastic search templates for the type of text query we need to run
			err = elasticsearch.SetupMatchTemplate(viper.GetString("es"))
			if err != nil {
				return err
			}

			// allow sniff to be set because in some networking environments sniffing doesn't work. Should be allowed in prod
			client, err := elastic.NewClient(elastic.SetURL(viper.GetString("es")), elastic.SetSniff(viper.GetBool("es-sniff")))
			if err != nil {
				return err
			}

			// set up caching es sample indexer
			esIndexer := elasticsearch.NewSampleIndexer(&elasticsearch.SampleIndexerConfig{
				Client: client,
				Index:  viper.GetString("es-index"),
			})
			sampleIndexer := storage.NewCachingIndexer(&storage.CachingIndexerConfig{
				Indexer:     esIndexer,
				MaxDuration: viper.GetDuration("es-writecache-duration"),
			})

			// create indexer and run
			i := indexer.NewIndexer(&indexer.Config{
				SampleIndexer: sampleIndexer,
				Source:        source,
			})
			prometheus.MustRegister(i)
			go func() {
				http.Handle("/metrics", prometheus.Handler())
				http.ListenAndServe(":8080", nil)
			}()
			log.Println("running...")
			return i.Run()
		},
	}

	Indexer.Flags().String("kafka-addrs", "", "one.example.com:9092,two.example.com:9092")
	Indexer.Flags().String("kafka-client-id", "vulcan-indexer", "set the kafka client id")
	Indexer.Flags().String("kafka-topic", "vulcan", "topic to read in kafka")
	Indexer.Flags().String("es", "http://elasticsearch:9200", "elasticsearch connection url")
	Indexer.Flags().Bool("es-sniff", true, "whether or not to sniff additional hosts in the cluster")
	Indexer.Flags().String("es-index", "vulcan", "the elasticsearch index to write documents into")
	Indexer.Flags().Duration("es-writecache-duration", time.Minute*10, "the duration to cache having written a value to es and to skip further writes of the same metric")

	return Indexer
}
