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

	"github.com/digitalocean/vulcan/kafka"
	"github.com/digitalocean/vulcan/scraper"
	"github.com/digitalocean/vulcan/zookeeper"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/satori/go.uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Scraper handles parsing the command line options, initializes, and starts the
// scraper service accordingling.  It is the entry point for the scraper
// service.
func Scraper() *cobra.Command {
	scraper := &cobra.Command{
		Use:   "scraper",
		Short: "runs a scraper agent to collect metrics into kafka",
		RunE: func(cmd *cobra.Command, args []string) error {
			log.SetLevel(log.DebugLevel)
			myID := uuid.NewV4()
			// bind pflags to viper so they are settable by env variables
			cmd.Flags().VisitAll(func(f *pflag.Flag) {
				viper.BindPFlag(f.Name, f)
			})

			// set up zk connection
			zkconn, _, err := zk.Connect(strings.Split(viper.GetString("zk-servers"), ","), viper.GetDuration("zk-timeout"))
			if err != nil {
				return err
			}

			// get targets from watching a zookeeper directory
			zkt, err := zookeeper.NewTargeter(&zookeeper.TargeterConfig{
				Conn: zkconn,
				Root: viper.GetString("zk-root"),
			})
			if err != nil {
				return err
			}

			// get a channel of online scrapers (including yourself)
			p, err := zookeeper.NewPool(&zookeeper.PoolConfig{
				ID:   myID.String(),
				Conn: zkconn,
				Root: viper.GetString("zk-root"),
			})
			if err != nil {
				return err
			}

			// filter targeter based on a consistent hash with available nodes in the pool
			ft := scraper.NewConsistentHashTargeter(&scraper.ConsistentHashTargeterConfig{
				Targeter: zkt,
				Pool:     p,
				ID:       myID.String(),
			})

			// create upstream kafka writer to receive data
			w, err := kafka.NewWriter(&kafka.WriterConfig{
				ClientID: viper.GetString("kafka-client-id"),
				Topic:    viper.GetString("kafka-topic"),
				Addrs:    strings.Split(viper.GetString("kafka-addrs"), ","),
			})
			if err != nil {
				return err
			}

			// create the scraper which orchestrates scraping targets and writing their
			// results to the upstream
			s := scraper.NewScraper(&scraper.Config{
				Targeter: ft,
				Writer:   w,
			})
			// prometheus.MustRegister(s)
			go func() {
				http.Handle("/metrics", prometheus.Handler())
				http.ListenAndServe(":8080", nil)
			}()
			log.Info("running...")
			s.Run()

			return nil
		},
	}

	scraper.Flags().Duration("zk-timeout", time.Second*10, "zookeeper timeout")
	scraper.Flags().String("zk-servers", "", "comma-separated list of zookeeper servers")
	scraper.Flags().String("zk-root", "/vulcan", "zookeeper path under which jobs are stored")
	scraper.Flags().String("kafka-topic", "vulcan", "kafka topic to write to")
	scraper.Flags().String("kafka-addrs", "", "one.example.com:9092,two.example.com:9092")
	scraper.Flags().String("kafka-client-id", "vulcan-scraper", "set the kafka client id")

	return scraper
}
