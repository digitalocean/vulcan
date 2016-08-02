package cmd

import (
	"net/http"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/satori/go.uuid"

	"github.com/digitalocean/vulcan/kafka"
	"github.com/digitalocean/vulcan/scraper"
	"github.com/digitalocean/vulcan/zookeeper"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var Scraper = &cobra.Command{
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
			Pool: viper.GetString("pool"),
		})
		if err != nil {
			return err
		}

		// get a channel of online scrapers (including yourself)
		p, err := zookeeper.NewPool(&zookeeper.PoolConfig{
			ID:   myID.String(),
			Conn: zkconn,
			Root: viper.GetString("zk-root"),
			Pool: viper.GetString("pool"),
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
		s := scraper.NewScraper(&scraper.ScraperConfig{
			Targeter: ft,
			Writer:   w,
		})
		// prometheus.MustRegister(s)
		go func() {
			http.Handle("/metrics", prometheus.Handler())
			http.ListenAndServe(":8080", nil)
		}()
		log.Info("running...")
		err = s.Run()
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	Scraper.Flags().Duration("zk-timeout", time.Second*10, "zookeeper timeout")
	Scraper.Flags().String("zk-servers", "", "comma-separated list of zookeeper servers")
	// TODO parse zk-root from zookeeper server uri e.g. server1,server2:2181/root/path
	Scraper.Flags().String("zk-root", "/vulcan", "zookeeper path under which jobs are stored")
	Scraper.Flags().String("kafka-topic", "vulcan", "kafka topic to write to")
	Scraper.Flags().String("kafka-addrs", "", "one.example.com:9092,two.example.com:9092")
	Scraper.Flags().String("kafka-client-id", "vulcan-scraper", "set the kafka client id")
	Scraper.Flags().String("pool", "default", "name of the scraper pool to join")
}
