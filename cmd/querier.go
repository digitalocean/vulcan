package cmd

import (
	"strings"
	"time"

	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/elasticsearch"
	"github.com/digitalocean/vulcan/querier"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Querier handles parsing the command line options, initializes and starts the
// querier service accordingling.  It is the entry point for the Querier
// service.
var Querier = &cobra.Command{
	Use:   "querier",
	Short: "runs the query service that implements PromQL and prometheus v1 api",
	RunE: func(cmd *cobra.Command, args []string) error {
		// bind pflags to viper so they are settable by env variables
		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			viper.BindPFlag(f.Name, f)
		})
		// create elasticsearch metric resolver
		mr, err := elasticsearch.NewMetricResolver(&elasticsearch.MetricResolverConfig{
			URL:   viper.GetString("es"),
			Sniff: viper.GetBool("es-sniff"),
			Index: viper.GetString("es-index"),
		})
		if err != nil {
			return err
		}
		// create cassandra datapoint reader
		dpr, err := cassandra.NewDatapointReader(&cassandra.DatapointReaderConfig{
			CassandraAddrs: strings.Split(viper.GetString("cassandra-addrs"), ","),
			Keyspace:       viper.GetString("cassandra-keyspace"),
			Timeout:        10 * time.Second,
		})
		if err != nil {
			return err
		}
		q := querier.NewQuerier(&querier.Config{
			DatapointReader: dpr,
			MetricResolver:  mr,
		})
		return q.Run()
	},
}

func init() {
	Querier.Flags().String("cassandra-addrs", "", "cassandra01.example.com")
	Querier.Flags().String("cassandra-keyspace", "vulcan", "cassandra keyspace to query")
	Querier.Flags().String("es", "http://elasticsearch:9200", "elasticsearch connection url")
	Querier.Flags().Bool("es-sniff", true, "whether or not to sniff additional hosts in the cluster")
	Querier.Flags().String("es-index", "vulcan", "the elasticsearch index to write documents into")
}
