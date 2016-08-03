package querier

import (
	"log"
	"net/url"
	"time"

	"github.com/digitalocean/vulcan/storage"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/web"
)

// Querier serves as the layer that layer takes incoming user queries
// and returns the results fetched from the storage system.
type Querier struct {
	prometheus.Collector

	dpr storage.DatapointReader
	mr  storage.MetricResolver

	queryDurations *prometheus.SummaryVec
}

// Config represents the configuration of a Querier object.
type Config struct {
	DatapointReader storage.DatapointReader
	MetricResolver  storage.MetricResolver
}

// NewQuerier returns creates a new instance of Querier.
func NewQuerier(config *Config) *Querier {
	return &Querier{
		queryDurations: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "vulcan",
				Subsystem: "querier",
				Name:      "duration_nanoseconds",
				Help:      "Durations of query stages",
			},
			[]string{"stage"},
		),
		dpr: config.DatapointReader,
		mr:  config.MetricResolver,
	}
}

// Describe implements prometheus.Collector.  Sends decriptors of the
// instance's queryDurations to the parameter ch.
func (q *Querier) Describe(ch chan<- *prometheus.Desc) {
	q.queryDurations.Describe(ch)
}

// Collect implements Collector.  Sends metrics collected by queryDurations
// to the parameter ch.
func (q *Querier) Collect(ch chan<- prometheus.Metric) {
	q.queryDurations.Collect(ch)
}

// Run starts the Querir HTTP service.
func (q *Querier) Run() error {
	ps, err := NewPrometheusWrapper(&PrometheusWrapperConfig{
		DatapointReader: q.dpr,
		MetricResolver:  q.mr,
	})
	if err != nil {
		return err
	}
	queryEngine := promql.NewEngine(ps, &promql.EngineOptions{
		MaxConcurrentQueries: 20,
		Timeout:              time.Minute * 10,
	})
	bsURL, _ := url.Parse("http://example.com/")
	ruleManager := rules.NewManager(&rules.ManagerOptions{
		SampleAppender: nil,
		Notifier:       nil,
		QueryEngine:    queryEngine,
		ExternalURL:    bsURL,
	})
	// status := &web.PrometheusStatus{
	// 	TargetPools: func() map[string]retrieval.Targets {
	// 		return map[string]retrieval.Targets{}
	// 	},
	// 	Rules: func() []rules.Rule {
	// 		return []rules.Rule{}
	// 	},
	// 	Flags: map[string]string{},
	// 	Birth: time.Now(),
	// }
	prometheus.MustRegister(ps)
	externalURL, _ := url.Parse("http://localhost:9090")
	tm := &retrieval.TargetManager{}
	webHandler := web.New(ps, queryEngine, tm, ruleManager, &web.PrometheusVersion{}, map[string]string{}, &web.Options{
		ListenAddress: ":9090",
		MetricsPath:   "/metrics",
		ExternalURL:   externalURL,
		RoutePrefix:   "/",
	})
	log.Println("running")
	webHandler.Run()
	return nil
}
