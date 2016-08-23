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
	r   storage.Resolver

	queryDurations *prometheus.SummaryVec
}

// Config represents the configuration of a Querier object.
type Config struct {
	DatapointReader storage.DatapointReader
	Resolver        storage.Resolver
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
		r:   config.Resolver,
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
		MetricResolver:  q.r,
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
