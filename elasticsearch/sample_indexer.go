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

package elasticsearch

import (
	"sync"
	"time"

	"github.com/digitalocean/vulcan/convert"
	"github.com/digitalocean/vulcan/indexer"
	"github.com/digitalocean/vulcan/model"

	"github.com/olivere/elastic"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "vulcan"
	subsystem = "elasticsearch_sample_indexer"
)

// SampleIndexer represents an object that takes bus messages and
// makes indexing decisions on the target ElasticSearch cluster.
type SampleIndexer struct {
	indexer.SampleIndexer
	prometheus.Collector

	Client *elastic.Client
	Index  string

	workCh chan *model.TimeSeries
	errCh  chan error
	done   chan struct{}
	once   *sync.Once

	indexDurations *prometheus.SummaryVec
}

// SampleIndexerConfig represents the configuration of a SampleIndexer.
type SampleIndexerConfig struct {
	Client *elastic.Client
	Index  string
}

// NewSampleIndexer creates a new instance of SampleIndexer.
func NewSampleIndexer(config *SampleIndexerConfig) *SampleIndexer {
	return &SampleIndexer{
		Client: config.Client,
		Index:  config.Index,

		workCh: make(chan *model.TimeSeries, 1),
		errCh:  make(chan error),
		done:   make(chan struct{}),
		once:   new(sync.Once),

		indexDurations: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "index_duration_seconds",
				Help:      "Durations of different elasticsearch_sample_indexer stages",
			},
			[]string{"mode"},
		),
	}
}

// Describe implements prometheus.Collector.  Sends decriptors of the
// instance's indexDurations to the parameter ch.
func (si *SampleIndexer) Describe(ch chan<- *prometheus.Desc) {
	si.indexDurations.Describe(ch)
}

// Collect implements prometheus.Collector.  Sends metrics collected bu the
// instance's indexDurations to the parameter ch.
func (si *SampleIndexer) Collect(ch chan<- prometheus.Metric) {
	si.indexDurations.Collect(ch)
}

func metricToESBody(ts *model.TimeSeries) (map[string]string, error) {
	labels := map[string]string{}

	for k, v := range ts.Labels {
		labels[convert.ESEscape(k)] = v
	}

	return labels, nil
}

// IndexSample checks if a metric label set exists in the database, if not
// it writes the label set.
func (si *SampleIndexer) IndexSample(ts *model.TimeSeries) error {
	t0 := time.Now()

	exists, err := si.Client.Exists().
		Index(si.Index).
		Type("sample").
		Id(ts.ID()).Do()
	if err != nil {
		return err
	}
	if exists {
		si.indexDurations.WithLabelValues("exists").Observe(time.Since(t0).Seconds())
		return nil
	}

	body, err := metricToESBody(ts)
	if err != nil {
		return err
	}

	_, err = si.Client.Index().
		Index(si.Index).
		Type("sample").
		Id(ts.ID()).
		BodyJson(body).
		Do()
	if err != nil {
		return err
	}

	si.indexDurations.WithLabelValues("insert").Observe(time.Since(t0).Seconds())
	return nil
}
