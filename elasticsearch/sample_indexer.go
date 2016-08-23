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
	"time"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"

	"github.com/olivere/elastic"
	"github.com/prometheus/client_golang/prometheus"
)

// SampleIndexer represents an object that takes bus messages and
// makes indexing decisions on the target ElasticSearch cluster.
type SampleIndexer struct {
	prometheus.Collector
	Client *elastic.Client
	Index  string

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
		indexDurations: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "vulcan",
				Subsystem: "elasticsearch_sample_indexer",
				Name:      "duration_nanoseconds",
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

func metricToESBody(m bus.Metric) (map[string]string, error) {
	labels := map[string]string{
		convert.ESEscape("__name__"): m.Name,
	}
	for k, v := range m.Labels {
		labels[convert.ESEscape(k)] = v
	}
	return labels, nil
}

// IndexSample implements the SampleIndexer interface.
func (si *SampleIndexer) IndexSample(s *bus.Sample) error {
	t0 := time.Now()
	key, err := convert.MetricToKey(s.Metric)
	if err != nil {
		return err
	}
	exists, err := si.Client.Exists().
		Index(si.Index).
		Type("sample").
		Id(key).Do()
	if err != nil {
		return err
	}
	if exists {
		si.indexDurations.WithLabelValues("exists").Observe(float64(time.Since(t0).Nanoseconds()))
		return nil
	}
	body, err := metricToESBody(s.Metric)
	if err != nil {
		return err
	}
	_, err = si.Client.Index().
		Index(si.Index).
		Type("sample").
		Id(key).
		BodyJson(body).
		Do()
	si.indexDurations.WithLabelValues("insert").Observe(float64(time.Since(t0).Nanoseconds()))
	return err
}
