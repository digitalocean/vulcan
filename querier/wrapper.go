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
	"errors"
	"fmt"
	"time"

	"github.com/digitalocean/vulcan/convert"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
)

const (
	metricNameKey model.LabelName = "__name__"
)

var (
	errNotImplemented = errors.New("not implemented")
)

// IteratorFactory allows the Wrapper to create iterators without needing to know
// about the implementing backend.
type IteratorFactory interface {
	// Iterator returns a SeriesIterator
	Iterator(m metric.Metric, from, through model.Time) (local.SeriesIterator, error)
}

// Wrapper implements the prometheus storage interface and provides us
// a way to query metrics from cassandra and feed those metrics into the
// prometheus query evaluator for reads.
type Wrapper struct {
	prometheus.Collector

	f IteratorFactory
	r Resolver

	queryDurations *prometheus.SummaryVec
	matchesFound   prometheus.Summary
}

// WrapperConfig represents the configuration of a
// Wrapper object.
type WrapperConfig struct {
	IteratorFactory IteratorFactory
	Resolver        Resolver
}

// NewWrapper creates a new instance of PrometheusWrapper
func NewWrapper(config *WrapperConfig) (*Wrapper, error) {
	w := &Wrapper{
		r: config.Resolver,
		f: config.IteratorFactory,
		matchesFound: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Namespace:  "vulcan",
				Subsystem:  "storage",
				Name:       "number_matches_found",
				Help:       "the number of metrics matched by a query",
				Objectives: map[float64]float64{0.01: 0.001, 0.1: 0.01, 0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			},
		),

		queryDurations: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "vulcan",
				Subsystem: "querier",
				Name:      "duration_nanoseconds",
				Help:      "Durations of different querier stages",
			},
			[]string{"stage"},
		),
	}
	return w, nil
}

// Append appends a sample to the underlying  Depending on the
// storage implementation, there are different guarantees for the fate
// of the sample after Append has returned. Remote storage
// implementation will simply drop samples if they cannot keep up with
// sending samples. Local storage implementations will only drop metrics
// upon unrecoverable errors.
func (w *Wrapper) Append(*model.Sample) error {
	return nil
}

// NeedsThrottling returns true if the underlying storage wishes to not
// receive any more samples. Append will still work but might lead to
// undue resource usage. It is recommended to call NeedsThrottling once
// before an upcoming batch of Append calls (e.g. a full scrape of a
// target or the evaluation of a rule group) and only proceed with the
// batch if NeedsThrottling returns false. In that way, the result of a
// scrape or of an evaluation of a rule group will always be appended
// completely or not at all, and the work of scraping or evaluation will
// not be performed in vain. Also, a call of NeedsThrottling is
// potentially expensive, so limiting the number of calls is reasonable.
//
// Only SampleAppenders for which it is considered critical to receive
// each and every sample should ever return true. SampleAppenders that
// tolerate not receiving all samples should always return false and
// instead drop samples as they see fit to avoid overload.
func (w *Wrapper) NeedsThrottling() bool {
	return false
}

// QueryRange returns a list of series iterators for the selected
// time range and label matchers. The iterators need to be closed
// after usage.
func (w *Wrapper) QueryRange(from, through model.Time, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error) {
	mets, err := w.MetricsForLabelMatchers(from, through, matchers)
	if err != nil {
		return []local.SeriesIterator{}, nil
	}
	itrs := make([]local.SeriesIterator, 0, len(mets))
	for _, met := range mets {
		itr, err := w.f.Iterator(met, from, through)
		if err != nil {
			return []local.SeriesIterator{}, err
		}
		itrs = append(itrs, itr)
	}

	return itrs, nil
}

// QueryInstant returns a list of series iterators for the selected
// instant and label matchers. The iterators need to be closed after usage.
func (w *Wrapper) QueryInstant(ts model.Time, stalenessDelta time.Duration, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error) {
	return w.QueryRange(ts.Add(-stalenessDelta), ts.Add(stalenessDelta), matchers...)
}

// MetricsForLabelMatchers returns the metrics from storage that satisfy
// the given sets of label matchers. Each set of matchers must contain at
// least one label matcher that does not match the empty string. Otherwise,
// an empty list is returned. Within one set of matchers, the intersection
// of matching series is computed. The final return value will be the union
// of the per-set results. The times from and through are hints for the
// storage to optimize the search. The storage MAY exclude metrics that
// have no samples in the specified interval from the returned map. In
// doubt, specify model.Earliest for from and model.Latest for through.
func (w *Wrapper) MetricsForLabelMatchers(from, through model.Time, matcherSets ...metric.LabelMatchers) ([]metric.Metric, error) {
	result := []metric.Metric{}
	defer func() {
		w.matchesFound.Observe(float64(len(result)))
	}()
	// convert prometheus matchers to vulcan matchers
	m, err := toMatches(matcherSets...)
	if err != nil {
		return result, err
	}
	// get matching time series
	tsb, err := w.r.Resolve(m)
	if err != nil {
		return result, err
	}
	return convert.TimeSeriesBatchToMetrics(tsb), nil
}

// LastSampleForLabelMatchers returns the last sample that has been
// ingested for the given sets of label matchers. If this instance of the
// Storage has never ingested a sample for the provided fingerprint (or
// the last ingestion is so long ago that the series has been archived),
// ZeroSample is returned. The label matching behavior is the same as in
// MetricsForLabelMatchers.
func (w *Wrapper) LastSampleForLabelMatchers(cutoff model.Time, matcherSets ...metric.LabelMatchers) (model.Vector, error) {
	return model.Vector{}, errNotImplemented
}

// LabelValuesForLabelName gets all of the label values that are associated with a given label name.
func (w *Wrapper) LabelValuesForLabelName(name model.LabelName) (model.LabelValues, error) {
	vals, err := w.r.Values(string(name))
	if err != nil {
		return model.LabelValues{}, err
	}

	res := make(model.LabelValues, 0, len(vals))
	for _, val := range vals {
		res = append(res, model.LabelValue(val))
	}

	return res, nil
}

// DropMetricsForLabelMatchers drops all time series associated with the given label matchers. Returns
// the number series that were dropped.
func (w *Wrapper) DropMetricsForLabelMatchers(...*metric.LabelMatcher) (int, error) {
	return 0, errNotImplemented
}

// Start runs the various maintenance loops in goroutines. Returns when the
// storage is ready to use. Keeps everything running in the background
// until Stop is called.
func (w *Wrapper) Start() error {
	return nil
}

// Stop shuts down the Storage gracefully, flushes all pending
// operations, stops all maintenance loops,and frees all resources.
func (w *Wrapper) Stop() error {
	return nil
}

// WaitForIndexing returns once all samples in the storage are
// indexed. Indexing is needed for FingerprintsForLabelMatchers and
// LabelValuesForLabelName and may lag behind.
func (w *Wrapper) WaitForIndexing() {
	return
}

// Describe implements prometheus.Collector. Sends decriptors of the
// instance's matchesFound and queryDurations to the parameter ch.
func (w *Wrapper) Describe(ch chan<- *prometheus.Desc) {
	w.matchesFound.Describe(ch)
	w.queryDurations.Describe(ch)
}

// Collect implements prometheus.Collector.Sends metrics collected by the
// instance's matchesFound and queryDurations to the parameter ch.
func (w *Wrapper) Collect(ch chan<- prometheus.Metric) {
	w.matchesFound.Collect(ch)
	w.queryDurations.Collect(ch)
}

func toMatches(matchers ...metric.LabelMatchers) ([]*Match, error) {
	matches := []*Match{}
	for _, mt := range matchers {
		for _, m := range mt {
			next := &Match{
				Name:  string(m.Name),
				Value: string(m.Value),
			}
			switch m.Type {
			case metric.Equal:
				next.Type = Equal
			case metric.NotEqual:
				next.Type = NotEqual
			case metric.RegexMatch:
				next.Type = RegexMatch
			case metric.RegexNoMatch:
				next.Type = RegexNoMatch
			default:
				return []*Match{}, fmt.Errorf("unhandled match type")
			}
			matches = append(matches, next)
		}
	}
	return matches, nil
}
