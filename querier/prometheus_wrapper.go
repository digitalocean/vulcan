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
	"fmt"
	"sync"
	"time"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"
	"github.com/digitalocean/vulcan/storage"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
)

// PrometheusWrapper implements the prometheus storage interface and provides us
// a way to query metrics from cassandra and feed those metrics into the
// prometheus query evaluator for reads.
//
// This is a hack to use the prometheus query evaluator. We should implement
// PromQL in a better way that doesn't make assumptions about metrics being
// available in-memory.
type PrometheusWrapper struct {
	prometheus.Collector

	Preloader      *preloader
	DPReader       storage.DatapointReader
	metricResolver storage.Resolver

	queryDurations *prometheus.SummaryVec
	matchesFound   prometheus.Summary
}

// Describe implements prometheus.Collector. Sends decriptors of the
// instance's matchesFound and queryDurations to the parameter ch.
func (pw *PrometheusWrapper) Describe(ch chan<- *prometheus.Desc) {
	pw.matchesFound.Describe(ch)
	pw.queryDurations.Describe(ch)
}

// Collect implements prometheus.Collector.Sends metrics collected by the
// instance's matchesFound and queryDurations to the parameter ch.
func (pw *PrometheusWrapper) Collect(ch chan<- prometheus.Metric) {
	pw.matchesFound.Collect(ch)
	pw.queryDurations.Collect(ch)
}

type preloader struct {
	DPReader storage.DatapointReader

	fingerPrintLock sync.RWMutex
	// since the storage interface requires that fetch metrics via fingerprint, we
	// have to cache mappings from fingerprints to metrics as they are requested
	HackFingerprint map[model.Fingerprint]model.Metric

	queryDurations *prometheus.SummaryVec
}

// SeriesIterator enables efficient access of sample values in a series. Its
// methods are not goroutine-safe. A SeriesIterator iterates over a snapshot of
// a series, i.e. it is safe to continue using a SeriesIterator after or during
// modifying the corresponding series, but the iterator will represent the state
// of the series prior to the modification.
type SeriesIterator struct {
	After, Before model.Time
	Metric        bus.Metric
	DPReader      storage.DatapointReader

	l              sync.Mutex
	ready          bool
	points         []bus.Datapoint
	queryDurations *prometheus.SummaryVec
}

// PrometheusWrapperConfig represents the configuration of a
// PrometheusWrapperConfig object.
type PrometheusWrapperConfig struct {
	DatapointReader storage.DatapointReader
	MetricResolver  storage.Resolver
}

// NewPrometheusWrapper creates a new instance of PrometheusWrapper
func NewPrometheusWrapper(config *PrometheusWrapperConfig) (pw *PrometheusWrapper, err error) {
	pw = &PrometheusWrapper{
		metricResolver: config.MetricResolver,
		DPReader:       config.DatapointReader,
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
	pw.Preloader = &preloader{
		DPReader:        config.DatapointReader,
		fingerPrintLock: sync.RWMutex{},
		HackFingerprint: map[model.Fingerprint]model.Metric{},

		queryDurations: pw.queryDurations,
	}
	return
}

func (si *SeriesIterator) fetch() {
	// early unsynchronized exit
	if si.ready {
		return
	}
	si.l.Lock()
	defer si.l.Unlock()
	// recheck condition
	if si.ready {
		return
	}
	t0 := time.Now()
	key, err := convert.MetricToKey(si.Metric)
	if err != nil {
		log.Println(err)
	}
	points, err := si.DPReader.ReadDatapoints(key, bus.Timestamp(si.After), bus.Timestamp(si.Before))
	if err != nil {
		log.Println(err)
	}
	si.points = points
	si.queryDurations.WithLabelValues("fetch").Observe(float64(time.Since(t0).Nanoseconds()))
	si.ready = true
}

// ValueAtOrBeforeTime gets the value that is closest before the given time.
// In case a value exists at precisely the given time, that value is returned.
// If no applicable value exists, ZeroSamplePair is returned.
func (si *SeriesIterator) ValueAtOrBeforeTime(t model.Time) model.SamplePair {
	// log.Infof("ValueAtOrBeforeTime: %v", t)
	si.fetch()
	for _, point := range si.points {
		if int64(point.Timestamp) <= int64(t) {
			return model.SamplePair{
				Timestamp: model.Time(point.Timestamp),
				Value:     model.SampleValue(point.Value),
			}
		}
	}
	return local.ZeroSamplePair
}

// RangeValues gets all values contained within a given interval.
func (si *SeriesIterator) RangeValues(r metric.Interval) []model.SamplePair {
	si.fetch()
	result := []model.SamplePair{}
	for i := len(si.points) - 1; i > 0; i-- {
		point := si.points[i]
		if int64(r.OldestInclusive) <= int64(point.Timestamp) && int64(point.Timestamp) <= int64(r.NewestInclusive) {
			result = append(result, model.SamplePair{
				Timestamp: model.Time(point.Timestamp),
				Value:     model.SampleValue(point.Value),
			})
		}
	}
	return result
}

// PreloadRange implements a prometheus.Preloader.
func (p *preloader) PreloadRange(fp model.Fingerprint, from, through model.Time) local.SeriesIterator {
	promMetric := p.metricForFingerprint(fp)
	kaiMetric := bus.Metric{Name: string(promMetric["__name__"]), Labels: map[string]string{}}
	for k, v := range promMetric {
		if k == "__name__" {
			continue
		}
		kaiMetric.Labels[string(k)] = string(v)
	}
	si := &SeriesIterator{
		After:    from,
		Before:   through,
		Metric:   kaiMetric,
		DPReader: p.DPReader,

		queryDurations: p.queryDurations,
	}
	// start fetching concurrently
	// this is hacky because prometheus assumes in-memory data and
	// does PreloadRange in-series with all the other metrics it wants
	// to preload. Goroutine-ing it makes us faster.
	go func() {
		si.fetch()
	}()
	return si
}

// PreloadInstant implements a prometheus.Preloader.
func (p *preloader) PreloadInstant(fp model.Fingerprint, timestamp model.Time, stalenessDelta time.Duration) local.SeriesIterator {
	promMetric := p.metricForFingerprint(fp)
	kaiMetric := bus.Metric{Name: string(promMetric["__name__"]), Labels: map[string]string{}}
	for k, v := range promMetric {
		if k == "__name__" {
			continue
		}
		kaiMetric.Labels[string(k)] = string(v)
	}
	return &SeriesIterator{
		After:    timestamp.Add(-stalenessDelta),
		Before:   timestamp,
		Metric:   kaiMetric,
		DPReader: p.DPReader,

		queryDurations: p.queryDurations,
	}
}

func (p *preloader) metricForFingerprint(fp model.Fingerprint) model.Metric {
	p.fingerPrintLock.RLock()
	defer p.fingerPrintLock.RUnlock()
	return p.HackFingerprint[fp]
}

// Close is a no-op but necessary to fulfil the prometheus storage interface
func (p *preloader) Close() {}

// Append is a no-op but necessary to fulfil the prometheus storage interface
func (pw *PrometheusWrapper) Append(sample *model.Sample) error {
	return nil
}

// NeedsThrottling is a no-op but necessary to fulfil the prometheus storage interface
func (pw *PrometheusWrapper) NeedsThrottling() bool {
	return false
}

// NewPreloader implements prometheus.Querier interface.
func (pw *PrometheusWrapper) NewPreloader() local.Preloader {
	return pw.Preloader
}

func toMatches(matchers ...*metric.LabelMatcher) ([]*storage.Match, error) {
	matches := make([]*storage.Match, 0, len(matchers))
	for _, m := range matchers {
		next := &storage.Match{
			Name:  string(m.Name),
			Value: string(m.Value),
		}
		switch m.Type {
		case metric.Equal:
			next.Type = storage.Equal
		case metric.NotEqual:
			next.Type = storage.NotEqual
		case metric.RegexMatch:
			next.Type = storage.RegexMatch
		case metric.RegexNoMatch:
			next.Type = storage.RegexNoMatch
		default:
			return []*storage.Match{}, fmt.Errorf("unhandled match type")
		}
		matches = append(matches, next)
	}
	return matches, nil
}

func toModelMetric(metrics []*bus.Metric) []model.Metric {
	// convert to model.Metric
	modms := make([]model.Metric, 0, len(metrics))
	for _, m := range metrics {
		modm := model.Metric{}
		for key, value := range m.Labels {
			modm[model.LabelName(key)] = model.LabelValue(value)
		}
		modm[model.LabelName("__name__")] = model.LabelValue(m.Name)
		modms = append(modms, modm)
	}
	return modms
}

func toFingerprintedMetric(metrics []model.Metric) map[model.Fingerprint]metric.Metric {
	fpm := map[model.Fingerprint]metric.Metric{}
	for _, m := range metrics {
		fp := m.Fingerprint()
		fpm[fp] = metric.Metric{Metric: m}
	}
	return fpm
}

// MetricsForLabelMatchers implements the prometheus storage interface. Given prometheus label
// matches, it returns the full metric names that match (using the metric fingerprint to fulfil
// the interface... which isn't ideal for our model but works).
func (pw *PrometheusWrapper) MetricsForLabelMatchers(from, through model.Time, matchers ...*metric.LabelMatcher) map[model.Fingerprint]metric.Metric {
	result := map[model.Fingerprint]metric.Metric{}
	defer func() {
		pw.matchesFound.Observe(float64(len(result)))
	}()
	// convert prometheus matchers to vulcan matchers
	m, err := toMatches(matchers...)
	if err != nil {
		log.Printf("%s", err)
		return map[model.Fingerprint]metric.Metric{}
	}
	// get matching metrics
	metrics, err := pw.metricResolver.Resolve(m)
	if err != nil {
		log.Println(err)
		return map[model.Fingerprint]metric.Metric{}
	}
	// convert vulcan metrics to fingerprinted prometheus metrics
	fpm := toFingerprintedMetric(toModelMetric(metrics))

	// hacky hacky hack to allow this preloader to resolve a fingerprint to
	// a metric since it will need to later fetch data for a metric with only
	// a fingerprint parameter
	// TODO write vulcan HTTP API from scratch to avoid this hack
	pw.Preloader.fingerPrintLock.Lock()
	// hack matching keys into fingerprints
	for fp, m := range fpm {
		pw.Preloader.HackFingerprint[fp] = m.Metric
	}
	pw.Preloader.fingerPrintLock.Unlock()
	// end hacky hacky hack

	result = fpm
	return result
}

// LastSampleForFingerprint Implements prometheus.Querier interface.  Returns
// the last sample for the provided fingerprint.
func (pw *PrometheusWrapper) LastSampleForFingerprint(model.Fingerprint) model.Sample {
	return model.Sample{}
}

// LabelValuesForLabelName Implements prometheus.Querier interface.
func (pw *PrometheusWrapper) LabelValuesForLabelName(name model.LabelName) model.LabelValues {
	vals, err := pw.metricResolver.Values(string(name))
	if err != nil {
		log.WithFields(log.Fields{
			"label_name": string(name),
		}).WithError(err).Warn("unable to get values for label name")
		return model.LabelValues{}
	}
	res := make(model.LabelValues, 0, len(vals))
	for _, val := range vals {
		res = append(res, model.LabelValue(val))
	}
	return res
}

// DropMetricsForFingerprints drops all time series associated with the given
// fingerprints.
func (pw *PrometheusWrapper) DropMetricsForFingerprints(...model.Fingerprint) {
	return
}

// Start runs the various maintenance loops in goroutines. Returns when the
// storage is ready to use. Keeps everything running in the background
// until Stop is called.
func (pw *PrometheusWrapper) Start() error {
	return nil
}

// Stop shuts down the Storage gracefully, flushes all pending
// operations, stops all maintenance loops,and frees all resources.
func (pw *PrometheusWrapper) Stop() error {
	return nil
}

// WaitForIndexing returns once all samples in the storage are
// indexed. Indexing is needed for FingerprintsForLabelMatchers and
// LabelValuesForLabelName and may lag behind.
func (pw *PrometheusWrapper) WaitForIndexing() {
}
