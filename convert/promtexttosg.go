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

package convert

import (
	"fmt"
	"io"
	"time"

	"github.com/digitalocean/vulcan/bus"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// PromTextToSG reads prometheus metrics in text exposition format and
// produces a SampleGroup
func PromTextToSG(in io.Reader) (bus.SampleGroup, error) {
	fam := []*dto.MetricFamily{}
	dec := expfmt.NewDecoder(in, expfmt.FmtText)
	for {
		var f dto.MetricFamily
		err := dec.Decode(&f)
		if err != nil {
			if err == io.EOF {
				break
			}
			return bus.SampleGroup{}, err
		}
		fam = append(fam, &f)
	}
	sg := familyToSampleGroup(fam)
	return sg, nil
}

const (
	labelType     = "__type__"
	labelLE       = "le"
	labelQuantile = "quantile"

	suffixBucket = "_bucket"
	suffixCount  = "_count"
	suffixSum    = "_sum"

	typeCounter   = "counter"
	typeGauge     = "gauge"
	typeHistogram = "histogram"
	typeSummary   = "summary"
)

func familyToSampleGroup(metricFamilies []*dto.MetricFamily) bus.SampleGroup {
	samples := bus.SampleGroup{}
	for _, mf := range metricFamilies {
		for _, prom := range mf.GetMetric() {
			m := bus.Metric{
				Name:   mf.GetName(),
				Labels: map[string]string{},
			}
			for _, label := range prom.GetLabel() {
				m.Labels[label.GetName()] = label.GetValue()
			}

			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				samples = append(samples, newCounterSample(prom, m))
			case dto.MetricType_GAUGE:
				samples = append(samples, newGaugeSample(prom, m))
			case dto.MetricType_HISTOGRAM:
				samples = collectHistogramSamples(samples, prom, m)
			case dto.MetricType_SUMMARY:
				samples = collectSummarySamples(samples, prom, m)
			}
		}
	}

	return samples
}

func newCounterSample(prom *dto.Metric, m bus.Metric) *bus.Sample {
	s := bus.Sample{
		Metric: m,
		Datapoint: bus.Datapoint{
			Value:     prom.Counter.GetValue(),
			Timestamp: promTimestamp(prom),
		},
	}
	s.Metric.Labels[labelType] = typeCounter

	return &s
}

func newGaugeSample(prom *dto.Metric, m bus.Metric) *bus.Sample {
	s := bus.Sample{
		Metric: m,
		Datapoint: bus.Datapoint{
			Value:     prom.Gauge.GetValue(),
			Timestamp: promTimestamp(prom),
		},
	}
	s.Metric.Labels[labelType] = typeGauge

	return &s
}

func collectHistogramSamples(samples bus.SampleGroup, prom *dto.Metric, m bus.Metric) bus.SampleGroup {
	// add bucket values, sum, and count for this prometheus metric
	m.Labels[labelType] = typeHistogram
	bucketName := m.Name + suffixBucket

	for _, b := range prom.Histogram.Bucket {
		myMetric := cloneMetric(m)
		myMetric.Name = bucketName
		myMetric.Labels[labelLE] = fmt.Sprint(b.GetUpperBound())

		s := bus.Sample{
			Metric: myMetric,
			Datapoint: bus.Datapoint{
				Value:     float64(b.GetCumulativeCount()),
				Timestamp: promTimestamp(prom),
			},
		}
		samples = append(samples, &s)
	}

	// do sum
	sum := cloneMetric(m)
	sum.Name = sum.Name + suffixSum
	s := bus.Sample{
		Metric: sum,
		Datapoint: bus.Datapoint{
			Value:     float64(*prom.Histogram.SampleSum),
			Timestamp: promTimestamp(prom),
		},
	}
	samples = append(samples, &s)

	// do count
	count := cloneMetric(m)
	count.Name = count.Name + suffixCount
	s = bus.Sample{
		Metric: count,
		Datapoint: bus.Datapoint{
			Value:     float64(*prom.Histogram.SampleCount),
			Timestamp: promTimestamp(prom),
		},
	}
	samples = append(samples, &s)

	return samples
}

func collectSummarySamples(samples bus.SampleGroup, prom *dto.Metric, m bus.Metric) bus.SampleGroup {
	m.Labels[labelType] = typeSummary

	for _, q := range prom.Summary.Quantile {
		myMetric := cloneMetric(m)
		myMetric.Labels[labelQuantile] = fmt.Sprint(q.GetQuantile())
		s := bus.Sample{
			Metric: myMetric,
			Datapoint: bus.Datapoint{
				Value:     float64(q.GetValue()),
				Timestamp: promTimestamp(prom),
			},
		}
		samples = append(samples, &s)
	}

	// do sum
	sum := cloneMetric(m)
	sum.Name = sum.Name + suffixSum
	s := bus.Sample{
		Metric: sum,
		Datapoint: bus.Datapoint{
			Value:     float64(*prom.Summary.SampleSum),
			Timestamp: promTimestamp(prom),
		},
	}
	samples = append(samples, &s)

	// do count
	count := cloneMetric(m)
	count.Name = count.Name + suffixCount
	s = bus.Sample{
		Metric: count,
		Datapoint: bus.Datapoint{
			Value:     float64(*prom.Summary.SampleCount),
			Timestamp: promTimestamp(prom),
		},
	}
	samples = append(samples, &s)

	return samples
}

func promTimestamp(m *dto.Metric) bus.Timestamp {
	if m.TimestampMs != nil {
		return bus.Timestamp(*m.TimestampMs)
	}
	return bus.Timestamp(time.Now().UnixNano() / 1e6)
}

func cloneMetric(m bus.Metric) bus.Metric {
	result := bus.Metric{
		Name:   m.Name,
		Labels: map[string]string{},
	}
	for k, v := range m.Labels {
		result.Labels[k] = v
	}
	return result
}
