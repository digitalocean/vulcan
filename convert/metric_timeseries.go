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
	"github.com/digitalocean/vulcan/model"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
)

// MetricToTimeSeries converts the prometheus storage metric type to a Vulcan
// model TimeSeries.
func MetricToTimeSeries(m metric.Metric) model.TimeSeries {
	ts := model.TimeSeries{
		Labels:  make(map[string]string, len(m.Metric)),
		Samples: []*model.Sample{},
	}
	for k, v := range m.Metric {
		ts.Labels[string(k)] = string(v)
	}
	return ts
}

// TimeSeriesToMetric converts the Vulcan storage metric type to a prometheus
// storage metric type.
func TimeSeriesToMetric(ts *model.TimeSeries) metric.Metric {
	m := metric.Metric{
		Metric: make(prommodel.Metric, len(ts.Labels)),
	}
	for k, v := range ts.Labels {
		m.Metric[prommodel.LabelName(k)] = prommodel.LabelValue(v)
	}
	return m
}

// TimeSeriesBatchToMetrics converts the Vulcan TimeSeriesBatch to a slice of
// prometheus storage metric type.
func TimeSeriesBatchToMetrics(tsb model.TimeSeriesBatch) []metric.Metric {
	result := make([]metric.Metric, len(tsb))
	for i, ts := range tsb {
		result[i] = TimeSeriesToMetric(ts)
	}
	return result
}
