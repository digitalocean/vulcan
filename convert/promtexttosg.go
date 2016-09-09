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
	"time"

	"github.com/digitalocean/vulcan/bus"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

// PromTextToSG reads prometheus metrics in text exposition format and
// produces a SampleGroup
func PromTextToSG(in []byte) (bus.SampleGroup, error) {
	wr := &remote.WriteRequest{}

	if err := proto.Unmarshal(in, wr); err != nil {
		return nil, err
	}

	sg := writeRequestToSampleGroup(wr)
	return sg, nil
}

func writeRequestToSampleGroup(wr *remote.WriteRequest) bus.SampleGroup {
	sg := bus.SampleGroup{}

	for _, ts := range wr.Timeseries {
		// Create metric attribute for TimeSeries samples
		m := bus.Metric{
			Labels: map[string]string{},
		}

		for _, lp := range ts.Labels {
			m.Labels[lp.Name] = lp.Value

			if lp.Name == model.MetricNameLabel {
				m.Name = lp.Value
			}
		}

		// Convert prometheus samples & add to Sample Group
		for _, ps := range ts.Samples {
			sg = append(sg, &bus.Sample{
				Metric: m,
				Datapoint: bus.Datapoint{
					Timestamp: checkTimestamp(ps),
					Value:     ps.Value,
				},
			})
		}
	}

	return sg
}

func checkTimestamp(ps *remote.Sample) bus.Timestamp {
	if ps.TimestampMs != 0 {
		return bus.Timestamp(ps.TimestampMs)
	}

	return bus.Timestamp(time.Now().UnixNano() / 1e6)
}
