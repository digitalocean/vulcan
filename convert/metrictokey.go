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
	"encoding/json"

	"github.com/digitalocean/vulcan/bus"
)

// MetricToKey creates a consistent key for a metric. It piggy-backs off the
// property that golang json package sorts map keys while marshalling
// https://golang.org/src/encoding/json/encode.go#L615
func MetricToKey(m bus.Metric) (string, error) {
	// assign metric name the label "__name__" and gather all other metric labels
	labels := map[string]string{
		"__name__": m.Name,
	}
	for k, v := range m.Labels {
		labels[k] = v
	}
	b, err := json.Marshal(labels)
	return string(b), err
}
