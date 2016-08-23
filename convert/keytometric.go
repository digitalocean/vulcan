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

// KeyToMetric produces a Vulcan metric for a consistent key.
func KeyToMetric(key string) (*bus.Metric, error) {
	m := &bus.Metric{
		Labels: map[string]string{},
	}
	mp := map[string]string{}
	err := json.Unmarshal([]byte(key), &mp)
	if err != nil {
		return m, err
	}
	for k, v := range mp {
		if k == "__name__" {
			m.Name = v
			continue
		}
		m.Labels[k] = v
	}
	return m, err
}
