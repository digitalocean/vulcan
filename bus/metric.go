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

package bus

// Metric must have a name and has 0..many labels. Modeled after prometheus
// metrics.
// TODO each prometheus metric also has a type (counter, gauge...) we should
// probably make this a field in the Metric struct. Right now the type is
// recorded as a label named "__type__".
// Deprecated in favor of model.TimeSeries
type Metric struct {
	Name   string
	Labels map[string]string
}
