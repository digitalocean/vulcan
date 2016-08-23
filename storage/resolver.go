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

package storage

import "github.com/digitalocean/vulcan/bus"

// MatchType is an enum representing a PromQL label match from "=", "!=", "=~"
// or "!~"
type MatchType int

const (
	// Equal is "=" in PromQL
	Equal MatchType = iota
	// NotEqual is "!=" in PromQL
	NotEqual
	// RegexMatch is "=~" in PromQL
	RegexMatch
	// RegexNoMatch is "!~" in PromQL
	RegexNoMatch
)

// Match represents the PromQL matching function and on what label name to
// operate.
type Match struct {
	Type  MatchType
	Name  string
	Value string
}

// Resolver is a interface that wraps a database that can answer questions
// on what metrics exist.
type Resolver interface {
	// Resolve makes a query using the provided key value pairs of query
	// params and transforms the results to Vulcan Metric type.
	Resolve([]*Match) ([]*bus.Metric, error)
	Values(field string) ([]string, error)
}
