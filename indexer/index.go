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

package indexer

import (
	"fmt"
	"regexp"
	"sync"
)

// Index can resolve matchers to fully qualified metric names.
type Index struct {
	entries map[string]map[string]string
	m       sync.RWMutex
}

func NewIndex() *Index {
	return &Index{
		entries: map[string]map[string]string{},
	}
}

// Add inserts an id and its labels into the index. It is expected that the labels for
// a given ID will always be the same.
func (i *Index) Add(id string, labels map[string]string) {
	i.m.RLock()
	_, ok := i.entries[id]
	i.m.RUnlock()
	if ok {
		return
	}
	i.m.Lock()
	_, ok = i.entries[id]
	if ok {
		i.m.Unlock()
		return
	}
	i.entries[id] = labels
	i.m.Unlock()
	return
}

// Resolve returns the unique timeseries IDs that match the provided matchers.
func (i *Index) Resolve(matchers []*Matcher) ([]string, error) {
	i.m.RLock()
	current := make(map[string]map[string]string, len(i.entries))
	for k, v := range i.entries {
		current[k] = v
	}
	i.m.RUnlock()
	// TODO magic re-ordering of matchers for maximum effectiveness.
	for _, matcher := range matchers {
		next := make(map[string]map[string]string, len(current))
	NextEntry:
		for id, labels := range current {
			for name, value := range labels {
				if matcher.Name != name {
					continue
				}
				switch matcher.Type {
				case MatcherType_Equal:
					if matcher.Value == value {
						next[id] = labels
						continue NextEntry
					}
				case MatcherType_NotEqual:
					if matcher.Value != value {
						next[id] = labels
						continue NextEntry
					}
				case MatcherType_RegexMatch:
					re, err := regexp.Compile(matcher.Value)
					if err != nil {
						return nil, err
					}
					if re.MatchString(value) {
						next[id] = labels
						continue NextEntry
					}
				case MatcherType_RegexNoMatch:
					re, err := regexp.Compile(matcher.Value)
					if err != nil {
						return nil, err
					}
					if !re.MatchString(value) {
						next[id] = labels
						continue NextEntry
					}
				default:
					panic("unhandled matcher type")
				}
			}
		}
		current = next
	}
	result := make([]string, 0, len(current))
	for id := range current {
		result = append(result, id)
	}
	return result, nil
}

// Values returns the unique values associated with a label.
func (i *Index) Values(field string) ([]string, error) {
	return []string{}, fmt.Errorf("not implemented")
}
