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

import "sync"

// NameIndex groups metrics by the label "__name__"
type NameIndex struct {
	entries map[string]*Index
	m       sync.RWMutex
}

// NewNameIndex creates a NameIndex.
func NewNameIndex() *NameIndex {
	return &NameIndex{
		entries: map[string]*Index{},
	}
}

// Add inserts an id and its labels into the index. It is expected that the labels for
// a given ID will always be the same.
func (ni *NameIndex) Add(id string, labels map[string]string) {
	name := labels["__name__"]
	ni.m.RLock()
	entry, ok := ni.entries[name]
	ni.m.RUnlock()
	if ok {
		entry.Add(id, labels)
		return
	}
	newEntry := NewIndex()
	ni.m.Lock()
	entry, ok = ni.entries[id]
	if ok {
		ni.m.Unlock()
		entry.Add(id, labels)
		return
	}
	ni.entries[name] = newEntry
	ni.m.Unlock()
	newEntry.Add(id, labels)
	return
}

// Resolve returns the unique timeseries IDs that match the provided matchers.
func (ni *NameIndex) Resolve(matchers []*Matcher) ([]string, error) {
	// ideally there's a name match and can resolve from just one index.
	for _, matcher := range matchers {
		if matcher.Type == MatcherType_Equal && matcher.Name == "__name__" {
			ni.m.RLock()
			i, ok := ni.entries[matcher.Value]
			ni.m.RUnlock()
			if !ok {
				return []string{}, nil
			}
			return i.Resolve(matchers)
		}
	}
	// otherwise we combine the resolve results from all indexes.
	ni.m.RLock()
	current := make(map[string]*Index, len(ni.entries))
	for k, v := range ni.entries {
		current[k] = v
	}
	ni.m.RUnlock()
	result := []string{}
	for _, i := range current {
		r, err := i.Resolve(matchers)
		if err != nil {
			return []string{}, err
		}
		result = append(result, r...)
	}
	return result, nil
}

// Values returns the unique values associated with a label.
func (ni *NameIndex) Values(field string) ([]string, error) {
	values := []string{}
	ni.m.RLock()
	defer ni.m.RUnlock()
	for _, idx := range ni.entries {
		v, err := idx.Values(field)
		if err != nil {
			return []string{}, err
		}
		values = append(values, v...)
	}
	values = dedupe(values)
	return values, nil
}
