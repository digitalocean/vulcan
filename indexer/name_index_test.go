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
	"encoding/json"
	"testing"

	"github.com/digitalocean/vulcan/model"
)

func TestNameIndex(t *testing.T) {
	i := NewNameIndex()
	entries := []map[string]string{
		map[string]string{
			"__name__": "node_load1",
			"region":   "nyc3",
		},
		map[string]string{
			"__name__": "node_load1",
			"region":   "nyc2",
		},
		map[string]string{
			"region": "nyc3",
			"oops":   "no name",
		},
	}
	for _, entry := range entries {
		raw, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		id := string(raw)
		labels, err := model.LabelsFromTimeSeriesID(id)
		if err != nil {
			t.Fatal(err)
		}
		i.Add(id, labels)
	}
	ids, err := i.Resolve([]*Matcher{
		{
			Type:  MatcherType_Equal,
			Name:  "__name__",
			Value: "node_load1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Errorf("expected ids to be len 2 but got %d", len(ids))
	}
}
