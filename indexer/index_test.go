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

func TestIndex(t *testing.T) {
	i := NewIndex()
	entries := []map[string]string{
		map[string]string{
			"test": "value",
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
			Name:  "test",
			Value: "value",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Errorf("expected ids to be len 1 but got %d", len(ids))
	}
}
