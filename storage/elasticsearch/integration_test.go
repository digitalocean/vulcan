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

// +build integration

package elasticsearch_test

import (
	"flag"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/digitalocean/vulcan/bus"
	oldes "github.com/digitalocean/vulcan/elasticsearch"
	"github.com/digitalocean/vulcan/storage"
	"github.com/digitalocean/vulcan/storage/elasticsearch"
	"github.com/olivere/elastic"
)

var es = flag.String("es", "http://localhost:9200", "elasticsearch url")

const matchAll = `{
  "template": "*",
  "order": 0,
  "settings": {
    "index": {
      "refresh_interval": "1s"
    }
  },
  "mappings": {
    "_default_": {
      "dynamic_templates": [
        {
          "string_fields": {
            "mapping": {
              "fields": {
                "raw": {
                  "ignore_above": 256,
                  "doc_values": true,
                  "index": "not_analyzed",
                  "type": "string"
                }
              },
              "omit_norms": true,
              "index": "analyzed",
              "type": "string"
            },
            "match_mapping_type": "string",
            "match": "*"
          }
        }
      ],
      "_all": {
        "omit_norms": true,
        "enabled": true
      }
    }
  }
}`

func down() error {
	err := downTemplate()
	if err != nil {
		return err
	}
	err = downIndex()
	return err
}

func downTemplate() error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/_template/match_all_0", *es), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func downIndex() error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/vulcan-integration", *es), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func up() error {
	err := upTemplate()
	if err != nil {
		return err
	}
	err = upEntries()
	return err
}

func upTemplate() error {
	url := fmt.Sprintf("%s/_template/match_all_0", *es)
	resp, err := http.DefaultClient.Post(url, "text/json", strings.NewReader(matchAll))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func upEntries() error {
	client, err := elastic.NewClient(elastic.SetURL(*es), elastic.SetSniff(false))
	if err != nil {
		return err
	}
	exists := []struct {
		Name   string
		Labels map[string]string
	}{
		{
			Name: "metric_1",
			Labels: map[string]string{
				"l": "value-one",
			},
		},
		{
			Name: "metric_1",
			Labels: map[string]string{
				"l":     "value-one",
				"other": "value",
			},
		},
		{
			Name: "metric_1",
			Labels: map[string]string{
				"l": "value-two",
			},
		},
		{
			Name: "metric_1",
			Labels: map[string]string{
				"l": "value-three",
			},
		},
	}
	i := oldes.NewSampleIndexer(&oldes.SampleIndexerConfig{
		Client: client,
		Index:  "vulcan-integration",
	})
	for _, e := range exists {
		err := i.IndexSample(&bus.Sample{
			Metric: bus.Metric{
				Name:   e.Name,
				Labels: e.Labels,
			},
		})
		if err != nil {
			return err
		}
	}
	// let elasticsearch realize it has data for a second
	// note the template needs time to match and it is set for a 1s refresh
	time.Sleep(time.Second * 2)
	return nil
}

func TestResolve(t *testing.T) {
	err := down()
	if err != nil {
		t.Error(err)
	}
	err = up()
	if err != nil {
		t.Error(err)
	}
	r, err := elasticsearch.NewResolver(&elasticsearch.ResolverConfig{
		URL:   *es,
		Sniff: false,
		Index: "vulcan-integration",
	})
	if err != nil {
		t.Error(err)
	}
	tests := []struct {
		Matches []*storage.Match
		Count   int
	}{
		{
			Matches: []*storage.Match{
				&storage.Match{
					Type:  storage.Equal,
					Name:  "__name__",
					Value: "metric_1",
				},
				&storage.Match{
					Type:  storage.Equal,
					Name:  "l",
					Value: "value-one",
				},
			},
			Count: 2,
		},
		{
			Matches: []*storage.Match{
				&storage.Match{
					Type:  storage.Equal,
					Name:  "__name__",
					Value: "metric_1",
				},
				&storage.Match{
					Type:  storage.Equal,
					Name:  "l",
					Value: "value-one",
				},

				&storage.Match{
					Type:  storage.Equal,
					Name:  "other",
					Value: "value",
				},
			},
			Count: 1,
		},
		{
			Matches: []*storage.Match{
				&storage.Match{
					Type:  storage.Equal,
					Name:  "__name__",
					Value: "metric_1",
				},
				&storage.Match{
					Type:  storage.NotEqual,
					Name:  "l",
					Value: "value-one",
				},
				&storage.Match{
					Type:  storage.Equal,
					Name:  "other",
					Value: "value",
				},
			},
			Count: 0,
		},
		{
			Matches: []*storage.Match{
				&storage.Match{
					Type:  storage.Equal,
					Name:  "__name__",
					Value: "metric_1",
				},
				&storage.Match{
					Type:  storage.NotEqual,
					Name:  "l",
					Value: "value-one",
				},
				&storage.Match{
					Type:  storage.Equal,
					Name:  "other",
					Value: "value",
				},
			},
			Count: 0,
		},
		{
			Matches: []*storage.Match{
				&storage.Match{
					Type:  storage.Equal,
					Name:  "__name__",
					Value: "metric_1",
				},
				&storage.Match{
					Type:  storage.RegexMatch,
					Name:  "l",
					Value: ".*one",
				},
				&storage.Match{
					Type:  storage.Equal,
					Name:  "other",
					Value: "value",
				},
			},
			Count: 1,
		},
		{
			Matches: []*storage.Match{
				&storage.Match{
					Type:  storage.Equal,
					Name:  "__name__",
					Value: "metric_1",
				},
				&storage.Match{
					Type:  storage.RegexNoMatch,
					Name:  "l",
					Value: ".*two",
				},
			},
			Count: 3,
		},
	}
	for _, test := range tests {
		m, err := r.Resolve(test.Matches)
		if err != nil {
			t.Error(err)
		}
		if len(m) != test.Count {
			t.Errorf("want %d resolved metrics got %d", test.Count, len(m))
		}
	}
}
