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

package elasticsearch

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"
	"github.com/digitalocean/vulcan/storage"

	"github.com/olivere/elastic"
)

// Resolver represents an object that makes queries against a target
// ElasticSearch cluster and transforms the results to a Vulcan Metric type.
type Resolver struct {
	client *elastic.Client
	index  string
}

// ResolverConfig represents the configuration of a Resolver.
type ResolverConfig struct {
	URL   string
	Sniff bool
	Index string
}

// NewResolver creates an instance of Resolver.
func NewResolver(config *ResolverConfig) (*Resolver, error) {
	client, err := elastic.NewClient(elastic.SetURL(config.URL), elastic.SetSniff(config.Sniff))
	if err != nil {
		return nil, err
	}
	return &Resolver{
		client: client,
		index:  config.Index,
	}, nil
}

// Resolve implements the storage.Resolver interface.
func (r *Resolver) Resolve(matches []*storage.Match) ([]*bus.Metric, error) {
	q := elastic.NewBoolQuery()
	for _, m := range matches {
		switch m.Type {
		case storage.Equal:
			q.Filter(elastic.NewTermQuery(fmt.Sprintf("%s.raw", convert.ESEscape(m.Name)), m.Value))
		case storage.NotEqual:
			q.MustNot(elastic.NewTermQuery(fmt.Sprintf("%s.raw", convert.ESEscape(m.Name)), m.Value))
		case storage.RegexMatch:
			q.Filter(elastic.NewRegexpQuery(fmt.Sprintf("%s.raw", convert.ESEscape(m.Name)), m.Value))
		case storage.RegexNoMatch:
			q.MustNot(elastic.NewRegexpQuery(fmt.Sprintf("%s.raw", convert.ESEscape(m.Name)), m.Value))
		default:
			return []*bus.Metric{}, fmt.Errorf("unhandled match type")
		}
	}
	sr, err := r.client.Search().
		Index(r.index).
		NoFields().         // only want the _id
		Sort("_doc", true). // sort by _doc since it is most efficient to return large results https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html
		Query(q).
		Size(10000). // arbitrarily long TODO handle sizing the result set better
		Do()
	if err != nil {
		return []*bus.Metric{}, err
	}
	ml := []*bus.Metric{}
	for _, hit := range sr.Hits.Hits {
		m, err := convert.KeyToMetric(hit.Id)
		if err != nil {
			return []*bus.Metric{}, err
		}
		ml = append(ml, m)
	}
	return ml, nil
}

// Values returns the unique values for a given metric field. This allows
// vulcan to provide hinting on what metrics exist or what label values
// a query should use in a filter.
func (r *Resolver) Values(field string) ([]string, error) {
	aggr := elastic.NewTermsAggregation().
		Field(convert.ESEscape(field)).
		Size(10000) // arbitrarily large
	res, err := r.client.Search().
		Index(r.index).
		Query(elastic.NewMatchAllQuery()).
		SearchType("count").
		Aggregation("label_values", aggr).
		Do()
	if err != nil {
		return []string{}, err
	}
	itms, found := res.Aggregations.Terms("label_values")
	if !found {
		return []string{}, nil
	}
	values := []string{}
	for _, bucket := range itms.Buckets {
		if key, ok := bucket.Key.(string); ok {
			values = append(values, key)
			continue
		}
		log.WithFields(log.Fields{
			"field": field,
		}).Warning("unable to cast result bucket key to string")
	}
	return values, nil
}
