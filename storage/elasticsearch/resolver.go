package elasticsearch

import (
	"fmt"

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
func (mr *Resolver) Resolve(matches []*storage.Match) ([]*bus.Metric, error) {
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
	sr, err := mr.client.Search().
		Index(mr.index).
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
