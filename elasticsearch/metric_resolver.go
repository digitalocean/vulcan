package elasticsearch

import (
	"fmt"

	"github.com/olivere/elastic"
	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"
)

type metricResolver struct {
	client *elastic.Client
	index  string
}

type MetricResolverConfig struct {
	URL   string
	Sniff bool
	Index string
}

func NewMetricResolver(config *MetricResolverConfig) (*metricResolver, error) {
	client, err := elastic.NewClient(elastic.SetURL(config.URL), elastic.SetSniff(config.Sniff))
	if err != nil {
		return nil, err
	}
	return &metricResolver{
		client: client,
		index:  config.Index,
	}, nil
}

func (mr *metricResolver) Resolve(eq map[string]string) ([]*bus.Metric, error) {
	q := elastic.NewBoolQuery()
	for k, v := range eq {
		q.Filter(elastic.NewTermQuery(fmt.Sprintf("%s.raw", convert.ESEscape(k)), v))
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
