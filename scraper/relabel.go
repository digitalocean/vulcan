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

package scraper

import (
	"net/http"
	"net/url"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	pconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
)

type relabelerFetcher struct {
	relabelCfgs []*pconfig.RelabelConfig
	client      *http.Client
}

func (r *relabelerFetcher) fetch(u *url.URL) ([]*dto.MetricFamily, error) {
	resp, err := r.client.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("response status %s", resp.Status)
	}

	fams, err := parse(resp.Body, resp.Header)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse response")
	}

	// relabel label sets
	for _, mf := range fams {
		for _, pm := range mf.Metric {
			if pm.GetLabel() != nil {
				// log and move on to next metric
				relabeled, err := r.relabel(pm.Label)
				if err != nil {
					log.WithFields(log.Fields{
						"metric_name": pm.String(),
					}).WithError(err).Error("could not relabel")
					continue
				}

				pm.Label = relabeled
			}
		}
	}

	return fams, nil
}

func (r *relabelerFetcher) relabel(labels []*dto.LabelPair) ([]*dto.LabelPair, error) {
	relabeledSets, err := retrieval.Relabel(toLabelSet(labels), r.relabelCfgs...)
	if err != nil {
		return nil, errors.Wrap(err, "could not metric relabel")
	}

	if relabeledSets == nil {
		return nil, errors.New("time series dropped")
	}

	return toLabelPair(relabeledSets), nil
}

func toLabelSet(lps []*dto.LabelPair) model.LabelSet {
	labelSet := make(model.LabelSet, len(lps))
	for _, lp := range lps {
		labelSet[model.LabelName(lp.GetName())] = model.LabelValue(lp.GetValue())
	}

	return labelSet
}

func toLabelPair(ls model.LabelSet) []*dto.LabelPair {
	var (
		labelpairs = make([]*dto.LabelPair, 0, len(ls))
		sPt        = func(s string) *string { return &s }
	)

	for k, v := range ls {
		labelpairs = append(labelpairs, &dto.LabelPair{
			Name:  sPt(string(k)),
			Value: sPt(string(v)),
		})
	}

	return labelpairs
}
