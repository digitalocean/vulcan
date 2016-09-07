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

package forwarder

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/digitalocean/vulcan/bus"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

// Forwarder represents an object that accepts metrics from Prometheus.
// Metrics are grouped by target instance and written to the configured
// Vulcan message bus.
type Forwarder struct {
	writer bus.Writer
}

// Config represents the configuration for a Forwarder.
type Config struct {
	Writer bus.Writer
}

// NewForwarder creates a new instance of Forwarder.
func NewForwarder(config *Config) *Forwarder {
	return &Forwarder{
		writer: config.Writer,
	}
}

// Write implements remote.WriteClient interface.
func (f *Forwarder) Write(ctx context.Context, req *remote.WriteRequest) (*remote.WriteResponse, error) {
	var (
		toWrite = map[string]*remote.WriteRequest{}
		ll      = log.WithFields(log.Fields{"source": "forwarder.Write"})
	)
	// Batch time series data by instance, fallback on address.
	for _, ts := range req.Timeseries {
		key, err := getKey(ts)
		if err != nil {
			ll.WithFields(log.Fields{
				"timeseries": ts,
			}).WithError(err).Error("could not formulate key from labels")

			continue
		}

		if _, ok := toWrite[key]; !ok {
			toWrite[key] = &remote.WriteRequest{
				Timeseries: []*remote.TimeSeries{ts},
			}
		} else {
			toWrite[key].Timeseries = append(toWrite[key].Timeseries, ts)
		}
	}

	// Write each batch to the Vulcan bus.
	for key, wr := range toWrite {
		ll.WithFields(log.Fields{
			"key":              key,
			"timeseries_count": len(wr.Timeseries),
		}).Info("writing to bus")

		if err := f.writer.Write(key, wr); err != nil {
			log.WithFields(log.Fields{
				"key": key,
			}).WithError(err).Error("failed to write to bus")

			continue
		}
	}

	return &remote.WriteResponse{}, nil
}

// getKey formulates the instance key based on Prometheus metric labels.
func getKey(ts *remote.TimeSeries) (string, error) {
	var (
		jobName, inst string
		instFound     bool
	)

	for _, l := range ts.Labels {
		if jobName != "" && instFound {
			break
		}

		switch l.Name {
		case model.JobLabel:
			jobName = l.Value

		case model.InstanceLabel:
			inst = l.Value
			instFound = true

		case model.AddressLabel:
			if inst == "" {
				inst = l.Value
			}
		}
	}

	if jobName == "" {
		return "", errors.New("missing job label")
	}

	if inst == "" {
		return "", errors.New("missing instance label and address label")
	}

	return fmt.Sprintf("%s-%s", jobName, inst), nil
}
