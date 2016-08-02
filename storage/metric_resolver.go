package storage

import "github.com/digitalocean/vulcan/bus"

type MetricResolver interface {
	Resolve(map[string]string) ([]*bus.Metric, error)
}
