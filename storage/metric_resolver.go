package storage

import "github.com/digitalocean/vulcan/bus"

// MetricResolver is a interface that wraps the Resolve method.
type MetricResolver interface {
	Resolve(map[string]string) ([]*bus.Metric, error)
}
