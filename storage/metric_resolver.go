package storage

import "github.com/digitalocean/vulcan/bus"

// MetricResolver is a interface that wraps the Resolve method.
type MetricResolver interface {
	// Resolve makes a query using the provided key value pairs of query
	// params and transforms the results to Vulcan Metric type.
	Resolve(map[string]string) ([]*bus.Metric, error)
}
