package convert

import (
	"encoding/json"

	"github.com/digitalocean/vulcan/bus"
)

// MetricToKey creates a consistent key for a metric. It piggy-backs off the
// property that golang json package sorts map keys while marshalling
// https://golang.org/src/encoding/json/encode.go#L615
func MetricToKey(m bus.Metric) (string, error) {
	// assign metric name the label "__name__" and gather all other metric labels
	labels := map[string]string{
		"__name__": m.Name,
	}
	for k, v := range m.Labels {
		labels[k] = v
	}
	b, err := json.Marshal(labels)
	return string(b), err
}
