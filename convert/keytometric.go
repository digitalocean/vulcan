package convert

import (
	"encoding/json"

	"github.com/digitalocean/vulcan/bus"
)

// KeyToMetric produces a Vulcan metric for a consistent key.
func KeyToMetric(key string) (*bus.Metric, error) {
	m := &bus.Metric{
		Labels: map[string]string{},
	}
	mp := map[string]string{}
	err := json.Unmarshal([]byte(key), &mp)
	if err != nil {
		return m, err
	}
	for k, v := range mp {
		if k == "__name__" {
			m.Name = v
			continue
		}
		m.Labels[k] = v
	}
	return m, err
}
