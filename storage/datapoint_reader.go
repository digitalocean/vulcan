package storage

import "github.com/digitalocean/vulcan/bus"

// DatapointReader is an interface that wraps the ReadDatapoints method.
type DatapointReader interface {
	ReadDatapoints(key string, after, before bus.Timestamp) ([]bus.Datapoint, error)
}
