package storage

import "github.com/digitalocean/vulcan/bus"

type DatapointReader interface {
	ReadDatapoints(key string, after, before bus.Timestamp) ([]bus.Datapoint, error)
}
