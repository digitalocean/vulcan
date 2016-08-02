package storage

import "github.com/digitalocean/vulcan/bus"

// SampleWriter is an interface that wraps methods needed to write to the
// database of the storage layer.
type SampleWriter interface {
	// WriteSample writes a single sample to a database
	WriteSample(*bus.Sample) error
}
