package storage

import "github.com/digitalocean/vulcan/bus"

type SampleWriter interface {
	// WriteSample writes a single sample to a database
	WriteSample(*bus.Sample) error
}
