package compressor

import (
	"context"
	"fmt"
	"time"

	"github.com/digitalocean/vulcan/model"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

// AccumulatorConfig is used to create a new Accumulator.
type AccumulatorConfig struct {
	Context          context.Context
	Flush            func(buf []byte, start, end int64)
	MaxSampleDelta   time.Duration
	MaxDirtyDuration time.Duration
}

// Accumulator collects samples for a single metric and calls a provided flush
// function when the internal data structure has reached a maximum size limit,
// or the time elapsed between the first and most recent sample exceeds a provided
// maximum, or the time elapsed from making the accumulator dirty to now exceeds
// a provided maximum.
type Accumulator struct {
	c    chunk.Chunk
	cfg  *AccumulatorConfig
	last int64
}

func NewAccumulator(cfg *AccumulatorConfig) (*Accumulator, error) {
	c, err := chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return nil, err
	}
	return &Accumulator{
		c:   c,
		cfg: cfg,
	}, nil
}

func (a *Accumulator) Append(sample model.Sample) error {
	if sample.TimestampMS < a.last {
		// silently skip appending samples older than our current position.
		return nil
	}
	chunks, err := a.c.Add(pmodel.SamplePair{
		Timestamp: pmodel.Time(sample.TimestampMS),
		Value:     pmodel.SampleValue(sample.Value),
	})
	if err != nil {
		return err
	}
	switch len(chunks) {
	case 1:
		a.c = chunks[0]
	case 2:
		buf := make([]byte, chunk.ChunkLen)
		err := chunks[0].MarshalToBuf(buf)
		if err != nil {
			return err
		}
		start := int64(chunks[0].FirstTime())
		end := a.last
		a.cfg.Flush(buf, start, end)
		a.c, err = chunk.NewForEncoding(chunk.Varbit)
		if err != nil {
			return err
		}
		iter := chunks[1].NewIterator()
		for iter.Scan() {
			chunks, err := a.c.Add(iter.Value())
			if err != nil {
				return err
			}
			if len(chunks) != 1 {
				return fmt.Errorf("expected overflow chunk items to fit into newly allocated chunk")
			}
			a.c = chunks[0]
		}
	default:
		panic("appending a sample to a chunk without error should always result in 1 or 2 elements in chunk slice")
	}
	a.last = sample.TimestampMS
	return nil
}
