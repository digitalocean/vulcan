// Copyright 2016 The Vulcan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compressor

import (
	"bytes"
	"context"
	"html/template"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

type lastSQLVars struct {
	Table string
}

type upsertSQLVars struct {
	Table string
	TTL   int
}

const (
	lastSQL = `
SELECT chunk
FROM {{.Table}}
WHERE id = ?
ORDER BY start DESC
LIMIT 1`
	upsertSQL = `
INSERT INTO {{.Table}} (id, start, chunk) 
VALUES (?, ?, ?)
USING TTL {{.TTL}}`
)

// Overwriter attemps to pack the most samples per byte into Cassandra. Because
// we have to flush varbit encoded data to cassanddra based on time, we are often
// flushing 1k varbit blocks that have space left for more samples. Using the overwriter,
// we read the last varbit block in the database to see if we can further compress datapoints
// into that block. This causes tombstones in cassandra, but should be compacted away after
// an early few compaction cycles. This seems worth it to get better compaction. In an attempt
// to make sure we don't cause compaction in Cassandra for data that is too old and already
// compacted into large sstables we no longer want to alter, we can set a time threshold that
// won't attempt to recompact blocks older than that threshold.
type Overwriter struct {
	cfg       *OverwriterConfig
	s         *gocql.Session
	work      chan *work
	lastSQL   string
	upsertSQL string
}

// OverwriterConfig is needed to create an Overwriter.
type OverwriterConfig struct {
	Context context.Context
	Session *gocql.Session
	Table   string
	TTL     time.Duration
	// Window is a duration relative to time.Now that if a previously flushed datapoint chunk
	// is within the window, we will attempt to compact a new flush with this data and overwrite.
	Window  time.Duration
	Workers int
}

// NewOverwriter is able to flush chunks to cassandra and attempts to compact existing
// data into more compact chunks.
func NewOverwriter(cfg *OverwriterConfig) (*Overwriter, error) {
	lastSQLBuf := bytes.NewBuffer(make([]byte, 0))
	t, err := template.New("lastSQL").Parse(lastSQL)
	if err != nil {
		return nil, err
	}
	err = t.Execute(lastSQLBuf, lastSQLVars{
		Table: cfg.Table,
	})
	if err != nil {
		return nil, err
	}
	t, err = template.New("upsertSQL").Parse(upsertSQL)
	if err != nil {
		return nil, err
	}
	upsertSQLBuf := bytes.NewBuffer(make([]byte, 0))
	err = t.Execute(upsertSQLBuf, upsertSQLVars{
		Table: cfg.Table,
		TTL:   int(cfg.TTL.Seconds()),
	})
	if err != nil {
		return nil, err
	}
	o := &Overwriter{
		cfg:       cfg,
		s:         cfg.Session,
		work:      make(chan *work),
		lastSQL:   lastSQLBuf.String(),
		upsertSQL: upsertSQLBuf.String(),
	}
	for i := 0; i < cfg.Workers; i++ {
		go o.worker()
	}
	return o, nil
}

type work struct {
	Context context.Context
	ID      string
	Chunk   chunk.Chunk
	Return  chan<- error
}

// Flush concurrently writes the provided chunks to cassandra.
func (o *Overwriter) Flush(ctx context.Context, chunks map[string]chunk.Chunk) error {
	ch := make(chan error, len(chunks))
	for id, c := range chunks {
		o.work <- &work{
			Chunk:   c,
			Context: ctx,
			ID:      id,
			Return:  ch,
		}
	}
	for i := 0; i < len(chunks); i++ {
		err := <-ch
		if err != nil {
			return err
		}
	}
	return nil
}

// Flush writes an individual chunk to cassandra.
func (o *Overwriter) flush(ctx context.Context, id string, c chunk.Chunk) error {
	last, err := o.last(ctx, id)
	if err != nil {
		return err
	}
	if last == nil {
		start := int64(c.FirstTime())
		return o.upsert(ctx, id, start, c)
	}
	// we don't want to overwrite data in cassandra once it has been in the database long
	// enough and has been compacted into more permenent sstables.
	cutoff := time.Now().Add(o.cfg.Window)
	start := int64(last.FirstTime())
	t := time.Unix(start/1e3, start*1e6)
	if t.Before(cutoff) {
		// insert new chunk instead of trying to compact and overwrite the last chunk.
		return o.upsert(ctx, id, start, c)
	}
	chunks, err := compact([]chunk.Chunk{last, c})
	if err != nil {
		return err
	}
	for _, chk := range chunks {
		start := int64(chk.FirstTime())
		err := o.upsert(ctx, id, start, chk)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Overwriter) worker() {
	for w := range o.work {
		w.Return <- o.flush(w.Context, w.ID, w.Chunk)
	}
}

// compact receives a time-ascending slice of chunks and returns a data-equivalent slice of
// chunks where as many datapoints as possible are written to the first chunk and any
// overflow is written to the following chunks.
func compact(chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	result := make([]chunk.Chunk, 0, 1)
	next, err := chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return nil, err
	}
	for _, c := range chunks {
		i := c.NewIterator()
		for i.Scan() {
			chunks, err := next.Add(i.Value())
			if err != nil {
				return nil, err
			}
			next = chunks[0]
			if len(chunks) == 1 {
				continue
			}
			if len(chunks) != 2 {
				panic("adding value to chunk should result in 1 or 2 length slice")
			}
			result = append(result, next)
			overflow := chunks[1]
			next, err = chunk.NewForEncoding(chunk.Varbit)
			if err != nil {
				return nil, err
			}
			oi := overflow.NewIterator()
			for oi.Scan() {
				chunks, err = next.Add(oi.Value())
				if err != nil {
					return nil, err
				}
				if len(chunks) != 1 {
					panic("expected to be able to add single value to new chunk without overflow")
				}
				next = chunks[0]
			}
		}
	}
	result = append(result, next)
	return result, nil
}

// last returns a nil-able chunk for the last varbit chunk in cassandra.
// Nil will be returned when there is no last value.
func (o *Overwriter) last(ctx context.Context, id string) (chunk.Chunk, error) {
	buf := make([]byte, 0)
	err := o.s.Query(o.lastSQL, id).WithContext(ctx).Scan(&buf)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c, err := chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return nil, err
	}
	err = c.UnmarshalFromBuf(buf)
	return c, err
}

func (o *Overwriter) upsert(ctx context.Context, id string, start int64, c chunk.Chunk) error {
	buf := make([]byte, chunk.ChunkLen)
	err := c.MarshalToBuf(buf)
	if err != nil {
		return err
	}
	return o.s.Query(o.upsertSQL, id, start, buf).WithContext(ctx).Exec()
}
