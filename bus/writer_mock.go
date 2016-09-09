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

package bus

import (
	"sort"

	"github.com/prometheus/prometheus/storage/remote"
)

// MOCKWRITEERR is a constant for the error string for a returned MockWriter error.
const MOCKWRITEERR = `WRITEERR`

// MockWriteArgs is an object that represents the arguments passed to the Write function.
type MockWriteArgs struct {
	Key          string
	WriteRequest *remote.WriteRequest
}

// MockWriter is an object respresents a fake message bus writer.
type MockWriter struct {
	WErr       error
	WriteCount int
	Args       []*MockWriteArgs
}

// NewMockWriter returns an instance of MockWriter.
func NewMockWriter() *MockWriter {
	return &MockWriter{
		Args: make([]*MockWriteArgs, 0),
	}
}

// Write implements bus.Writer.
func (w *MockWriter) Write(k string, wr *remote.WriteRequest) error {
	w.Args = append(w.Args, &MockWriteArgs{Key: k, WriteRequest: wr})

	if w.WErr == nil {
		w.WriteCount++
		return nil
	}

	return w.WErr
}

type argsSorter struct {
	args []*MockWriteArgs
	by   func(a1, a2 *MockWriteArgs) bool
}

// Len implements sort.Sort interface.
func (s *argsSorter) Len() int { return len(s.args) }

// Swap implements sort.Sort interface.
func (s *argsSorter) Swap(i, j int) {
	s.args[i], s.args[j] = s.args[j], s.args[i]
}

// Less implements sort.Sort interface.
func (s *argsSorter) Less(i, j int) bool {
	return s.by(s.args[i], s.args[j])
}

// ByMockWriterArgs is a less function that defines how to order a MockWriteArgs.
type ByMockWriterArgs func(a1, a2 *MockWriteArgs) bool

// Sort sorts the MockWriteArgs slice by the criteria defined in the By function.
func (by ByMockWriterArgs) Sort(args []*MockWriteArgs) {
	s := &argsSorter{args: args, by: by}
	sort.Sort(s)
}

// Sorting types

// WriteArgKey allows sorting by the Key associated of MockWriteArgs.
func WriteArgKey(a1, a2 *MockWriteArgs) bool {
	return a1.Key < a2.Key
}

// WriteArgTimeSeriesLength allows sorting by the length of the Timeseries slice of
// the WriteRequest associated with MockWriteArgs.
func WriteArgTimeSeriesLength(a1, a2 *MockWriteArgs) bool {
	return len(a1.WriteRequest.Timeseries) < len(a2.WriteRequest.Timeseries)
}
