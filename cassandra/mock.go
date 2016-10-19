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

package cassandra

import (
	"errors"

	"github.com/digitalocean/vulcan/model"
)

// ErrMockReaderNoMoreSamples is returned when a MockReader.Sample slice is empty
var ErrMockReaderNoMoreSamples = errors.New("sample slice empty")

var _ Reader = &MockReader{}

// MockReader is a mock version of the the Reader interface.
type MockReader struct {
	Samples []*model.Sample
	Err     error
	Args    []string
}

// GetLastSample implements cassandra.Reader interface.
func (r *MockReader) GetLastSample(fqmn string) (*model.Sample, error) {
	var s *model.Sample

	r.Args = append(r.Args, fqmn)

	if r.Err != nil {
		return s, r.Err
	}

	if len(r.Samples) > 0 {
		s, r.Samples = r.Samples[0], r.Samples[1:]
		return s, nil
	}

	return nil, ErrMockReaderNoMoreSamples
}
