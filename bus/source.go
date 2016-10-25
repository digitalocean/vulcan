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

import "github.com/digitalocean/vulcan/model"

// SourcePayload is a TimeSeriesBatch and an Ack function to signal that
// the payload has been processed.
type SourcePayload struct {
	TimeSeriesBatch model.TimeSeriesBatch
	Ack             func()
}

// Source is a mechanism for reading from the bus.
type Source interface {
	// Err SHOULD ONLY be called AFTER the messages channel has closed.
	// This lets the caller determine if the messages channel closed because
	// of an error or completed.
	Err() error
	// Messages returns a readable channel of SourcePayload. The payloads'
	// Ack function MUST be called after the caller is done processing the
	// payload. The channel will be closed when the Source encounters an
	// error or the stream finishes. The caller SHOULD call Error() after
	// the channel closes to determine if the channel closed because of
	// an error or not.
	Messages() <-chan *SourcePayload
}
