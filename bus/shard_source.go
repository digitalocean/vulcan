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

import "context"

// ShardSource is a mechanism for reading from the bus that exposes that data
// can come from different partitions that a consumer may gain/lose responsibility
// for consuming while still being active. This is represented by the Messages
// function returning a channel of channel of SourcePayload. When the Messages
// channel closes, the ShardSource is done. When a SourcePayload channel ends,
// that shard of data is done. When a new SourcePayload channel comes over the
// Messages channel, the ShardSource is now responsible for a new shard of data.
type ShardSource interface {
	// Err SHOULD ONLY be called AFTER the messages channel has closed.
	// This lets the caller determine if the messages channel closed because
	// of an error or completed.
	Err() error
	Messages(context.Context) <-chan <-chan *SourcePayload
}
