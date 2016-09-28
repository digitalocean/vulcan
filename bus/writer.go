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

import "github.com/prometheus/prometheus/storage/remote"

// Writer is an interface that wraps the Write method to a message bus.
type Writer interface {
	// Write writes the timeseries data to the configured.
	// The key can be used as a partition key to where the the paylad req
	// should be written.
	Write(key string, req *remote.WriteRequest) error
}
