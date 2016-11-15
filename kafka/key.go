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

package kafka

// Job is necessary for creating kafka routing key. It's its own type mostly to protect the
// user from inadvertently swapping the order of Job and Instance to the Key function.
type Job string

// Instance is necessary for creating a kafka routing key. It's its own type mostly to protect the
// user from inadvertently swapping the order of Job and Instance to the Key function.
type Instance string

const separator byte = '-'

// Key returns a routing key from a job and instance. Since this is called many many times,
// we build the result byte slice with a pre-allocated buffer and copy instead of a simple
// fmt.Sprintf call.
func Key(job Job, instance Instance) []byte {
	buf := make([]byte, len(job)+len(instance)+1)
	copy(buf, job)
	buf[len(job)] = separator
	copy(buf[len(job)+1:], instance)
	return buf
}
