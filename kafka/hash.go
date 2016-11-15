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

import "hash/fnv"

// HashNegativeAndReflectInsanity matches the partition hashing algorithm found in the
// sarama golang kafka driver. Casting a uint32 to an int and then handling the negative
// by negating the number seems insane, but return int32(h.Sum32() % uint32(count)) is not
// the same.
func HashNegativeAndReflectInsanity(key []byte, count int) int32 {
	h := fnv.New32a()
	// Write (via the embedded io.Writer interface) adds more data to the running hash.
	// It never returns an error.
	h.Write(key)
	p := int32(h.Sum32()) % int32(count)
	if p < 0 {
		p = -p
	}
	return p
}
