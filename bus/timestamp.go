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

// Timestamp is the milliseconds since the unix epoch. We use this instead of go's time.Time
// since prometheus expects time to be represented in this way, and this value is also more
// natural for our data model.
//
// converting to a go time.Time value is simple: `time.Unix(t/1000, (ts%1000)*1000*1000))`
// but does cost extra instructions when we do this for EVERY datapoint only to turn around
// and turn it back into a int64 for prometheus query engine. Plus, dividing is an expensive
// instruction.
type Timestamp int64
