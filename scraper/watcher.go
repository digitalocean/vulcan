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

package scraper

// TargetWatcher is an interface that represents something that can return
// a slice of active Targeters
type TargetWatcher interface {
	Targets() <-chan []Targeter
}

// JobWatcher is an interface that wraps the Target method.
type JobWatcher interface {
	Jobs() <-chan []Job
}
