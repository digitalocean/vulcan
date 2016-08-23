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

/*
Package bus defines interfaces for writing to and reading from the vulcan metric
bus and the datastructures that get passed. Different packages (e.g. kafka)
should actually implement the bus interfaces.
*/
package bus

// TODO scraper.Writer interface should live in bus package
// TODO Metrics and Samples are a concept that exist in all layers of vulcan,
// not just the bus. These structs should likely be in a more fundamental
// package.
