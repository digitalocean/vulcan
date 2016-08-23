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

package convert

import "strings"

// ESEscape escapes out "." characters which are illegal in fieldnames
// in es 2.x
// This turns existing "_" characters into double "__" characters, and then
// turns "." characters into "_" characters
func ESEscape(field string) string {
	field = strings.Replace(field, "_", "__", -1)
	field = strings.Replace(field, ".", "_", -1)
	return field
}
