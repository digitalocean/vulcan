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

package model

import "fmt"

// Errors represents a slice of errors.
// It implements the error interface and is meant to collect errors for
// reporting reason.  It should not be use in a scenario where a single
// error needs to trigger a failure.
type Errors []error

func (e Errors) Error() string {
	if len(e) == 1 {
		return e.Error()
	}

	msg := "multiple errors occured:"
	for _, err := range e {
		msg += fmt.Sprintf("\n%s", err.Error())
	}

	return msg
}

// Occurred checks if at least one 1 has occurred.
func (e Errors) Occurred() bool {
	return len(e) > 0
}
