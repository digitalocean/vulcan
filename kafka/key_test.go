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

package kafka_test

import (
	"testing"

	"github.com/digitalocean/vulcan/kafka"
)

func TestKey(t *testing.T) {
	tests := []struct {
		Job      string
		Instance string
		Result   []byte
	}{
		{
			Job:      "www",
			Instance: "node01.example.com:8080",
			Result:   []byte("www-node01.example.com:8080"),
		},
		{
			Job:      "⌘",
			Instance: "世界",
			Result:   []byte("⌘-世界"),
		},
	}
	for _, test := range tests {
		key := kafka.Key(kafka.Job(test.Job), kafka.Instance(test.Instance))
		if string(key) != string(test.Result) {
			t.Errorf("unexpected result got=%s want=%s", key, test.Result)
		}
	}
}
