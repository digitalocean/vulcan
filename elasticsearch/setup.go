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

package elasticsearch

import (
	"fmt"
	"net/http"
	"strings"
)

const matchAll = `{
  "template": "*",
  "order": 0,
  "settings": {
    "index": {
      "refresh_interval": "5s"
    }
  },
  "mappings": {
    "_default_": {
      "dynamic_templates": [
        {
          "string_fields": {
            "mapping": {
              "fields": {
                "raw": {
                  "ignore_above": 256,
                  "doc_values": true,
                  "index": "not_analyzed",
                  "type": "string"
                }
              },
              "omit_norms": true,
              "index": "analyzed",
              "type": "string"
            },
            "match_mapping_type": "string",
            "match": "*"
          }
        }
      ],
      "_all": {
        "omit_norms": true,
        "enabled": true
      }
    }
  }
}`

// SetupMatchTemplate sets up elastic search templates for the type of text
// query we need to run
func SetupMatchTemplate(base string) error {
	url := fmt.Sprintf("%s/_template/match_all_0", base)
	_, err := http.Post(url, "text/json", strings.NewReader(matchAll))
	if err != nil {
		return err
	}
	return nil
}
