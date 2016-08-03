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

func SetupMatchTemplate(base string) error {
	url := fmt.Sprintf("%s/_template/match_all_0", base)
	_, err := http.Post(url, "text/json", strings.NewReader(matchAll))
	if err != nil {
		return err
	}
	return nil
}
