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

package cacher

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// chunksResp is returned over HTTP and JSON encoded.
type chunksResp struct {
	Chunks [][]byte `json:"chunks"`
}

// getChunks attempts to read an URL for a chunksResp.
func getChunks(u *url.URL) (*chunksResp, error) {
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("received status %s", resp.Status)
	}
	dec := json.NewDecoder(resp.Body)
	var cr chunksResp
	err = dec.Decode(&cr)
	return &cr, err
}
