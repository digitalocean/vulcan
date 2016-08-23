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

package convert_test

import (
	"bytes"
	"testing"

	"github.com/digitalocean/vulcan/convert"
)

func TestPromTextToSG(t *testing.T) {
	sg, err := convert.PromTextToSG(bytes.NewReader([]byte(`# HELP node_network_receive_packets Network device statistic receive_packets.
# TYPE node_network_receive_packets gauge
node_network_receive_packets{device="eth0",job="docker-compose-node-exporter",instance="node_exporter:9100"} 826 1464748563529
node_network_receive_packets{device="lo",job="docker-compose-node-exporter",instance="node_exporter:9100"} 0 1464748563529
# HELP go_memstats_alloc_bytes_total Total number of bytes allocated, even if freed.
# TYPE go_memstats_alloc_bytes_total counter
go_memstats_alloc_bytes_total{job="docker-compose-node-exporter",instance="node_exporter:9100"} 3.90805888e+08 1464748563529
# HELP node_memory_SReclaimable Memory information field SReclaimable.
# TYPE node_memory_SReclaimable gauge
node_memory_SReclaimable{job="docker-compose-node-exporter",instance="node_exporter:9100"} 4.2274816e+07 1464748563529
`)))
	if err != nil {
		t.Error(err)
	}
	if len(sg) != 4 {
		t.Errorf("expected 4 samples but got %d", len(sg))
	}
}
