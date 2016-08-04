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
