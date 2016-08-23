package zookeeper

import (
	pconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/retrieval/discovery"
)

func k8discovery(config *pconfig.KubernetesSDConfig) (retrieval.TargetProvider, error) {
	return discovery.NewKubernetesDiscovery(config)
}
