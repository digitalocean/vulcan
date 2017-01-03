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

package ci_test

import (
	"context"
	"fmt"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
)

const (
	kafkaImage = "ches/kafka:0.10.1.0"
)

type kafkaCreateTopicConfig struct {
	Network           string
	Topic             string
	ReplicationFactor int
	Partitions        int
	Zookeeper         string
}

func kafkaCreateTopic(ctx context.Context, cli *client.Client, cfg *kafkaCreateTopicConfig) error {
	err := ensureImage(ctx, cli, kafkaImage)
	if err != nil {
		return err
	}
	return nil
}

type kafka struct {
	Name string
}

func newKafka() *kafka {
	return &kafka{
		Name: fmt.Sprintf("kafka_%s", id()),
	}
}

type kafkaStartConfig struct {
	Network   string
	Zookeeper string
}

func (k *kafka) start(ctx context.Context, cli *client.Client, cfg *kafkaStartConfig) error {
	err := ensureImage(ctx, cli, kafkaImage)
	if err != nil {
		return err
	}
	cc, err := cli.ContainerCreate(ctx, &container.Config{
		Image: kafkaImage,
		Env: []string{
			fmt.Sprintf("ZOOKEEPER_CONNECTION_STRING=%s", cfg.Zookeeper),
		},
	}, &container.HostConfig{
		AutoRemove:  true,
		NetworkMode: container.NetworkMode(cfg.Network),
	}, &network.NetworkingConfig{}, k.Name)
	if err != nil {
		return err
	}
	err = cli.ContainerStart(ctx, cc.ID, types.ContainerStartOptions{})
	if err != nil {
		return err
	}
	return wait(ctx, cli, cfg.Network, fmt.Sprintf("%s:9092", k.Name))
}
