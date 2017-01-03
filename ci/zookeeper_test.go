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
	zookeeperImage = "zookeeper:3.4.9"
)

type zookeeper struct {
	Name string
}

func newZookeeper() *zookeeper {
	return &zookeeper{
		Name: fmt.Sprintf("zk_%s", id()),
	}
}

type zookeeperStartConfig struct {
	Network string
}

func (zk *zookeeper) start(ctx context.Context, cli *client.Client, cfg *zookeeperStartConfig) error {
	err := ensureImage(ctx, cli, zookeeperImage)
	if err != nil {
		return err
	}
	cc, err := cli.ContainerCreate(ctx, &container.Config{
		Image: zookeeperImage,
	}, &container.HostConfig{
		AutoRemove:  true,
		NetworkMode: container.NetworkMode(cfg.Network),
	}, &network.NetworkingConfig{}, zk.Name)
	if err != nil {
		return err
	}
	err = cli.ContainerStart(ctx, cc.ID, types.ContainerStartOptions{})
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("%s:2181", zk.Name)
	return wait(ctx, cli, cfg.Network, addr)
}
