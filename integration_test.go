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

// +build integration

package main_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

func setup() error {
	return nil
}

func teardown() error {
	return nil
}

func TestPackage(t *testing.T) {
	cli, err := client.NewEnvClient()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	containers, err := cli.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	spew.Dump(containers)
	rc, err := cli.ImagePull(ctx, "zookeeper:3.4.9", types.ImagePullOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	body, err := cli.ContainerCreate(ctx, &container.Config{
		ExposedPorts: nat.PortSet{
			"2181/tcp": struct{}{},
		},
		Image: "zookeeper:3.4.9",
	}, &container.HostConfig{
		AutoRemove:  true,
		NetworkMode: "ci_vulcan",
		PortBindings: nat.PortMap{
			"2181/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "0/tcp",
				},
			},
		},
	}, &network.NetworkingConfig{}, "zk")
	if err != nil {
		t.Fatal(err)
	}
	err = cli.ContainerStart(ctx, body.ID, types.ContainerStartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	j, _, err := cli.ContainerInspectWithRaw(ctx, body.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	spew.Dump(j)
	spew.Dump(body.ID)
	var port string
	for p, bindings := range j.NetworkSettings.NetworkSettingsBase.Ports {
		if p != "2181/tcp" {
			continue
		}
		for _, binding := range bindings {
			if binding.HostIP != "0.0.0.0" {
				continue
			}
			port = binding.HostPort
		}
	}
	addr := fmt.Sprintf("127.0.0.1:%s", port)
	_, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
}
