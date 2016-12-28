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
