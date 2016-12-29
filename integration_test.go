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
	"archive/tar"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

const (
	netcatImage              = "gophernet/netcat"
	networkName              = "ci_vulcan"
	waitForDependenciesImage = "dadarek/wait-for-dependencies:0.1"
	vulcanImage              = "ci_vulcan"
	zookeeperImage           = "zookeeper:3.4.9"
)

func setup(ctx context.Context, c *client.Client) error {
	err := buildVulcan(ctx, c)
	if err != nil {
		return err
	}
	err = runVulcanForwarder(ctx, c)
	if err != nil {
		return err
	}
	return nil
}

func wait(ctx context.Context, c *client.Client, addr string) (int64, error) {
	rc, err := c.ImagePull(ctx, waitForDependenciesImage, types.ImagePullOptions{})
	if err != nil {
		return -1, err
	}
	_, err = ioutil.ReadAll(rc)
	if err != nil {
		return -1, err
	}
	cc, err := c.ContainerCreate(ctx, &container.Config{
		Image: waitForDependenciesImage,
		Cmd: strslice.StrSlice{
			addr,
		},
	}, &container.HostConfig{
		AutoRemove:  true,
		NetworkMode: networkName,
	}, &network.NetworkingConfig{}, "")
	if err != nil {
		return -1, err
	}
	err = c.ContainerStart(ctx, cc.ID, types.ContainerStartOptions{})
	if err != nil {
		return -1, err
	}
	tctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*5))
	defer cancel()
	return c.ContainerWait(tctx, cc.ID)
}

func setupZookeeper(ctx context.Context, c *client.Client) error {
	// ensure image is pulled and available before trying to create.
	rc, err := c.ImagePull(ctx, zookeeperImage, types.ImagePullOptions{})
	if err != nil {
		return err
	}
	_, err = io.Copy(os.Stderr, rc)
	if err != nil {
		return err
	}
	cc, err := c.ContainerCreate(ctx, &container.Config{
		Image: zookeeperImage,
	}, &container.HostConfig{
		AutoRemove:  true,
		NetworkMode: networkName,
		// PortBindings: nat.PortMap{
		// 	"2181/tcp": []nat.PortBinding{
		// 		{
		// 			HostIP:   "0.0.0.0",
		// 			HostPort: "0/tcp",
		// 		},
		// 	},
		// },
	}, &network.NetworkingConfig{}, "zk")
	if err != nil {
		return err
	}
	err = c.ContainerStart(ctx, cc.ID, types.ContainerStartOptions{})
	if err != nil {
		return err
	}
	code, err := wait(ctx, c, "zk:2181")
	if err != nil {
		return err
	}
	spew.Dump(code)
	return nil
}

func setupKafka(ctx context.Context, c *client.Client) error {
	return nil
}

func buildVulcan(ctx context.Context, c *client.Client) error {
	// prepare build context (a tar file)
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)
	files := []struct {
		Name string
		Mode int64
	}{
		{
			Name: "Dockerfile",
			Mode: 0600,
		},
		{
			Name: "target/vulcan_linux_amd64",
			Mode: 0755,
		},
	}
	for _, file := range files {
		f, err := os.Open(file.Name)
		if err != nil {
			return err
		}
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		hdr := &tar.Header{
			Name: file.Name,
			Mode: file.Mode,
			Size: fi.Size(),
		}
		err = tw.WriteHeader(hdr)
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, f)
		if err != nil {
			return err
		}
	}
	buildContext := bytes.NewReader(buf.Bytes())
	resp, err := c.ImageBuild(ctx, buildContext, types.ImageBuildOptions{
		Tags: []string{vulcanImage},
	})
	if err != nil {
		return err
	}
	_, err = io.Copy(os.Stderr, resp.Body)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func runVulcanForwarder(ctx context.Context, c *client.Client) error {
	cc, err := c.ContainerCreate(ctx, &container.Config{
		Image: vulcanImage,
		Cmd: strslice.StrSlice{
			"forwarder",
		},
	}, &container.HostConfig{
		AutoRemove:  true,
		NetworkMode: networkName,
		PortBindings: nat.PortMap{
			"8080/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "0/tcp",
				},
			},
		},
	}, &network.NetworkingConfig{}, "forwarder")
	if err != nil {
		return err
	}
	err = c.ContainerStart(ctx, cc.ID, types.ContainerStartOptions{})
	if err != nil {
		return err
	}
	// code, err := wait(ctx, c, "forwarder:8080")
	// if err != nil {
	// return err
	// }
	// spew.Dump(code)
	return nil
}

func teardown() error {
	return nil
}

func TestPackage(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewEnvClient()
	if err != nil {
		t.Fatal(err)
	}
	err = setup(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	containers, err := c.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	spew.Dump(containers)

	// j, _, err := cli.ContainerInspectWithRaw(ctx, body.ID, false)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// spew.Dump(j)
	// spew.Dump(body.ID)
	// var port string
	// for p, bindings := range j.NetworkSettings.NetworkSettingsBase.Ports {
	// 	if p != "2181/tcp" {
	// 		continue
	// 	}
	// 	for _, binding := range bindings {
	// 		if binding.HostIP != "0.0.0.0" {
	// 			continue
	// 		}
	// 		port = binding.HostPort
	// 	}
	// }
	// addr := fmt.Sprintf("127.0.0.1:%s", port)
	// _, err = net.Dial("tcp", addr)
	// if err != nil {
	// 	t.Fatal(err)
	// }
}
