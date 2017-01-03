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
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/client"
)

const (
	vulcanImagePrefix = "ci_vulcan"
)

type vulcan struct {
	Image string
	Name  string
}

func newVulcan(ctx context.Context, cli *client.Client) (*vulcan, error) {
	// prepare build context (a tar file)
	image := fmt.Sprintf("%s_%s", vulcanImagePrefix, id())
	name := fmt.Sprintf("vulcan_%s", id())
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)
	files := []struct {
		Source string
		Name   string
		Mode   int64
	}{
		{
			Source: "ci/docker/vulcan/Dockerfile",
			Name:   "Dockerfile",
			Mode:   0600,
		},
		{
			Source: "target/vulcan_linux_amd64",
			Name:   "target/vulcan_linux_amd64",
			Mode:   0755,
		},
	}
	for _, file := range files {
		f, err := os.Open(file.Source)
		if err != nil {
			return nil, err
		}
		fi, err := f.Stat()
		if err != nil {
			return nil, err
		}
		hdr := &tar.Header{
			Name: file.Name,
			Mode: file.Mode,
			Size: fi.Size(),
		}
		err = tw.WriteHeader(hdr)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(tw, f)
		if err != nil {
			return nil, err
		}
	}
	buildContext := bytes.NewReader(buf.Bytes())
	resp, err := cli.ImageBuild(ctx, buildContext, types.ImageBuildOptions{
		Tags: []string{image},
	}
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(os.Stderr, resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return &vulcan{
		Image: image,
		Name:  name,
	}, nil
}

type vulcanStartConfig struct {
	Args    []string
	Env     []string
	Network string
}

func (v *vulcan) start(ctx context.Context, cli *client.Client, cfg *vulcanStartConfig) error {
	err := ensureImage(ctx, cli, v.Image)
	if err != nil {
		return err
	}
	cc, err := cli.ContainerCreate(ctx, &container.Config{
		Image: kafkaImage,
		Env:   cfg.Env,
		Cmd:   strslice.StrSlice(cfg.Args),
	}, &container.HostConfig{
		AutoRemove:  true,
		NetworkMode: container.NetworkMode(cfg.Network),
	}, &network.NetworkingConfig{}, v.Name)
	if err != nil {
		return err
	}
	return cli.ContainerStart(ctx, cc.ID, types.ContainerStartOptions{})
}
