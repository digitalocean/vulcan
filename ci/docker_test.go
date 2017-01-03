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
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/client"
	"github.com/oklog/ulid"
)

const (
	waitForDependenciesImage = "dadarek/wait-for-dependencies:0.1"
)

func id() string {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("%s", ulid.MustNew(ulid.Timestamp(time.Now()), rnd))
}

func createNetwork(ctx context.Context, cli *client.Client, prefix string) (string, error) {
	name := fmt.Sprintf("%s_%s", prefix, id())
	_, err := cli.NetworkCreate(ctx, name, types.NetworkCreate{})
	if err != nil {
		return "", err
	}
	return name, nil
}

func ensureImage(ctx context.Context, cli *client.Client, image string) error {
	rc, err := cli.ImagePull(ctx, image, types.ImagePullOptions{})
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = ioutil.ReadAll(rc)
	if err != nil {
		return err
	}
	return nil
}

// wait will poll the target addr on the provided network until it responds or timeout occurs and an error returns.
func wait(ctx context.Context, c *client.Client, nwork, addr string) error {
	rc, err := c.ImagePull(ctx, waitForDependenciesImage, types.ImagePullOptions{})
	if err != nil {
		return err
	}
	_, err = ioutil.ReadAll(rc)
	if err != nil {
		return err
	}
	cc, err := c.ContainerCreate(ctx, &container.Config{
		Image: waitForDependenciesImage,
		Cmd: strslice.StrSlice{
			addr,
		},
	}, &container.HostConfig{
		AutoRemove:  true,
		NetworkMode: container.NetworkMode(nwork),
	}, &network.NetworkingConfig{}, "")
	if err != nil {
		return err
	}
	err = c.ContainerStart(ctx, cc.ID, types.ContainerStartOptions{})
	if err != nil {
		return err
	}
	tctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*50))
	defer cancel()
	code, err := c.ContainerWait(tctx, cc.ID)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("expected 0 error code from wait cmd but got %d", code)
	}
	return nil
}
