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

package ci_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/client"
)

type setupResult struct {
	Network string
}

func setup(cli *client.Client) (*setupResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	network, err := createNetwork(ctx, cli, "ci_vulcan")
	if err != nil {
		return nil, err
	}
}

func TestKafka(t *testing.T) {
	ctx := context.Background()
	cli, err := client.NewEnvClient()
	if err != nil {
		t.Fatal(err)
	}

	log.Println("building vulcan container")
	v, err := newVulcan(ctx, cli)
	if err != nil {
		t.Error(err)
	}
	kafka1 := newKafka()
	var oerr error
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("starting zookeeper")
		zk1 := newZookeeper()
		err = zk1.start(ctx, cli, &zookeeperStartConfig{
			Network: network,
		})
		if err != nil {
			oerr = err
			return
		}
		log.Println("starting kafka")
		err = kafka1.start(ctx, cli, &kafkaStartConfig{
			Network:   network,
			Zookeeper: fmt.Sprintf("%s:2181", zk1.Name),
		})
		if err != nil {
			oerr = err
			return
		}

	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("starting cassandra")
		c1 := newCassandra()
		err = c1.start(ctx, cli, &cassandraStartConfig{
			Network: network,
		})
		if err != nil {
			oerr = err
			return
		}
	}()
	go func() {
		defer wg.Done()
		log.Println("starting forwarder")
		err := v.start(ctx, cli, &vulcanStartConfig{
			Args: []string{"forwarder"},
			Env: []string{
				fmt.Sprintf("VULCAN_KAFKA_ADDRS=%s:9042", kafka1.Name),
				"VULCAN_KAFKA_TOPIC=vulcan-ci",
			},
		})
		if err != nil {
			oerr = err
			return
		}
	}()
	wg.Wait()
	if oerr != nil {
		t.Fatal(err)
	}
}
