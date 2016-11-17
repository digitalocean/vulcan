package main

import (
	"context"

	"github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	"github.com/digitalocean/vulcan/indexer"

	"google.golang.org/grpc"
)

const (
	address = "localhost:8080"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		logrus.WithError(err).Error("while connecting")
	}
	defer conn.Close()
	c := indexer.NewResolverClient(conn)
	req := &indexer.ResolveRequest{
		Matchers: []*indexer.Matcher{
			{
				Type:  indexer.MatcherType_Equal,
				Name:  "__name__",
				Value: "node_load1",
			},
		},
	}
	r, err := c.Resolve(context.Background(), req)
	if err != nil {
		logrus.WithError(err).Error("while resolving")
	}
	spew.Dump(r)

}
