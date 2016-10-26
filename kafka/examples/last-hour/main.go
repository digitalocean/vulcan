package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/digitalocean/vulcan/kafka"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-term
		cancel()
		<-term // if we receive a second signal, let's exit hard.
		os.Exit(1)
	}()
	ws, err := kafka.NewWindowedSource(&kafka.WindowedSourceConfig{
		Addrs:    []string{"localhost"},
		ClientID: "last-hour",
		Ctx:      ctx,
		GroupID:  "last-hour",
		Target:   time.Now().Add(-time.Hour),
		Topics:   []string{"vulcan"},
	})
	if err != nil {
		panic(err)
	}
	for m := range ws.Messages() {
		m.Ack()
	}
}
