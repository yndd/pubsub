package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/yndd/ndd-runtime/pkg/logging"
	"github.com/yndd/pubsub/subscriber"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var name string
var subject string

func main() {
	flag.StringVar(&name, "name", "multi_subscriber", "subscriber name")
	flag.StringVar(&subject, "subject", "state.>", "subject")

	flag.Parse()

	l := zap.New()
	subsc := subscriber.NewNATSSubscriber(subscriber.Config{
		Address:  nats.DefaultURL,
		Insecure: true,
	}, logging.NewLogrLogger(l))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer subsc.Stop()

	go func() {
		for m := range subsc.Subscribe(ctx, name+"0", subject) {
			fmt.Printf("0: %s: %+v\n", time.Now(), m)
		}
	}()

	go func() {
		for m := range subsc.Subscribe(ctx, name+"1", subject) {
			fmt.Printf("1: %s: %+v\n", time.Now(), m)
		}
	}()
	<-ctx.Done()
}
