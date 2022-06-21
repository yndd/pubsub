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

var subject string

func main() {
	flag.StringVar(&subject, "subject", "state.>", "subject")
	flag.Parse()

	l := zap.New()
	subsc := subscriber.NewNATSSubscriber(subscriber.Config{
		Address:  nats.DefaultURL,
		Insecure: true,
	}, logging.NewLogrLogger(l))

	defer subsc.Stop("", "") // stop all

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msgs, err := subsc.Get(ctx, subject)
	if err != nil {
		panic(err)
	}
	for _, m := range msgs {
		fmt.Printf("%s: %+v\n", time.Now(), m)
	}
}
