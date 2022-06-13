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
	flag.StringVar(&name, "name", "subscriber_all", "subscriber name")
	flag.StringVar(&subject, "subject", "state.>", "subject")
	flag.Parse()

	l := zap.New()
	subsc := subscriber.NewNATSSubscriber(subscriber.Config{
		Address:  nats.DefaultURL,
		Insecure: true,
	}, logging.NewLogrLogger(l))

	defer subsc.Stop(name, subject)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for m := range subsc.SubscribeAll(ctx, name, subject) {
		fmt.Printf("%s: %+v\n", time.Now(), m)
	}
}
