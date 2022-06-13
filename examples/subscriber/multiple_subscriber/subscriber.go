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

	defer subsc.Stop(name, subject)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		go func(i int) {
			for m := range subsc.Subscribe(ctx, fmt.Sprintf("%s-%d", name, i), subject) {
				fmt.Printf("%s-%d: %s: %+v\n", name, i, time.Now(), m)
			}
		}(i)
	}
	<-ctx.Done()
}
