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
var seq uint64

func main() {
	flag.StringVar(&name, "name", "subscriber_seq", "subscriber name")
	flag.StringVar(&subject, "subject", "state.>", "subject")
	flag.Uint64Var(&seq, "seq", 42, "sequence number")
	flag.Parse()

	l := zap.New()
	subsc := subscriber.NewNATSSubscriber(subscriber.Config{
		Address:  nats.DefaultURL,
		Insecure: true,
	}, logging.NewLogrLogger(l))

	defer subsc.Stop(name, subject)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for m := range subsc.SubscribeSeq(ctx, name, subject, seq) {
		fmt.Printf("%s: %+v\n", time.Now(), m)
	}
}
