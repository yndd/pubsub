package main

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/yndd/ndd-runtime/pkg/logging"
	"github.com/yndd/pubsub"
	"github.com/yndd/pubsub/publisher"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	l := zap.New()

	pub := publisher.NewNATSPublisher(publisher.Config{
		Address:    nats.DefaultURL,
		StreamName: "state",
		Subjects:   []string{"state.>"},
		Insecure:   true,
	}, logging.NewLogrLogger(l))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := pub.Publish(ctx, &pubsub.Msg{
		Subject:   "state.target1.interface",
		Timestamp: time.Now().UnixNano(),
		Operation: pubsub.Operation_OPERATION_UPDATE,
		Data:      []byte{},
	})
	if err != nil {
		panic(err)
	}
}
