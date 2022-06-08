package main

import (
	"context"
	"encoding/binary"
	"flag"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/yndd/ndd-runtime/pkg/logging"
	"github.com/yndd/pubsub"
	"github.com/yndd/pubsub/publisher"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var stream string
var subject string

func main() {
	flag.StringVar(&stream, "stream", "state", "stream name")
	flag.StringVar(&subject, "subject", "state.target1.interface", "subject")
	flag.Parse()
	l := zap.New()

	pub := publisher.NewNATSPublisher(publisher.Config{
		Address:    nats.DefaultURL,
		StreamName: stream,
		Subjects:   []string{stream + ".>"},
		Insecure:   true,
	}, logging.NewLogrLogger(l))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *pubsub.Msg)

	go pub.PublishFromCh(ctx, ch)

	ticker := time.NewTicker(time.Second)
	i := uint64(0)
	for {
		select {
		case <-ticker.C:
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, i)
			i++
			ch <- &pubsub.Msg{
				Subject:   subject,
				Timestamp: time.Now().UnixNano(),
				Operation: pubsub.Operation_OPERATION_UPDATE,
				Data:      b,
			}
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}
