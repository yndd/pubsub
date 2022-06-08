package publisher

import (
	"context"

	"github.com/yndd/pubsub"
)

type Publisher interface {
	Publish(ctx context.Context, m *pubsub.Msg) error
	PublishFromCh(ctx context.Context, ch chan *pubsub.Msg)
}
