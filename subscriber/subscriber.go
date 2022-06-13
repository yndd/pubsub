package subscriber

import (
	"context"
	"time"

	"github.com/yndd/pubsub"
)

type Subscriber interface {
	Subscribe(ctx context.Context, name, subject string) chan *pubsub.Msg
	SubscribeCh(ctx context.Context, name, subject string, ch chan *pubsub.Msg)
	QueueSubscribe(ctx context.Context, name, queue, subject string) chan *pubsub.Msg
	QueueSubscribeCh(ctx context.Context, name, queue, subject string, ch chan *pubsub.Msg)
	//
	SubscribeAll(ctx context.Context, name, subject string) chan *pubsub.Msg
	SubscribeAllCh(ctx context.Context, name, subject string, ch chan *pubsub.Msg)
	QueueSubscribeAll(ctx context.Context, name, queue, subject string) chan *pubsub.Msg
	QueueSubscribeAllCh(ctx context.Context, name, queue, subject string, ch chan *pubsub.Msg)
	//
	SubscribeLast(ctx context.Context, name, subject string) chan *pubsub.Msg
	SubscribeLastCh(ctx context.Context, name, subject string, ch chan *pubsub.Msg)
	QueueSubscribeLast(ctx context.Context, name, queue, subject string) chan *pubsub.Msg
	QueueSubscribeLastCh(ctx context.Context, name, queue, subject string, ch chan *pubsub.Msg)
	//
	SubscribeLastPerSubject(ctx context.Context, name, subject string) chan *pubsub.Msg
	SubscribeLastPerSubjectCh(ctx context.Context, name, subject string, ch chan *pubsub.Msg)
	QueueSubscribeLastPerSubject(ctx context.Context, name, queue, subject string) chan *pubsub.Msg
	QueueSubscribeLastPerSubjectCh(ctx context.Context, name, queue, subject string, ch chan *pubsub.Msg)
	//
	SubscribeSeq(ctx context.Context, name, subject string, seq uint64) chan *pubsub.Msg
	SubscribeSeqCh(ctx context.Context, name, subject string, seq uint64, ch chan *pubsub.Msg)
	QueueSubscribeSeq(ctx context.Context, name, queue, subject string, seq uint64) chan *pubsub.Msg
	QueueSubscribeSeqCh(ctx context.Context, name, queue, subject string, seq uint64, ch chan *pubsub.Msg)
	//
	SubscribeSince(ctx context.Context, name, subject string, ts time.Time) chan *pubsub.Msg
	SubscribeSinceCh(ctx context.Context, name, subject string, ts time.Time, ch chan *pubsub.Msg)
	QueueSubscribeSince(ctx context.Context, name, queue, subject string, ts time.Time) chan *pubsub.Msg
	QueueSubscribeSinceCh(ctx context.Context, name, queue, subject string, ts time.Time, ch chan *pubsub.Msg)
	//
	Stop(name, subject string)
}
