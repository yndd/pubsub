package publisher

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/yndd/ndd-runtime/pkg/logging"
	"github.com/yndd/pubsub"
	"google.golang.org/protobuf/proto"
)

const (
	reconnectDelay = time.Second
)

type natsPublisher struct {
	Config
	logger logging.Logger
}

type Config struct {
	Address             string
	StreamName          string
	Subjects            []string
	Insecure            bool
	CertificateSecret   string
	CaCertificateSecret string
}

func NewNATSPublisher(c Config, l logging.Logger) Publisher {
	if l == nil {
		l = logging.NewNopLogger()
	}
	return &natsPublisher{
		Config: c,
		logger: l,
	}
}

// Publish creates a nats connection and stream context then publishes the msg
func (p *natsPublisher) Publish(ctx context.Context, m *pubsub.Msg) error {
	nc, jsc, err := p.dialConn(ctx)
	if err != nil {
		return err
	}
	defer nc.Close()
	return p.publish(jsc, m)
}

// PublishFromCh reads messages from channel ch and publishes the received messages,
// until ctx is Done or the channel is closed.
func (p *natsPublisher) PublishFromCh(ctx context.Context, ch chan *pubsub.Msg) {
	p.publishWorker(ctx, ch)
}

// publishWorker reads messages from channel ch and publishes the received messages,
// until ctx is Done or the channel is closed.
func (p *natsPublisher) publishWorker(ctx context.Context, ch chan *pubsub.Msg) {
START:
	select {
	case <-ctx.Done():
		return
	default:
		nc, jsc, err := p.dialConn(ctx)
		if err != nil {
			return
		}
		defer nc.Close()
		for {
			select {
			case <-ctx.Done():
				p.logger.Debug("nats publisher stopped", "error", ctx.Err())
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				err = p.publish(jsc, m)
				if err != nil {
					p.logger.Info("JetStream Publish error", "error", err)
					nc.Close()
					goto START
				}
			}
		}
	}
}

// createStream creates a stream if it does not exist using JetStreamContext
func createStream(js nats.JetStreamContext, str *nats.StreamConfig) error {
	// Check if the stream already exists; if not, create it.
	stream, err := js.StreamInfo(str.Name)
	if err != nil {
		// ignore Notfound error and continue
		if !errors.Is(err, nats.ErrStreamNotFound) {
			return err
		}
	}
	// stream does not exist, create it
	if stream == nil {
		_, err = js.AddStream(str)
		if err != nil {
			return err
		}
	}
	return nil
}

// publish marshals Msg to proto format and publishes it to m.Subject
func (p *natsPublisher) publish(jsc nats.JetStreamContext, m *pubsub.Msg) error {
	p.logger.Debug("publish", "msg", m)
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	_, err = jsc.Publish(m.Subject, b)
	if err != nil {
		return err
	}
	return nil
}

// dialConn will (re)try create a NATS connection and JetStream context until it succeeds or ctx is Done
func (p *natsPublisher) dialConn(ctx context.Context) (*nats.Conn, nats.JetStreamContext, error) {
	opts := make([]nats.Option, 0)
	opts = append(opts,
		nats.ReconnectWait(reconnectDelay),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			p.logger.Info("NATS", "subscription", sub.Subject, "error", err)
		}),
		nats.DisconnectHandler(func(_ *nats.Conn) {
			p.logger.Info("Disconnected from NATS")
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			p.logger.Info("NATS connection is closed")
		}),
	)
STARTCONN:
	select {
	case <-ctx.Done():
		p.logger.Info("context done", "error", ctx.Err())
		return nil, nil, ctx.Err()
	default:
		nc, err := nats.Connect(p.Address, opts...)
		if err != nil {
			p.logger.Info("nats connection failed", "error", ctx.Err())
			time.Sleep(time.Second)
			goto STARTCONN
		}
		jsc, err := nc.JetStream()
		if err != nil {
			p.logger.Info("inconsistent JetStream Options", "error", err)
			nc.Close()
			time.Sleep(time.Second)
			goto STARTCONN
		}
		err = createStream(jsc,
			&nats.StreamConfig{
				Name:     p.StreamName,
				Subjects: p.Subjects,
			})
		if err != nil {
			p.logger.Info("failed creating stream", "error", err)
			nc.Close()
			time.Sleep(time.Second)
			goto STARTCONN
		}
		return nc, jsc, nil
	}
}
