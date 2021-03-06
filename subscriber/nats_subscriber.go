package subscriber

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	jsm "github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/yndd/ndd-runtime/pkg/logging"
	"github.com/yndd/pubsub"
	"google.golang.org/protobuf/proto"
)

const (
	reconnectDelay     = time.Second
	defaultBufferSize  = 1024
	fetchBatchSize     = 100
	defaultPullMaxWait = 100 * time.Millisecond
)

type natsSubscriber struct {
	Config
	logger logging.Logger

	m           *sync.RWMutex
	nc          *nats.Conn
	subscribers map[string]map[string]context.CancelFunc
}

type Config struct {
	// Consumer durable name
	Name string
	// NATS address
	Address string
	// consumer buffer size
	BufferSize uint64
	// NATS insecure connection
	Insecure bool
	// Pull max wait time
	PullMaxWait time.Duration
	//
	CertificateSecret   string
	CaCertificateSecret string
}

func NewNATSSubscriber(c Config, l logging.Logger) Subscriber {
	if l == nil {
		l = logging.NewNopLogger()
	}
	if c.BufferSize == 0 {
		c.BufferSize = defaultBufferSize
	}
	if c.PullMaxWait <= 0 {
		c.PullMaxWait = defaultPullMaxWait
	}
	return &natsSubscriber{
		Config:      c,
		logger:      l,
		m:           &sync.RWMutex{},
		subscribers: make(map[string]map[string]context.CancelFunc),
	}
}

// regular subscribe(s)
func (s *natsSubscriber) Subscribe(ctx context.Context, name, subject string) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.subscribeCh(ctx, name, subject, ch)
	return ch
}

func (s *natsSubscriber) SubscribeCh(ctx context.Context, name, subject string, ch chan *pubsub.Msg) {
	s.subscribeCh(ctx, name, subject, ch)
}

func (s *natsSubscriber) QueueSubscribe(ctx context.Context, name, queue, subject string) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.queueSubscribeCh(ctx, name, queue, subject, ch)
	return ch
}

func (s *natsSubscriber) QueueSubscribeCh(ctx context.Context, name, queue, subject string, ch chan *pubsub.Msg) {
	s.queueSubscribeCh(ctx, name, queue, subject, ch)
}

// get all messages in stream subscribe(s)
func (s *natsSubscriber) SubscribeAll(ctx context.Context, name, subject string) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.subscribeCh(ctx, name, subject, ch, nats.DeliverAll())
	return ch
}

func (s *natsSubscriber) SubscribeAllCh(ctx context.Context, name, subject string, ch chan *pubsub.Msg) {
	s.subscribeCh(ctx, name, subject, ch, nats.DeliverAll())
}

func (s *natsSubscriber) QueueSubscribeAll(ctx context.Context, name, queue, subject string) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.DeliverAll())
	return ch
}

func (s *natsSubscriber) QueueSubscribeAllCh(ctx context.Context, name, queue, subject string, ch chan *pubsub.Msg) {
	s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.DeliverAll())
}

func (s *natsSubscriber) SubscribeLast(ctx context.Context, name, subject string) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.subscribeCh(ctx, name, subject, ch, nats.DeliverLast())
	return ch
}

func (s *natsSubscriber) SubscribeLastCh(ctx context.Context, name, subject string, ch chan *pubsub.Msg) {
	s.subscribeCh(ctx, name, subject, ch, nats.DeliverLast())
}

func (s *natsSubscriber) QueueSubscribeLast(ctx context.Context, name, queue, subject string) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.DeliverLast())
	return ch
}

func (s *natsSubscriber) QueueSubscribeLastCh(ctx context.Context, name, queue, subject string, ch chan *pubsub.Msg) {
	s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.DeliverLast())
}

func (s *natsSubscriber) SubscribeLastPerSubject(ctx context.Context, name, subject string) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.subscribeCh(ctx, name, subject, ch, nats.DeliverLastPerSubject())
	return ch
}

func (s *natsSubscriber) SubscribeLastPerSubjectCh(ctx context.Context, name, subject string, ch chan *pubsub.Msg) {
	s.subscribeCh(ctx, name, subject, ch, nats.DeliverLastPerSubject())
}

func (s *natsSubscriber) QueueSubscribeLastPerSubject(ctx context.Context, name, queue, subject string) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.DeliverLastPerSubject())
	return ch
}

func (s *natsSubscriber) QueueSubscribeLastPerSubjectCh(ctx context.Context, name, queue, subject string, ch chan *pubsub.Msg) {
	s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.DeliverLastPerSubject())
}

func (s *natsSubscriber) SubscribeSeq(ctx context.Context, name, subject string, seq uint64) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.subscribeCh(ctx, name, subject, ch, nats.StartSequence(seq))
	return ch
}

func (s *natsSubscriber) SubscribeSeqCh(ctx context.Context, name, subject string, seq uint64, ch chan *pubsub.Msg) {
	s.subscribeCh(ctx, name, subject, ch, nats.StartSequence(seq))
}

func (s *natsSubscriber) QueueSubscribeSeq(ctx context.Context, name, queue, subject string, seq uint64) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.StartSequence(seq))
	return ch
}

func (s *natsSubscriber) QueueSubscribeSeqCh(ctx context.Context, name, queue, subject string, seq uint64, ch chan *pubsub.Msg) {
	s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.StartSequence(seq))
}

func (s *natsSubscriber) SubscribeSince(ctx context.Context, name, subject string, ts time.Time) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.subscribeCh(ctx, name, subject, ch, nats.StartTime(ts))
	return ch
}

func (s *natsSubscriber) SubscribeSinceCh(ctx context.Context, name, subject string, ts time.Time, ch chan *pubsub.Msg) {
	s.subscribeCh(ctx, name, subject, ch, nats.StartTime(ts))
}

func (s *natsSubscriber) QueueSubscribeSince(ctx context.Context, name, queue, subject string, ts time.Time) chan *pubsub.Msg {
	ch := make(chan *pubsub.Msg)
	go s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.StartTime(ts))
	return ch
}

func (s *natsSubscriber) QueueSubscribeSinceCh(ctx context.Context, name, queue, subject string, ts time.Time, ch chan *pubsub.Msg) {
	s.queueSubscribeCh(ctx, name, queue, subject, ch, nats.StartTime(ts))
}

// dialConn will (re)try create a NATS connection and JetStream context until it succeeds or ctx is Done
func (s *natsSubscriber) dialConn(ctx context.Context) (nats.JetStreamContext, error) {
	opts := make([]nats.Option, 0)
	opts = append(opts,
		nats.ReconnectWait(reconnectDelay),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			s.logger.Info("NATS", "error", err)
		}),
		nats.DisconnectHandler(func(c *nats.Conn) {
			s.logger.Info("Disconnected from NATS")
		}),
		nats.ClosedHandler(func(c *nats.Conn) {
			s.logger.Info("NATS connection is closed")
		}),
	)
	var err error
STARTCONN:
	select {
	case <-ctx.Done():
		s.logger.Info("context done", "error", ctx.Err())
		return nil, ctx.Err()
	default:
		s.m.Lock()
		if s.nc == nil || !s.nc.IsConnected() {
			s.nc, err = nats.Connect(s.Address, opts...)
			if err != nil {
				s.m.Unlock()
				s.logger.Info("nats connection failed", "error", err)
				time.Sleep(reconnectDelay)
				goto STARTCONN
			}
		}
		s.m.Unlock()
		jsc, err := s.nc.JetStream()
		if err != nil {
			s.logger.Info("inconsistent JetStream Options", "error", err)
			time.Sleep(reconnectDelay)
			goto STARTCONN
		}
		return jsc, nil
	}
}

// subscribe generic func
func (s *natsSubscriber) subscribeCh(ctx context.Context, name, subject string, ch chan *pubsub.Msg, opts ...nats.SubOpt) {
	if opts == nil {
		opts = make([]nats.SubOpt, 0)
	}
	if name != "" {
		opts = append(opts, nats.Durable(name))
	}
	logger := s.logger.WithValues("name", name, "subject", subject)

	ctx, cancel := context.WithCancel(ctx)
	s.m.Lock()
	if _, ok := s.subscribers[name]; !ok {
		s.subscribers[name] = make(map[string]context.CancelFunc)
	}
	s.subscribers[name][subject] = cancel
	s.m.Unlock()
START:
	select {
	case <-ctx.Done():
		return
	default:
		jsc, err := s.dialConn(ctx)
		if err != nil {
			logger.Info("nats dial connection failed", "error", err)
			return
		}
		natsCh := make(chan *nats.Msg, s.BufferSize)
		sub, err := jsc.ChanSubscribe(subject, natsCh, opts...)
		if err != nil {
			logger.Info("nats subscribe failed", "error", err)
			time.Sleep(reconnectDelay)
			goto START
		}
		defer sub.Unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case nm, ok := <-natsCh:
				if !ok {
					return
				}
				logger.Debug("rcvd msg", "msg", nm)

				err = nm.Ack(nats.Context(ctx))
				if err != nil {
					logger.Info("msg ack failed", "error", err)
					continue
				}
				logger.Debug("acked msg", "msg", nm)

				msgInfo, err := jsm.ParseJSMsgMetadata(nm)
				if err != nil {
					logger.Info("msg metadata parse failed", "error", err)
					continue
				}
				logger.Debug("rcvd msgInfo", "msgInfo", msgInfo)
				m := new(pubsub.Msg)
				err = proto.Unmarshal(nm.Data, m)
				if err != nil {
					logger.Info("proto unmarshal failed", "error", err)
					continue
				}
				m.Sequence = msgInfo.StreamSequence()
				ch <- m
			}
		}
	}
}

// queue subscribe generic func
func (s *natsSubscriber) queueSubscribeCh(ctx context.Context, name, queue string, subject string, ch chan *pubsub.Msg, opts ...nats.SubOpt) {
	if opts == nil {
		opts = make([]nats.SubOpt, 0)
	}
	if name != "" {
		opts = append(opts, nats.Durable(name))
	}
	logger := s.logger.WithValues("name", name, "queue", queue, "subject", subject)

	ctx, cancel := context.WithCancel(ctx)
	s.m.Lock()
	qname := fmt.Sprintf("%s/%s", queue, name)
	if _, ok := s.subscribers[name]; !ok {
		s.subscribers[qname] = make(map[string]context.CancelFunc)
	}
	s.subscribers[qname][subject] = cancel
	s.m.Unlock()
START:
	select {
	case <-ctx.Done():
		return
	default:
		jsc, err := s.dialConn(ctx)
		if err != nil {
			logger.Info("nats dial connection failed", "error", err)
			return
		}
		natsCh := make(chan *nats.Msg, s.BufferSize)
		sub, err := jsc.ChanQueueSubscribe(subject, queue, natsCh, opts...)
		if err != nil {
			logger.Info("nats subscribe failed", "error", err)
			time.Sleep(reconnectDelay)
			goto START
		}
		defer sub.Unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case nm, ok := <-natsCh:
				if !ok {
					return
				}
				logger.Debug("rcvd msg", "msg", nm)

				err = nm.Ack(nats.Context(ctx))
				if err != nil {
					logger.Info("msg ack failed", "error", err)
					continue
				}
				logger.Debug("acked msg", "msg", nm)

				msgInfo, err := jsm.ParseJSMsgMetadata(nm)
				if err != nil {
					logger.Info("msg metadata parse failed", "error", err)
					continue
				}
				logger.Debug("rcvd msgInfo", "msgInfo", msgInfo)
				m := new(pubsub.Msg)
				err = proto.Unmarshal(nm.Data, m)
				if err != nil {
					logger.Info("proto unmarshal failed", "error", err)
					continue
				}
				m.Sequence = msgInfo.StreamSequence()
				ch <- m
			}
		}
	}
}

func (s *natsSubscriber) Get(ctx context.Context, subject string) ([]*pubsub.Msg, error) {
	return s.pull(ctx, "", subject, nats.DeliverLastPerSubject())
}

func (s *natsSubscriber) pull(ctx context.Context, name, subject string, opts ...nats.SubOpt) ([]*pubsub.Msg, error) {
	logger := s.logger.WithValues("pull", "subject", subject)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		jsc, err := s.dialConn(ctx)
		if err != nil {
			return nil, fmt.Errorf("nats dial connection failed error: %v", err)
		}
		if name == "" {
			name = uuid.New().String()
		}
		logger.Info("creating pull subscription")

		sub, err := jsc.PullSubscribe(subject, name, opts...)
		if err != nil {
			return nil, err
		}

		deadline := time.Now() // get messages with timestamp before this deadline
		done := false          // set to true when we get a msg with a timestamp after the deadline

		rs := make([]*pubsub.Msg, 0, fetchBatchSize)
		for {
			if done {
				break
			}
			logger.Debug("fetching next batch of msgs")
			natsMsgs, err := sub.Fetch(fetchBatchSize, nats.MaxWait(s.PullMaxWait))
			if err != nil {
				if errors.Is(err, nats.ErrTimeout) {
					logger.Debug("no new messages, breaking")
					break
				}
				logger.Info("fetch err", "err", err)
				break
			}
			numMsgs := len(natsMsgs)
			logger.Info("fetch done", "numMsgs", numMsgs)
			if numMsgs == 0 {
				break
			}
			for _, nm := range natsMsgs {
				logger.Debug("received msg", "msg", nm)
				err = nm.Ack(nats.Context(ctx))
				if err != nil {
					logger.Info("msg ack failed", "error", err)
					continue
				}
				logger.Debug("acked msg", "msg", nm)

				msgInfo, err := jsm.ParseJSMsgMetadata(nm)
				if err != nil {
					logger.Info("msg metadata parse failed", "error", err)
					continue
				}
				if msgInfo.TimeStamp().After(deadline) {
					done = true
					break
				}
				logger.Debug("rcvd msgInfo", "msgInfo", msgInfo)
				m := new(pubsub.Msg)
				err = proto.Unmarshal(nm.Data, m)
				if err != nil {
					logger.Info("proto unmarshal failed", "error", err)
					continue
				}
				m.Sequence = msgInfo.StreamSequence()
				rs = append(rs, m)
			}
		}
		return rs, nil
	}
}

func (s *natsSubscriber) Stop(name, subject string) {
	s.m.Lock()
	defer s.m.Unlock()
	// no connection, nothing to stop
	if s.nc == nil {
		s.subscribers = make(map[string]map[string]context.CancelFunc)
		return
	}
	// no name, stop all
	if name == "" {
		for _, subj := range s.subscribers {
			for _, cfn := range subj {
				cfn()
			}
		}
		s.nc.Close()
		s.subscribers = make(map[string]map[string]context.CancelFunc)
		return
	}
	// name provided, check if it exists
	if subj, ok := s.subscribers[name]; ok {
		// name exists, no subject provided, stop all subscription to all subject for the named subscriber
		if subject == "" {
			for _, cfn := range subj {
				cfn()
			}
			delete(s.subscribers, name)
			return
		}
		// name exists and subject provided
		if cfn, ok := subj[subject]; ok {
			cfn()
			delete(subj, subject)
		}
	}
}
