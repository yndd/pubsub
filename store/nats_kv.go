package store

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/yndd/ndd-runtime/pkg/logging"
)

const (
	reconnectDelay    = time.Second
	defaultBucketName = "ndd-system"
)

type natsKVStore struct {
	Config
	logger logging.Logger
	m      *sync.RWMutex
	nc     *nats.Conn
	kv     nats.KeyValue
}
type StorageType string

const (
	Memory StorageType = "memory"
	File   StorageType = "file"
)

type Config struct {
	Address             string
	Insecure            bool
	CertificateSecret   string
	CaCertificateSecret string
	//
	Bucket       string
	Description  string
	MaxValueSize int32
	History      uint8
	TTL          time.Duration
	MaxBytes     int64
	Storage      StorageType
	Replicas     int
}

func NewNATSKVStore(ctx context.Context, c Config, l logging.Logger) (Store, error) {
	if c.Bucket == "" {
		c.Bucket = defaultBucketName
	}
	if l == nil {
		l = logging.NewNopLogger()
	}
	n := &natsKVStore{
		Config: c,
		logger: l,
		m:      &sync.RWMutex{},
	}

	jsc, err := n.dialConn(ctx)
	if err != nil {
		return nil, err
	}
	natsStorage := nats.MemoryStorage
	if c.Storage == File {
		natsStorage = nats.FileStorage
	}
	n.kv, err = jsc.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       c.Bucket,
		Description:  c.Description,
		MaxValueSize: c.MaxValueSize,
		History:      c.History,
		TTL:          c.TTL,
		MaxBytes:     c.MaxBytes,
		Storage:      natsStorage,
		Replicas:     c.Replicas,
	})
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (n *natsKVStore) Get(key string) (*KV, error) {
	e, err := n.kv.Get(key)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, os.ErrNotExist
	}

	return &KV{
		Key:       e.Key(),
		Value:     e.Value(),
		Revision:  e.Revision(),
		Operation: operation(e),
	}, nil
}

func (n *natsKVStore) Put(key string, value []byte) (uint64, error) {
	return n.kv.Put(key, value)
}

func (n *natsKVStore) Create(key string, value []byte) (uint64, error) {
	return n.kv.Create(key, value)
}

func (n *natsKVStore) Update(key string, value []byte, last uint64) (uint64, error) {
	return n.kv.Update(key, value, last)
}

func (n *natsKVStore) Delete(key string, rev uint64) error {
	var opts []nats.DeleteOpt
	if rev > 0 {
		opts = []nats.DeleteOpt{nats.LastRevision(rev)}
	}
	return n.kv.Delete(key, opts...)
}

func (n *natsKVStore) Watch(ctx context.Context, keys string) (chan *KV, error) {
	kvCh := make(chan *KV)
	err := n.WatchCh(ctx, keys, kvCh)
	if err != nil {
		return nil, err
	}
	return kvCh, nil
}

func (n *natsKVStore) WatchCh(ctx context.Context, keys string, ch chan *KV) error {
	var kw nats.KeyWatcher
	var err error
	switch keys {
	case "":
		kw, err = n.kv.WatchAll()
	default:
		kw, err = n.kv.Watch(keys)
	}
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				kw.Stop()
				return
			case nkv, ok := <-kw.Updates():
				if !ok {
					return
				}
				if nkv == nil {
					continue
				}
				ch <- &KV{
					Key:       nkv.Key(),
					Value:     nkv.Value(),
					Revision:  nkv.Revision(),
					Operation: operation(nkv),
				}
			}
		}
	}()
	return nil
}

func (n *natsKVStore) History(key string) ([]*KV, error) {
	nkvs, err := n.kv.History(key)
	if err != nil {
		return nil, err
	}
	kvs := make([]*KV, 0, len(nkvs))
	for _, nkv := range nkvs {
		kvs = append(kvs, toKV(nkv))
	}
	return kvs, nil
}

func (n *natsKVStore) Keys() ([]string, error) {
	return n.kv.Keys()
}

func (n *natsKVStore) GetRevision(key string, revision uint64) (*KV, error) {
	nkv, err := n.kv.GetRevision(key, revision)
	if err != nil {
		return nil, err
	}
	return toKV(nkv), nil
}

// dialConn will (re)try create a NATS connection and JetStream context until it succeeds or ctx is Done
func (n *natsKVStore) dialConn(ctx context.Context) (nats.JetStreamContext, error) {
	opts := make([]nats.Option, 0)
	opts = append(opts,
		nats.ReconnectWait(reconnectDelay),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			n.logger.Info("NATS", "error", err)
		}),
		nats.DisconnectHandler(func(c *nats.Conn) {
			n.logger.Info("Disconnected from NATS")
		}),
		nats.ClosedHandler(func(c *nats.Conn) {
			n.logger.Info("NATS connection is closed")
		}),
	)
	var err error
STARTCONN:
	select {
	case <-ctx.Done():
		n.logger.Info("context done", "error", ctx.Err())
		return nil, ctx.Err()
	default:
		n.m.Lock()
		if n.nc == nil || !n.nc.IsConnected() {
			n.nc, err = nats.Connect(n.Address, opts...)
			if err != nil {
				n.m.Unlock()
				n.logger.Info("nats connection failed", "error", err)
				time.Sleep(reconnectDelay)
				goto STARTCONN
			}
		}
		n.m.Unlock()
		jsc, err := n.nc.JetStream()
		if err != nil {
			n.logger.Info("inconsistent JetStream Options", "error", err)
			time.Sleep(reconnectDelay)
			goto STARTCONN
		}
		return jsc, nil
	}
}

func operation(nkv nats.KeyValueEntry) Operation {
	switch nkv.Operation() {
	case nats.KeyValueDelete:
		return DELETEOperation
	case nats.KeyValuePut:
		return PUTOperation
	case nats.KeyValuePurge:
		return PURGEOperation
	default:
		return UNKNOWNOperation
	}
}

func toKV(nkv nats.KeyValueEntry) *KV {
	return &KV{
		Key:       nkv.Key(),
		Value:     nkv.Value(),
		Revision:  nkv.Revision(),
		Operation: operation(nkv),
	}
}
