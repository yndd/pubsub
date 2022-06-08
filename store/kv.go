package store

import (
	"context"
)

type KV struct {
	Key       string
	Value     []byte
	Revision  uint64
	Operation Operation
}

type Operation string

const (
	PUTOperation     Operation = "PUT"
	DELETEOperation  Operation = "DELETE"
	PURGEOperation   Operation = "PURGE"
	UNKNOWNOperation Operation = "UNKNOWN"
)

type Store interface {
	Create(key string, value []byte) (uint64, error)
	Get(key string) (*KV, error)
	GetRevision(key string, rev uint64) (*KV, error)
	//
	Update(key string, value []byte, rev uint64) (uint64, error)
	Delete(key string, rev uint64) error
	Watch(ctx context.Context, keys string) (chan *KV, error)
	WatchCh(ctx context.Context, keys string, ch chan *KV) error
	//
	History(key string) ([]*KV, error)
	Keys() ([]string, error)
}
