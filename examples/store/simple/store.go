package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/yndd/ndd-runtime/pkg/logging"
	"github.com/yndd/pubsub/store"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	c := store.Config{
		Address:  nats.DefaultURL,
		Insecure: true,

		Bucket:      "yndd",
		Description: "yndd KV bucket",
		History:     10,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := zap.New()
	kvs, err := store.NewNATSKVStore(ctx, c, logging.NewLogrLogger(l))
	if err != nil {
		panic(err)
	}
	fmt.Println("creating {key1, val1}")
	rev, err := kvs.Create("key1", []byte("val1"))
	if err != nil {
		panic(err)
	}
	fmt.Println("key1 revision", rev)
	fmt.Println("updating {key1, val2}")
	rev, err = kvs.Update("key1", []byte("val2"), rev)
	if err != nil {
		panic(err)
	}
	fmt.Println("key1 revision", rev)
	kv1, err := kvs.Get("key1")
	if err != nil {
		panic(err)
	}
	fmt.Println("key1", kv1)
	fmt.Println("deleting key1")
	err = kvs.Delete("key1", kv1.Revision)
	if err != nil {
		panic(err)
	}
	_, err = kvs.Get("key1")
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			fmt.Println("key1 not found")
			return
		}
		panic(err)
	}
}
