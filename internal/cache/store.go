// Package cache contains the local, single-node cache implementation.
package cache

import (
	"errors"
	"time"
)

var ErrCapacity = errors.New("store capacity reached")

// Store is the boundary between HTTP/protocol code and local storage.
// Keep it small: distributed replication can later wrap the same interface.
type Store interface {
	Get(key string) (value []byte, found bool)
	Put(key string, value []byte, ttl time.Duration) error
	Delete(key string) error
}
