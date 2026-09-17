// Package cache contains the local, single-node cache implementation.
package cache

import "time"

// Store is the boundary between HTTP/protocol code and local storage.
// Keep it small: distributed replication can later wrap the same interface.
type Store interface {
	Get(key string) (value []byte, found bool)
	Put(key string, value []byte, ttl time.Duration)
	Delete(key string)
}
