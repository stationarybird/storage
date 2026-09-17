package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	value      []byte
	expiration time.Time
	node       *list.Element
}
type MemoryStore struct {
	capacity int
	entries  map[string]*entry
	order    *list.List // Front is most recently used; back is least recently used.
	mu       sync.RWMutex
}

func NewMemoryStore(capacity int) *MemoryStore {
	return &MemoryStore{
		capacity: capacity,
		entries:  make(map[string]*entry),
		order:    list.New(),
	}
}

func (s *MemoryStore) Get(key string) ([]byte, bool) {
	// Get may lazily delete an expired entry, so it needs exclusive access.
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.entries[key]
	if !exists {
		return nil, false
	}

	if !item.expiration.IsZero() && !time.Now().Before(item.expiration) {
		s.remove(key, item)
		return nil, false
	}

	s.order.MoveToFront(item.node)
	return append([]byte(nil), item.value...), true
}

func (s *MemoryStore) Put(key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiration time.Time
	if ttl > 0 {
		expiration = time.Now().Add(ttl)
	}

	if item, exists := s.entries[key]; exists {
		item.value = append([]byte(nil), value...)
		item.expiration = expiration
		s.order.MoveToFront(item.node)
		return nil
	}

	item := &entry{
		value:      append([]byte(nil), value...),
		expiration: expiration,
	}
	item.node = s.order.PushFront(key)
	s.entries[key] = item

	if s.capacity > 0 && s.order.Len() > s.capacity {
		leastRecent := s.order.Back()
		leastRecentKey := leastRecent.Value.(string)
		s.remove(leastRecentKey, s.entries[leastRecentKey])
	}
	return nil
}

func (s *MemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if item, exists := s.entries[key]; exists {
		s.remove(key, item)
	}
	return nil
}

// remove deletes an entry and its LRU node. The caller must hold s.mu.
func (s *MemoryStore) remove(key string, item *entry) {
	delete(s.entries, key)
	s.order.Remove(item.node)
}
