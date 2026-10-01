package cache

import (
	"fmt"
	"os"
	"reflect"

	"distributedcache/internal/version"
)

// CheckReplicaMode prevents silently mixing the old single-copy data format
// with versioned records. An offline migration is required for existing data.
func (s *DurableStore) CheckReplicaMode() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.entries {
		if len(r.Siblings) == 0 {
			return fmt.Errorf("WAL contains unversioned data; use a fresh CACHE_WAL_PATH for replication (old files remain unchanged)")
		}
	}
	return nil
}

// Records returns durable siblings, including tombstones and expired versions.
// Versioned records have no storage-level TTL: removing metadata could resurrect
// an older value arriving from an offline replica.
func (s *DurableStore) Records(key string) ([]version.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, os.ErrClosed
	}
	r, ok := s.entries[key]
	if !ok {
		return []version.Record{}, nil
	}
	if len(r.Siblings) == 0 {
		return nil, fmt.Errorf("legacy unversioned key; use fresh replicated storage or migrate first")
	}
	return version.Merge(r.Siblings)
}

// MergeRecords atomically merges and fsyncs the resulting sibling set before ack.
func (s *DurableStore) MergeRecords(key string, incoming []version.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if s.failed != nil {
		return s.failed
	}
	old, exists := s.entries[key]
	if exists && len(old.Siblings) == 0 {
		return fmt.Errorf("legacy unversioned key cannot be overwritten by replication")
	}
	merged, err := version.Merge(old.Siblings, incoming)
	if err != nil {
		return err
	}
	if len(merged) == 0 || reflect.DeepEqual(old.Siblings, merged) {
		return nil
	}
	if !exists && s.capacity > 0 && len(s.entries) >= s.capacity {
		return ErrCapacity
	}
	return s.append(walRecord{Version: 2, Op: "put", Key: key, Siblings: merged})
}
