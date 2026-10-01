package cache

import (
	"distributedcache/internal/version"
	"path/filepath"
	"testing"
	"time"
)

func TestReplicaPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.wal")
	s := openTestStore(t, path, 10)
	a, _ := version.New("a", "one", false, time.Time{}, nil)
	b, _ := version.New("b", "two", false, time.Time{}, nil)
	mustSucceed(t, s.MergeRecords("key", []version.Record{a, b}))
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	records, err := s.Records("key")
	mustSucceed(t, err)
	if len(records) != 2 {
		t.Fatal("WAL lost siblings")
	}
	del, _ := version.New("a", "", true, time.Time{}, version.Context(records))
	mustSucceed(t, s.MergeRecords("key", []version.Record{del}))
	expired, _ := version.New("b", "expired", false, time.Now().Add(-time.Hour), nil)
	mustSucceed(t, s.MergeRecords("ttl", []version.Record{expired}))
	mustSucceed(t, s.Snapshot())
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	mustSucceed(t, s.MergeRecords("key", []version.Record{a}))
	records, err = s.Records("key")
	mustSucceed(t, err)
	if len(records) != 1 || !records[0].Deleted {
		t.Fatal("old value resurrected after snapshot")
	}
	records, err = s.Records("ttl")
	mustSucceed(t, err)
	if len(records) != 1 || records[0].Live(time.Now()) {
		t.Fatal("expired causal barrier lost")
	}
}

func TestReplicaFailedWriteAndIdempotency(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "replica.wal"), 1)
	r, _ := version.New("a", "value", false, time.Time{}, nil)
	mustSucceed(t, s.MergeRecords("key", []version.Record{r}))
	sequence := s.sequence
	mustSucceed(t, s.MergeRecords("key", []version.Record{r}))
	if s.sequence != sequence {
		t.Fatal("duplicate delivery appended another WAL record")
	}
	if err := s.MergeRecords("other", []version.Record{r}); err != ErrCapacity {
		t.Fatalf("capacity: %v", err)
	}
	next, _ := version.New("a", "next", false, time.Time{}, r.Clock)
	mustSucceed(t, s.file.Close())
	if err := s.MergeRecords("key", []version.Record{next}); err == nil {
		t.Fatal("acknowledged failed WAL write")
	}
	records, err := s.Records("key")
	mustSucceed(t, err)
	if records[0].Value != "value" {
		t.Fatal("failed write changed state")
	}
}
