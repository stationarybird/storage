package cache

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func mustSucceed(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotRecoveryAndCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s := openTestStore(t, path, 10)
	mustSucceed(t, s.Put("keep", []byte("old"), 0))
	mustSucceed(t, s.Put("deleted", []byte("x"), 0))
	mustSucceed(t, s.Delete("deleted"))
	mustSucceed(t, s.Put("expired", []byte("x"), time.Nanosecond))
	mustSucceed(t, s.Put("live", []byte("x"), time.Hour))
	expires := s.entries["live"].Expires
	mustSucceed(t, s.Snapshot())
	info, err := os.Stat(path)
	mustSucceed(t, err)
	if info.Size() != 0 {
		t.Fatal("WAL not compacted")
	}
	// Compaction must retain the file lock.
	if other, err := OpenDurableStore(path, 10); err == nil {
		other.Close()
		t.Fatal("snapshot released WAL lock")
	}
	mustSucceed(t, s.Put("keep", []byte("new"), 0))
	mustSucceed(t, s.Delete("live"))
	mustSucceed(t, s.Put("later", []byte("x"), 0))
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	if v, ok := s.Get("keep"); !ok || string(v) != "new" {
		t.Fatalf("value: %q %v", v, ok)
	}
	for _, key := range []string{"deleted", "expired", "live"} {
		if _, ok := s.Get(key); ok {
			t.Fatalf("resurrected %s", key)
		}
	}
	// Preserve an absolute expiry through repeated checkpoints.
	mustSucceed(t, s.Put("ttl", []byte("x"), time.Until(expires)))
	expires = s.entries["ttl"].Expires
	mustSucceed(t, s.Snapshot())
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	if !s.entries["ttl"].Expires.Equal(expires) {
		t.Fatal("expiry reset")
	}
	if _, ok := s.Get("later"); !ok {
		t.Fatal("lost later write")
	}
}

func TestSnapshotCrashBoundaries(t *testing.T) {
	if path := os.Getenv("CACHE_SNAPSHOT_CRASH_PATH"); path != "" {
		s := openTestStore(t, path, 10)
		mustSucceed(t, s.Put("deleted", []byte("x"), 0))
		mustSucceed(t, s.Delete("deleted"))
		mustSucceed(t, s.Put("kept", []byte("x"), 0))
		switch os.Getenv("CACHE_SNAPSHOT_PHASE") {
		case "before-publication":
			mustSucceed(t, os.WriteFile(filepath.Join(filepath.Dir(path), ".snapshot-interrupted"), []byte("partial"), 0600))
		case "after-publication":
			s.mu.Lock()
			mustSucceed(t, s.publishSnapshot())
			s.mu.Unlock()
		case "after-compaction":
			mustSucceed(t, s.Snapshot())
		}
		os.Exit(0)
	}
	for _, phase := range []string{"before-publication", "after-publication", "after-compaction"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.wal")
			cmd := exec.Command(os.Args[0], "-test.run=^TestSnapshotCrashBoundaries$")
			cmd.Env = append(os.Environ(), "CACHE_SNAPSHOT_CRASH_PATH="+path, "CACHE_SNAPSHOT_PHASE="+phase)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v: %s", err, output)
			}
			s := openTestStore(t, path, 10)
			if _, ok := s.Get("kept"); !ok {
				t.Fatal("lost acknowledged write")
			}
			if _, ok := s.Get("deleted"); ok {
				t.Fatal("resurrected delete")
			}
			mustSucceed(t, s.Put("next", []byte("x"), 0))
			mustSucceed(t, s.Close())
			s = openTestStore(t, path, 10)
			if _, ok := s.Get("next"); !ok {
				t.Fatal("lost post-recovery write")
			}
		})
	}
}

func TestSnapshotCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s := openTestStore(t, path, 10)
	mustSucceed(t, s.Put("a", []byte("x"), 0))
	mustSucceed(t, s.Snapshot())
	mustSucceed(t, s.Close())
	data, err := os.ReadFile(path + ".snapshot")
	mustSucceed(t, err)
	var envelope snapshotEnvelope
	mustSucceed(t, json.Unmarshal(data, &envelope))
	envelope.Checksum ^= 1
	data, err = json.Marshal(envelope)
	mustSucceed(t, err)
	mustSucceed(t, os.WriteFile(path+".snapshot", data, 0600))
	if s, err := OpenDurableStore(path, 10); err == nil {
		s.Close()
		t.Fatal("ignored corrupt snapshot")
	}
}

func TestSnapshotLegacyWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	payload := []byte(`{"version":1,"op":"put","key":"legacy","value":"eA=="}`)
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	mustSucceed(t, os.WriteFile(path, frame, 0600))
	s := openTestStore(t, path, 10)
	mustSucceed(t, s.Put("new", []byte("x"), 0))
	s.mu.Lock()
	mustSucceed(t, s.publishSnapshot()) // Old unnumbered WAL still present.
	s.mu.Unlock()
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	mustSucceed(t, s.Snapshot())
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	for _, key := range []string{"legacy", "new"} {
		if v, ok := s.Get(key); !ok || string(v) != "x" {
			t.Fatalf("lost %s", key)
		}
	}
}

func TestSnapshotPublicationFailureKeepsWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s := openTestStore(t, path, 10)
	mustSucceed(t, s.Put("a", []byte("x"), 0))
	s.snapshotPath = filepath.Join(path, "invalid")
	if err := s.Snapshot(); err == nil {
		t.Fatal("expected publication failure")
	}
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	if _, ok := s.Get("a"); !ok {
		t.Fatal("lost WAL on publication failure")
	}
}

func TestSnapshotConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s := openTestStore(t, path, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 5 {
			if err := s.Snapshot(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for _, key := range []string{"a", "b", "c"} {
		mustSucceed(t, s.Put(key, []byte(key), 0))
	}
	wg.Wait()
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	for _, key := range []string{"a", "b", "c"} {
		if v, ok := s.Get(key); !ok || string(v) != key {
			t.Fatalf("lost %s", key)
		}
	}
}

func TestEmptySnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s := openTestStore(t, path, 10)
	mustSucceed(t, s.Snapshot())
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	mustSucceed(t, s.Put("a", nil, 0))
	mustSucceed(t, s.Delete("a"))
	mustSucceed(t, s.Snapshot())
	mustSucceed(t, s.Close())
	s = openTestStore(t, path, 10)
	if _, ok := s.Get("a"); ok {
		t.Fatal("resurrected deleted key")
	}
}
