package cache

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDurableAbruptExit(t *testing.T) {
	if path := os.Getenv("CACHE_TEST_CRASH_WAL"); path != "" {
		s, err := OpenDurableStore(path, 10)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Put("acknowledged", []byte("survives"), 0); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // No Close, defers, or graceful shutdown.
	}
	path := filepath.Join(t.TempDir(), "crash.wal")
	cmd := exec.Command(os.Args[0], "-test.run=^TestDurableAbruptExit$")
	cmd.Env = append(os.Environ(), "CACHE_TEST_CRASH_WAL="+path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v: %s", err, output)
	}
	s := openTestStore(t, path, 10)
	if value, found := s.Get("acknowledged"); !found || string(value) != "survives" {
		t.Fatalf("after abrupt exit: %q, %v", value, found)
	}
}

func openTestStore(t *testing.T, path string, capacity int) *DurableStore {
	t.Helper()
	s, err := OpenDurableStore(path, capacity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestDurableRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s := openTestStore(t, path, 3)
	for _, value := range []string{"old", "new"} {
		if err := s.Put("key", []byte(value), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("deleted", []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("deleted"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("expired", []byte("x"), time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("live", []byte("x"), time.Hour); err != nil {
		t.Fatal(err)
	}
	expires := s.entries["live"].Expires
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 1) // Reduced capacity must not discard recovered data.
	if v, ok := s.Get("key"); !ok || string(v) != "new" {
		t.Fatalf("recovered %q, %v", v, ok)
	}
	for _, key := range []string{"deleted", "expired"} {
		if _, ok := s.Get(key); ok {
			t.Fatalf("resurrected %s", key)
		}
	}
	if !s.entries["live"].Expires.Equal(expires) {
		t.Fatal("restart changed expiry")
	}
	if err := s.Put("overflow", nil, 0); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}
}

func TestDurableCapacityAndOwnership(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "store.wal"), 1)
	value := []byte("value")
	if err := s.Put("a", value, 0); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	v, _ := s.Get("a")
	if string(v) != "value" {
		t.Fatal("input aliases store")
	}
	v[0] = 'Y'
	if v, _ := s.Get("a"); string(v) != "value" {
		t.Fatal("output aliases store")
	}
	if err := s.Put("b", nil, 0); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	if err := s.Put("a", []byte("updated"), time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", nil, 0); err != nil {
		t.Fatalf("expired key consumes capacity: %v", err)
	}
}

func TestDurableIncompleteTail(t *testing.T) {
	for _, tail := range [][]byte{{0, 0}, {0, 0, 0, 10, 0, 0, 0, 0, 'x'}} {
		path := filepath.Join(t.TempDir(), "store.wal")
		s := openTestStore(t, path, 10)
		if err := s.Put("a", []byte("saved"), 0); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(tail); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		s = openTestStore(t, path, 10)
		if v, ok := s.Get("a"); !ok || string(v) != "saved" {
			t.Fatal("lost complete record")
		}
		if err := s.Put("b", []byte("after recovery"), 0); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
		s = openTestStore(t, path, 10)
		if _, ok := s.Get("b"); !ok {
			t.Fatal("append after recovery failed")
		}
	}
}

func TestDurableRejectsCorruptionAndSecondOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s := openTestStore(t, path, 10)
	if other, err := OpenDurableStore(path, 10); err == nil {
		other.Close()
		t.Fatal("allowed second WAL owner")
	}
	if err := s.Put("a", []byte("saved"), 0); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, 8); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if recovered, err := OpenDurableStore(path, 10); err == nil {
		recovered.Close()
		t.Fatal("accepted corrupt WAL")
	}
}

func TestDurableWriteFailure(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "store.wal"), 10)
	if err := s.Put("a", []byte("old"), 0); err != nil {
		t.Fatal(err)
	}
	_ = s.file.Close() // Simulate an unavailable WAL.
	if err := s.Put("a", []byte("new"), 0); err == nil {
		t.Fatal("acknowledged failed write")
	}
	if v, _ := s.Get("a"); string(v) != "old" {
		t.Fatal("failed write changed memory")
	}
	if err := s.Delete("a"); err == nil {
		t.Fatal("accepted write after WAL failure")
	}
}

func TestDurableConcurrentAccess(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "store.wal"), 10)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if err := s.Put("shared", []byte("value"), 0); err != nil {
					t.Error(err)
				}
				s.Get("shared")
				if err := s.Delete("shared"); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}
