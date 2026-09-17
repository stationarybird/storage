package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryStorePutGetAndOverwrite(t *testing.T) {
	store := NewMemoryStore(10)
	store.Put("language", []byte("go"), 0)

	value, found := store.Get("language")
	if !found {
		t.Fatal("Get() did not find stored key")
	}
	if got, want := string(value), "go"; got != want {
		t.Fatalf("Get() = %q, want %q", got, want)
	}

	store.Put("language", []byte("python"), 0)
	value, found = store.Get("language")
	if !found || string(value) != "python" {
		t.Fatalf("Get() after overwrite = %q, found=%v", value, found)
	}
}

func TestMemoryStoreMissingAndDelete(t *testing.T) {
	store := NewMemoryStore(10)
	if _, found := store.Get("missing"); found {
		t.Fatal("Get() found a key that was never stored")
	}

	store.Put("key", []byte("value"), 0)
	store.Delete("key")
	store.Delete("key") // Deletes must be idempotent.
	if _, found := store.Get("key"); found {
		t.Fatal("Get() found a deleted key")
	}
}

func TestMemoryStoreExpiresEntries(t *testing.T) {
	store := NewMemoryStore(10)
	store.Put("short-lived", []byte("value"), 10*time.Millisecond)

	time.Sleep(25 * time.Millisecond)
	if _, found := store.Get("short-lived"); found {
		t.Fatal("Get() found an expired key")
	}
	if got := len(store.entries); got != 0 {
		t.Fatalf("stored entry count after expiry = %d, want 0", got)
	}
	if got := store.order.Len(); got != 0 {
		t.Fatalf("LRU node count after expiry = %d, want 0", got)
	}
}

func TestMemoryStoreCopiesValuesAtBoundary(t *testing.T) {
	store := NewMemoryStore(10)
	input := []byte("original")
	store.Put("key", input, 0)

	input[0] = 'X'
	value, found := store.Get("key")
	if !found || string(value) != "original" {
		t.Fatalf("stored value = %q, found=%v; cache retained caller mutation", value, found)
	}

	value[0] = 'Y'
	value, found = store.Get("key")
	if !found || string(value) != "original" {
		t.Fatalf("stored value = %q, found=%v; cache exposed internal bytes", value, found)
	}
}

func TestMemoryStoreEvictsLeastRecentlyUsedEntry(t *testing.T) {
	store := NewMemoryStore(2)
	store.Put("a", []byte("first"), 0)
	store.Put("b", []byte("second"), 0)

	if _, found := store.Get("a"); !found {
		t.Fatal("Get(a) did not find stored key")
	}
	store.Put("c", []byte("third"), 0)

	if _, found := store.Get("b"); found {
		t.Fatal("Get(b) found least-recently-used entry after eviction")
	}
	for _, key := range []string{"a", "c"} {
		if _, found := store.Get(key); !found {
			t.Fatalf("Get(%q) did not find retained entry", key)
		}
	}
}

func TestMemoryStoreOverwriteDoesNotCreateAnotherEntry(t *testing.T) {
	store := NewMemoryStore(2)
	store.Put("a", []byte("first"), 0)
	store.Put("b", []byte("second"), 0)
	store.Put("a", []byte("updated"), 0)
	store.Put("c", []byte("third"), 0)

	if got := len(store.entries); got != 2 {
		t.Fatalf("stored entry count = %d, want 2", got)
	}
	if got := store.order.Len(); got != 2 {
		t.Fatalf("LRU node count = %d, want 2", got)
	}
	if _, found := store.Get("b"); found {
		t.Fatal("Get(b) found key that should have been evicted")
	}
	value, found := store.Get("a")
	if !found || string(value) != "updated" {
		t.Fatalf("Get(a) = %q, found=%v", value, found)
	}
}

func TestMemoryStoreConcurrentAccess(t *testing.T) {
	store := NewMemoryStore(100)
	const workers = 16
	const operations = 200

	var group sync.WaitGroup
	for worker := range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for operation := range operations {
				key := fmt.Sprintf("key-%d", operation%8)
				store.Put(key, []byte(fmt.Sprintf("worker-%d", worker)), 0)
				store.Get(key)
				if operation%10 == 0 {
					store.Delete(key)
				}
			}
		}()
	}
	group.Wait()
}
