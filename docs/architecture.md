# Architecture notes

## Now: single-node foundation

`main` owns process lifecycle and configuration. `internal/httpapi` translates
HTTP requests to calls on `cache.Store`. `internal/cache` owns storage,
concurrency, expiry, and eviction.

The server now opens `cache.DurableStore` with a WAL at `cache.wal` by default.
Run `go run .`; set `CACHE_WAL_PATH` to choose another file (its parent directory
must already exist). Each WAL has an exclusive process lock on Unix systems.

Durable writes append a versioned, length-prefixed JSON record with a CRC32
checksum and call `Sync` before updating memory or returning HTTP 204. Recovery
replays puts/deletes in order, truncates incomplete trailing frames, and rejects
checksum corruption. A failed write/sync returns HTTP 503 and disables further
writes until reopening. Such a failed operation has an uncertain outcome: it may
appear after recovery even though the client received an error.

TTL is stored as an absolute timestamp, so downtime counts toward expiry. Clock
changes affect expiry. Expired keys are reclaimed on reads and before puts.
Capacity is a count of live keys, not bytes. New keys at capacity receive HTTP
507; updates and deletes remain allowed. Recovery preserves all live keys even
if capacity has been reduced. Non-positive capacity means unlimited.

`MemoryStore` remains available as the volatile LRU cache implementation.
`DurableStore` does not evict live data: all live values must fit in memory.
The WAL grows without compaction and recovery reads the entire log. Snapshots
and disk-backed reads are future work. Frames are limited to 16 MiB including
JSON/base64 overhead. Snapshot support is not implemented yet.

## Later: distributed node

Add packages beside `cache` rather than embedding distributed concerns in it:

* `internal/ring` — consistent hashing and replica preference lists.
* `internal/replication` — quorum coordination, versions, read repair.
* `internal/membership` — gossip, failure detection, and rebalancing.
* `internal/persistence` — WAL and snapshots, when local durability begins.

The HTTP API should depend on a narrow interface, so changing from local
storage to a quorum coordinator does not require rewriting handlers.
