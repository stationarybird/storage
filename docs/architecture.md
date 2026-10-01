# Architecture notes

## Now: single-node foundation

`main` owns process lifecycle and configuration. `internal/httpapi` translates
HTTP requests to calls on `cache.Store`. `internal/cache` owns storage,
concurrency, expiry, and eviction.

The server now opens `cache.DurableStore` with a WAL at `replicated.wal` by default.
Run `go run .`; set `CACHE_WAL_PATH` to choose another file (its parent directory
must already exist). Each WAL has an exclusive process lock on Unix systems.

Durable writes append a versioned, length-prefixed JSON record with a CRC32
checksum and call `Sync` before updating memory or returning HTTP 204. Recovery
replays puts/deletes in order, truncates incomplete trailing frames, and rejects
checksum corruption. A failed write/sync returns HTTP 503 and disables further
writes until reopening. Such a failed operation has an uncertain outcome: it may
appear after recovery even though the client received an error.

TTL is stored as an absolute timestamp, so downtime counts toward expiry. Clock
changes affect expiry. Standalone storage reclaims expired keys. Replicated
storage retains expiry metadata and tombstones to prevent resurrection, so its
capacity counts retained keys, not just live values. Capacity errors count as
failed replica acknowledgements; a quorum failure returns 503. Non-positive
capacity means unlimited. See [replication.md](replication.md).

`MemoryStore` remains available as the volatile LRU cache implementation.
`DurableStore` does not evict live data: all live values must fit in memory.
WAL frames are limited to 16 MiB including JSON/base64 overhead. Disk-backed
reads remain future work.

## Snapshots and compaction

`DurableStore.Snapshot()` captures entries and their absolute expiry times
in `<WAL path>.snapshot`. Graceful server shutdown invokes it after HTTP draining.
There is no periodic snapshot scheduler yet; an uninterrupted server continues
growing its WAL until a snapshot is requested.

Each new WAL mutation has an increasing sequence number. Recovery loads the
checksummed snapshot and replays only records newer than its sequence. Legacy
WALs without sequence numbers are supported by assigning their physical order.
Keep the WAL and snapshot together when moving or backing up a stopped store;
deleting the snapshot after compaction loses the data it contains.

Snapshot creation holds the store mutex, temporarily pausing reads and writes.
It writes a temporary file beside the destination, syncs it, renames it over the
snapshot, and syncs the directory. Only then does it truncate and sync the WAL.
The WAL inode stays in place, preserving its exclusive lock. Interrupted
temporary files are ignored during recovery. Corrupt snapshots fail startup;
there is no unsafe fallback to a WAL that may already have been compacted.

Snapshot serialization currently uses additional memory proportional to the
live dataset. Tests exercise abrupt process exit at publication/compaction
boundaries; they do not emulate filesystem or hardware power-loss behavior.

## Browser lab

Run `go run .` and open `http://localhost:8080/`. The dashboard now shows actual
configured ownership via `GET /debug/cluster?key=...`. KV responses identify the
coordinator and replica nodes. See [local-cluster.md](local-cluster.md) for a
three-process demo. The standalone diagnostic `/debug/placement` remains
available for hypothetical placement calculations.

Request flow is browser -> quorum coordinator -> ring replicas -> DurableStores.
Internal endpoints bypass coordination. The UI shows conflicts and retains
context to let users resolve them through a subsequent Put or Delete.

## Distributed placement

`internal/ring` now provides an immutable consistent-hash ring. For example,
`ring.New([]string{"a", "b", "c"}, 64)` assigns 64 positions per node;
`Owner(key)` returns the next clockwise node and `Replicas(key, 3)` returns
distinct node IDs in preference order. Hashing uses the first 64 bits of SHA-256
with separate key/node encodings and deterministic collision tie-breaking.
All nodes must agree on membership, encoding, and virtual-node count.

The coordinator uses the ring for replica selection. The ring does not move
records or send replication traffic. Safe membership changes require data
movement, which is not implemented yet.

Add packages beside `cache` rather than embedding distributed concerns in it:

* `internal/ring` — consistent hashing and replica preference lists.
* `internal/replication` — quorum coordination, versions, read repair.
* `internal/membership` — gossip, failure detection, and rebalancing.
* `internal/persistence` — WAL and snapshots, when local durability begins.

The HTTP API should depend on a narrow interface, so changing from local
storage to a quorum coordinator does not require rewriting handlers.
