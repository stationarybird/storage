# Architecture notes

## Now: single-node foundation

`main` owns process lifecycle and configuration. `internal/httpapi` translates
HTTP requests to calls on `cache.Store`. `internal/cache` owns storage,
concurrency, expiry, and eviction.

## Later: distributed node

Add packages beside `cache` rather than embedding distributed concerns in it:

* `internal/ring` — consistent hashing and replica preference lists.
* `internal/replication` — quorum coordination, versions, read repair.
* `internal/membership` — gossip, failure detection, and rebalancing.
* `internal/persistence` — WAL and snapshots, when local durability begins.

The HTTP API should depend on a narrow interface, so changing from local
storage to a quorum coordinator does not require rewriting handlers.
