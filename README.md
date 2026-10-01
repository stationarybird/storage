# distributedCache

A Dynamo-inspired distributed key-value store written in Go. Nodes use
consistent hashing to select replicas, coordinate quorum reads and writes, and
persist versioned records through a write-ahead log and snapshots.

This is a learning project under active development. It currently implements
fixed-membership replication and read repair; dynamic membership, hinted
handoff, anti-entropy, and a Raft metadata plane are planned.

## Features

- **Consistent hashing:** 64 virtual positions per node and distinct physical
  replicas selected in clockwise order.
- **Configurable quorums:** parallel requests to N replicas; reads require R
  successful replies and writes require W durable acknowledgements.
- **Causal versions:** concurrent writes remain as siblings, with explicit
  context-based resolution instead of timestamp-based last-write-wins.
- **Read repair:** merge observed versions and repair stale replicas after reads.
- **Durability:** checksummed WAL records, fsync before acknowledgement, atomic
  snapshot publication, sequence-based recovery, and WAL compaction.
- **Deletes and expiry:** replicated tombstones and absolute TTL timestamps,
  retaining causal metadata to prevent stale-value resurrection.
- **Browser dashboard:** Get/Put/Delete, replica placement, quorum settings,
  and conflict resolution.

A separate volatile `MemoryStore` implements TTL and O(1) LRU eviction. The
replicated server uses `DurableStore`, which rejects capacity overflow instead
of evicting authoritative records.

## Quick start

Requires Go 1.24 or later on macOS or Linux. WAL locking uses Unix file locks.
There are no external Go dependencies or frontend build steps.

```sh
go run .
```

Open [localhost:8080](http://localhost:8080). Select **Put** to save a value, then
**Get** to retrieve it. A single node runs with N=R=W=1.

By default, records are stored in `replicated.wal` in the working directory.
Graceful shutdown creates `replicated.wal.snapshot` and compacts the WAL.
Restarting rebuilds the in-memory state from those files. Keep both files
together; the snapshot is part of the database, not an optional backup.

## Three-node cluster

Build the binary and create separate storage directories:

```sh
go build -o bin/cache-node .
mkdir -p data/a data/b data/c
```

Set this identical membership in **each of three terminals**:

```sh
export CACHE_PEERS='{"a":"http://localhost:8080","b":"http://localhost:8081","c":"http://localhost:8082"}'
```

Run one node per terminal, from the project root:

```sh
# Terminal 1
CACHE_NODE_ID=a CACHE_LISTEN_ADDR=:8080 CACHE_WAL_PATH=data/a/replicated.wal ./bin/cache-node
```

```sh
# Terminal 2
CACHE_NODE_ID=b CACHE_LISTEN_ADDR=:8081 CACHE_WAL_PATH=data/b/replicated.wal ./bin/cache-node
```

```sh
# Terminal 3
CACHE_NODE_ID=c CACHE_LISTEN_ADDR=:8082 CACHE_WAL_PATH=data/c/replicated.wal ./bin/cache-node
```

The defaults are **N=3, R=2, W=2**. Write through A and read through B or C.
With three members and three replicas, every key has the same three replica
nodes; with more members than replicas, placement distributes keys across
different subsets of the cluster.

To exercise failure handling:

1. Put a key through A's UI.
2. Stop C with Ctrl-C, then update and read the key through A or B.
3. Restart C with the same command and storage files.
4. Read the key to trigger repair of the replica that missed the update.

One unavailable replica is tolerated at these settings. With two unavailable,
requests cannot satisfy a quorum and return `503`.

Use fresh replicated WAL paths when upgrading from the earlier single-owner
implementation. Automatic migration is not implemented, and startup rejects
unversioned keys. Preserve the old WALs and snapshots separately.

## Request flow

```text
Browser / HTTP client
        |
        v
Any node acts as coordinator
        |
        v
Consistent-hash ring selects N replicas
        |
        +----------+----------+
        v          v          v
     Replica A  Replica B  Replica C
     WAL + RAM  WAL + RAM  WAL + RAM
        |          |          |
        +----------+----------+
                   |
          R reads / W durable writes
                   |
                   v
             Client response
```

Writes merge with each replica's existing versions and sync to its WAL before
acknowledgement. The coordinator returns once W replicas succeed; requests to
the remaining replicas continue within their deadline.

Reads merge the first R successful responses. Background collection of late
responses drives best-effort repair. There is no permanent leader, and internal
replica endpoints never forward requests to other nodes.

## HTTP API

Values are JSON strings. Keys are URL path segments; URL-encode special characters.

```sh
# Create or update a value; ttl_ms=0 means no expiry.
curl -i -X PUT http://localhost:8080/v1/kv/hello \
  -H 'Content-Type: application/json' \
  -d '{"value":"world","ttl_ms":0}'

# Read through another node.
curl -i http://localhost:8082/v1/kv/hello

# Delete by writing a replicated tombstone.
curl -i -X DELETE http://localhost:8081/v1/kv/hello

# Inspect configured placement (not replica health).
curl 'http://localhost:8080/debug/cluster?key=hello'
```

| Response | Meaning |
| --- | --- |
| `200` | A single live value was read |
| `204` | A mutation reached its write quorum |
| `400` | Invalid request |
| `404` | No live value in the observed read quorum |
| `409` | Concurrent siblings require explicit resolution |
| `503` | The operation could not satisfy its quorum |

GET includes `context` and `siblings`. Send that context in a subsequent PUT or
DELETE to supersede the versions you observed. Explicit `"context":{}` means
an unobserved write and can create a conflict. If context is omitted, the
coordinator first reads a quorum; it refuses to silently resolve multiple
observed siblings. The dashboard remembers returned context for the next write.

## Configuration

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `CACHE_NODE_ID` | `node-a` | Stable node identifier |
| `CACHE_LISTEN_ADDR` | `:8080` | HTTP listen address |
| `CACHE_PEERS` | Local node at `http://localhost:8080` | JSON map of node IDs to HTTP origins |
| `CACHE_WAL_PATH` | `replicated.wal` | Per-node WAL path; parent directory must exist |
| `CACHE_REPLICAS` | `min(3, membership size)` | Replica count N |
| `CACHE_READ_QUORUM` | `floor(N/2)+1` | Read acknowledgements R |
| `CACHE_WRITE_QUORUM` | `floor(N/2)+1` | Durable write acknowledgements W |

Use identical membership and N/R/W settings on every node. A custom listen
address requires explicit `CACHE_PEERS`. Quorums must satisfy
`1 <= R,W <= N <= membership size`. Capacity is currently fixed in configuration
code at 1,000 retained keys per node; the network deadline is two seconds.

## Guarantees and limitations

- Majority quorums tolerate a single replica failure in the default three-node
  configuration, but only the majority side of a partition remains writable.
  R=W=1 favors availability at the cost of stale reads and more conflicts.
- Quorum overlap does **not** provide linearizability. Reads may miss concurrent
  versions held only by late responders.
- Failed or timed-out writes may have partially committed. Public retries are
  not deduplicated; delivery of the same internal record is idempotent.
- Membership is static. Changing the ring does not migrate existing data.
- Read repair is access-driven. Unread keys have no automatic convergence yet;
  hinted handoff and periodic anti-entropy are not implemented.
- Tombstones, expiry metadata, and causal event history are retained without
  garbage collection. All records must fit in RAM, and retained keys consume
  capacity even when deleted or expired.
- Snapshots pause store operations and run on graceful shutdown or explicit
  `Snapshot()` calls. There is no periodic snapshot scheduler.
- This is an unauthenticated development system. Configuration fingerprints
  detect mismatched peers but do not authenticate them.

## Tests

```sh
go test -race ./...
go vet ./...
```

Tests cover ring placement, concurrent access, causal merging, WAL/snapshot
recovery, abrupt process exit, replica failures, slow responses, partial quorum
failures, sibling resolution, and read repair. Integration tests bind temporary
loopback ports. No throughput or latency benchmark claims are made yet.

## Code map

```text
main.go                    Startup, shutdown, and snapshot lifecycle
config.go                  Environment configuration
internal/cache/            Volatile cache and durable replica storage
internal/version/          Causal records and sibling merging
internal/ring/             Consistent hashing and replica selection
internal/replication/      Quorum coordination and read repair
internal/httpapi/          Public/internal endpoints and embedded dashboard
internal/routing/          Earlier single-owner routing implementation
docs/                      Architecture, replication contract, and cluster guide
```

## Roadmap

Next: failure detection and hinted handoff, gossip membership and live
rebalancing, Merkle-tree anti-entropy, metrics/tracing, and broader property/fuzz
tests. Planned Python tooling will provide workloads, chaos experiments,
correctness checks, and plots. A small Raft-backed metadata plane comes last;
it will not sit in the normal read/write path.

Further reading: [Architecture](docs/architecture.md),
[Replication contract](docs/replication.md), and
[Local cluster guide](docs/local-cluster.md).
