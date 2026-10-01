# Replication contract

`main` now starts the replicated coordinator. The old single-owner constructor
remains for its tests and learning examples; it is not used by the executable.

## Quorums and failures

The ring chooses N distinct replicas. The coordinator contacts all of them in
parallel, returning after R successful reads or W fsynced writes. Remaining
requests continue within a two-second deadline. Internal endpoints access their
local durable stores and check the cluster configuration fingerprint.
R/W count distinct physical nodes; the coordinator counts only if selected.
A closed or broken replica returns an error, never a successful miss.

Defaults are N=min(3,node count), R=W=floor(N/2)+1. N=3,R=W=2 tolerates one failed
replica but only a majority partition can satisfy requests. R=W=1 favors
availability on either side of a partition, accepting stale reads and conflicts.
This is fixed-set replication, not sloppy quorums: no fallback placement or
hinted handoff exists yet. R+W>N gives overlap under fixed membership, not
linearizability. A read can miss siblings present only on late responders.

Failed mutations may persist on some replicas; no rollback is attempted. Retries
create new versions. Delivery of the same internal record is idempotent, but
public request-ID deduplication is not implemented. Network work is bounded;
a stalled local filesystem sync cannot be interrupted by HTTP timeouts.
Shutdown may interrupt late repair; successful writes already reached W disks.

## Causality and conflicts

Each mutation gets a random 128-bit event ID prefixed by the coordinator ID.
Its context is an event vector: observed IDs mapped to 1, plus its new event.
This simple representation avoids counter reuse across restarts and preserves
concurrent writes even at one coordinator. It grows with key history; compact
dotted vectors and causal metadata garbage collection remain future work.
Wall-clock timestamps never choose conflict winners.

Replicas atomically merge records, keeping causally maximal versions (siblings).
GET returns `key`, `value` for a single live version, `siblings`, and `context`.
Conflicts return 409, including a concurrent live value and tombstone. If no
sibling is live, GET returns 404 with context for safe recreation.

PUT accepts `{"value":"text","ttl_ms":0,"context":{...}}`. DELETE accepts an
optional `{"context":{...}}` body. Send the context returned by GET to supersede
the versions you observed. Explicit `{}` context means an unobserved write and
can create siblings. A stale context never supersedes unobserved versions.

Omitting context triggers a quorum read before writing. This costs read latency
and requires R as well as W. Multiple observed siblings cause 409 rather than
silent resolution. Explicit context lets the caller write using W alone.
The UI remembers context from reads/conflicts and sends it with the next write.

Two PUTs with `"context":{}` create siblings. GET their combined context, then
PUT the chosen value with that context to resolve them.

## Deletes, expiry, and persistence

DELETE creates a versioned tombstone. The coordinator computes absolute expiry
once and sends identical records to every replica. Expired records remain causal
barriers. Neither expiry nor snapshots discard their metadata, because doing so
could resurrect a stale value from an offline replica. Tombstones/expired keys
continue consuming capacity; safe garbage collection is not implemented.

Sibling sets use WAL record version 2, synced before acknowledgement. Snapshots
preserve siblings, tombstones and expiry metadata. Older binaries reject the new
format rather than silently dropping metadata. There is no automatic migration:
run replicated nodes on fresh WAL paths and retain old files separately.

## Repair and limitations

After a successful read, the coordinator gathers late responses until its
deadline, merges all observed versions, and best-effort repairs every replica.
Repair merges rather than replaces, preserving unrelated concurrent writes.
An unavailable replica catches up on later reads after returning. Untouched
keys do not automatically converge: handoff and anti-entropy are future work.

All live values and retained metadata must fit in RAM. Metadata grows with
history. Internal endpoints and client contexts trust the development network;
configuration fingerprints are not authentication. Rebalancing, deduplication,
bounded tombstone retention, and periodic anti-entropy remain unimplemented.
