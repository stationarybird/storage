# Run three local nodes

Build once from the project root:

```sh
go build -o bin/cache-node .
mkdir -p data/a data/b data/c
```

In each of three terminals, set the same membership:

```sh
export CACHE_PEERS='{"a":"http://localhost:8080","b":"http://localhost:8081","c":"http://localhost:8082"}'
```

Then run one command per terminal:

```sh
CACHE_NODE_ID=a CACHE_LISTEN_ADDR=:8080 CACHE_WAL_PATH=data/a/replicated.wal ./bin/cache-node
```

```sh
CACHE_NODE_ID=b CACHE_LISTEN_ADDR=:8081 CACHE_WAL_PATH=data/b/replicated.wal ./bin/cache-node
```

```sh
CACHE_NODE_ID=c CACHE_LISTEN_ADDR=:8082 CACHE_WAL_PATH=data/c/replicated.wal ./bin/cache-node
```

Open http://localhost:8080/ and http://localhost:8082/. Put a key in one tab and
Get the same key in the other. The response shows the coordinator and replicas.
The ring panel displays actual configured placement, not a liveness check.

```sh
curl -i -X PUT http://localhost:8080/v1/kv/hello -d '{"value":"world"}'
curl -i http://localhost:8082/v1/kv/hello
curl 'http://localhost:8081/debug/cluster?key=hello'
```

Defaults are N=3 replicas, R=2 reads, W=2 durable writes. Override with
`CACHE_REPLICAS`, `CACHE_READ_QUORUM`, and `CACHE_WRITE_QUORUM`, identically on all
nodes. N defaults to min(3, membership size); R/W default to a majority of N.

Stop C with Ctrl-C and Put/Get through A and B: a quorum is still available.
Restart C with its same WAL and read the key to trigger repair. With two nodes
down, operations return 503. A failed write may have committed partially.

Internal requests use `/internal/replica/{key}` and do not forward. The cluster
fingerprint is not authentication; this is for a trusted development network.
See [replication.md](replication.md) for conflict resolution and guarantees.

Keep membership and addresses identical and fixed across nodes. Changing them
does not migrate old data. Use fresh WAL paths for the cluster demo; earlier
single-node data is not automatically redistributed or converted. Keep the old
files; the new server rejects unversioned keys and uses fresh `replicated.wal`
paths in this example. Stop each node with Ctrl-C
to create its own snapshot. `go run .` without configuration still runs a
single node at localhost:8080 with N=R=W=1 and `replicated.wal`.
