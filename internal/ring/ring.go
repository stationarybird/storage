// Package ring calculates which nodes own a key. It does not store data or
// send requests. Nodes must use the same membership and virtual-node count.
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

type point struct {
	position uint64
	nodeID   string
	index    int
}

// Ring is an immutable placement table, safe for concurrent lookups. Construct
// a new ring when membership changes; moving existing data is a separate task.
type Ring struct {
	points []point
	nodes  int
}

// Position exposes a copy of a virtual node's position for visualization.
// Encode uint64 as a string so browser number rounding cannot alter it.
type Position struct {
	NodeID   string `json:"node_id"`
	Position uint64 `json:"position,string"`
}

func (r *Ring) Positions() []Position {
	positions := make([]Position, len(r.points))
	for i, p := range r.points {
		positions[i] = Position{NodeID: p.nodeID, Position: p.position}
	}
	return positions
}

func KeyPosition(key string) uint64 { return hash("key:" + key) }

// New gives each unique node ID virtualNodes positions on the circle.
// Use 1 to explore the algorithm; larger counts spread ownership more evenly.
// Empty membership is allowed. Empty or duplicate IDs and nonpositive counts
// are rejected. Addresses should be maintained separately from stable node IDs.
func New(nodeIDs []string, virtualNodes int) (*Ring, error) {
	if virtualNodes <= 0 {
		return nil, fmt.Errorf("virtual-node count must be positive")
	}
	seen := make(map[string]bool, len(nodeIDs))
	r := &Ring{nodes: len(nodeIDs)}
	for _, id := range nodeIDs {
		if id == "" || seen[id] {
			return nil, fmt.Errorf("empty or duplicate node ID %q", id)
		}
		seen[id] = true
		for i := 0; i < virtualNodes; i++ {
			// Length-prefix IDs to make the encoding unambiguous even if an
			// ID contains delimiters. Node and key hashes use separate domains.
			encoded := fmt.Sprintf("node:%d:%s:%d", len(id), id, i)
			r.points = append(r.points, point{position: hash(encoded), nodeID: id, index: i})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		a, b := r.points[i], r.points[j]
		if a.position != b.position {
			return a.position < b.position
		}
		// Deterministic collision handling independent of input order.
		if a.nodeID != b.nodeID {
			return a.nodeID < b.nodeID
		}
		return a.index < b.index
	})
	return r, nil
}

// Owner returns the first node clockwise from the key, wrapping at the end.
// found is false when the ring is empty.
func (r *Ring) Owner(key string) (nodeID string, found bool) {
	if len(r.points) == 0 {
		return "", false
	}
	return r.points[r.start(hash("key:"+key))].nodeID, true
}

// Replicas returns up to count distinct physical node IDs in clockwise order,
// starting with the owner. If membership is too small, callers receive fewer
// nodes and must decide whether their replication requirements can be met.
func (r *Ring) Replicas(key string, count int) []string {
	if count <= 0 || len(r.points) == 0 {
		return nil
	}
	if count > r.nodes {
		count = r.nodes
	}
	result := make([]string, 0, count)
	seen := make(map[string]bool, count)
	start := r.start(hash("key:" + key))
	for offset := 0; offset < len(r.points) && len(result) < count; offset++ {
		id := r.points[(start+offset)%len(r.points)].nodeID
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
}

func (r *Ring) start(position uint64) int {
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].position >= position })
	if i == len(r.points) {
		return 0
	}
	return i
}

func hash(value string) uint64 {
	sum := sha256.Sum256([]byte(value))
	return binary.BigEndian.Uint64(sum[:8])
}
