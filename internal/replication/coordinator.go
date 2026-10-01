// Package replication coordinates fixed replica sets with configurable quorums.
package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"distributedcache/internal/ring"
	"distributedcache/internal/version"
)

type Store interface {
	Records(string) ([]version.Record, error)
	MergeRecords(string, []version.Record) error
}

type Options struct {
	N       int
	R       int
	W       int
	Timeout time.Duration
}

func Defaults(nodes int) Options {
	n := min(3, nodes)
	return Options{N: n, R: n/2 + 1, W: n/2 + 1, Timeout: 2 * time.Second}
}

type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type Coordinator struct {
	id          string
	peers       map[string]string
	nodes       []Node
	ring        *ring.Ring
	vnodes      int
	options     Options
	store       Store
	fingerprint string
	client      *http.Client
}

func New(id string, peers map[string]string, vnodes int, options Options, store Store) (*Coordinator, error) {
	if options.N < 1 || options.N > len(peers) || options.R < 1 || options.R > options.N || options.W < 1 || options.W > options.N || options.Timeout <= 0 {
		return nil, fmt.Errorf("require 1 <= R,W <= N <= membership and positive timeout")
	}
	if _, ok := peers[id]; !ok || id == "" {
		return nil, fmt.Errorf("local node must be in membership")
	}
	c := &Coordinator{id: id, peers: map[string]string{}, vnodes: vnodes, options: options, store: store, client: &http.Client{Timeout: options.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	ids := []string{}
	for node, address := range peers {
		u, err := url.Parse(address)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid peer origin: %q", address)
		}
		c.peers[node] = strings.TrimRight(address, "/")
		ids = append(ids, node)
	}
	sort.Strings(ids)
	for _, node := range ids {
		c.nodes = append(c.nodes, Node{node, c.peers[node]})
	}
	var err error
	c.ring, err = ring.New(ids, vnodes)
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(struct {
		Protocol        int
		Nodes           []Node
		Vnodes, N, R, W int
	}{1, c.nodes, vnodes, options.N, options.R, options.W})
	hash := sha256.Sum256(data)
	c.fingerprint = hex.EncodeToString(hash[:])
	return c, nil
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func failure(w http.ResponseWriter, status int, message string) {
	respond(w, status, map[string]string{"error": message})
}

func decode(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected one JSON object")
	}
	return nil
}

func (c *Coordinator) Placement(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	replicas := c.ring.Replicas(key, c.options.N)
	respond(w, 200, struct {
		Mode      string          `json:"mode"`
		Receiving string          `json:"receiving_node"`
		Owner     string          `json:"owner"`
		Nodes     []Node          `json:"nodes"`
		Vnodes    int             `json:"virtual_nodes"`
		Position  uint64          `json:"key_position,string"`
		Positions []ring.Position `json:"positions"`
		Replicas  []string        `json:"replicas"`
		R         int             `json:"read_quorum"`
		W         int             `json:"write_quorum"`
	}{"replicated", c.id, replicas[0], c.nodes, c.vnodes, ring.KeyPosition(key), c.ring.Positions(), replicas, c.options.R, c.options.W})
}

func (c *Coordinator) Internal(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	member := false
	for _, id := range c.ring.Replicas(key, c.options.N) {
		if id == c.id {
			member = true
		}
	}
	if !member || r.Header.Get("X-Cluster-Config") != c.fingerprint {
		failure(w, 409, "replica membership/configuration mismatch")
		return
	}
	w.Header().Set("X-Replica-Node", c.id)
	if r.Method == "GET" {
		records, err := c.store.Records(key)
		if err != nil {
			failure(w, 503, err.Error())
			return
		}
		respond(w, 200, records)
		return
	}
	var records []version.Record
	if err := decode(w, r, &records); err != nil || len(records) == 0 {
		failure(w, 400, "expected versioned records")
		return
	}
	if _, err := version.Merge(records); err != nil {
		failure(w, 400, err.Error())
		return
	}
	if err := c.store.MergeRecords(key, records); err != nil {
		failure(w, 503, err.Error())
		return
	}
	w.WriteHeader(204)
}

type result struct {
	records []version.Record
	err     error
}

func (c *Coordinator) call(ctx context.Context, node, key string, records []version.Record, write bool) result {
	if node == c.id {
		if write {
			return result{err: c.store.MergeRecords(key, records)}
		}
		r, err := c.store.Records(key)
		return result{r, err}
	}
	method := "GET"
	var body io.Reader
	if write {
		method = "PUT"
		data, err := json.Marshal(records)
		if err != nil {
			return result{err: err}
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.peers[node]+"/internal/replica/"+url.PathEscape(key), body)
	if err != nil {
		return result{err: err}
	}
	req.Header.Set("X-Cluster-Config", c.fingerprint)
	req.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(req)
	if err != nil {
		return result{err: err}
	}
	defer response.Body.Close()
	want := 200
	if write {
		want = 204
	}
	if response.StatusCode != want {
		return result{err: fmt.Errorf("replica %s returned %d", node, response.StatusCode)}
	}
	if response.Header.Get("X-Replica-Node") != node {
		return result{err: fmt.Errorf("replica identity mismatch for %s", node)}
	}
	if write {
		return result{}
	}
	var out []version.Record
	if err := json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&out); err != nil {
		return result{err: err}
	}
	out, err = version.Merge(out)
	return result{out, err}
}

// Fanout continues after an early quorum response, but is bounded by Timeout.
// A failed mutation can have committed on some replicas; it is never rolled back.
func (c *Coordinator) fanout(key string, records []version.Record, write bool) (chan result, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), c.options.Timeout)
	results := make(chan result, c.options.N)
	for _, node := range c.ring.Replicas(key, c.options.N) {
		go func() { results <- c.call(ctx, node, key, records, write) }()
	}
	return results, ctx, cancel
}

func mergeResults(results []result) ([]version.Record, error) {
	groups := [][]version.Record{}
	for _, r := range results {
		if r.err == nil {
			groups = append(groups, r.records)
		}
	}
	return version.Merge(groups...)
}

func (c *Coordinator) quorum(ctx context.Context, key string, records []version.Record, write bool) ([]version.Record, error) {
	results, work, cancel := c.fanout(key, records, write)
	needed := c.options.R
	if write {
		needed = c.options.W
	}
	all := []result{}
	success := 0
	for len(all) < c.options.N {
		select {
		case <-ctx.Done():
			cancel()
			return nil, ctx.Err()
		case <-work.Done():
			cancel()
			return nil, fmt.Errorf("quorum unavailable; mutation outcome may be uncertain")
		case result := <-results:
			all = append(all, result)
			if result.err == nil {
				success++
			}
			if success >= needed {
				merged, err := mergeResults(all)
				// Drain late responses and reconcile the complete observed set.
				go c.finish(key, results, work, cancel, all, write)
				return merged, err
			}
		}
	}
	cancel()
	return nil, fmt.Errorf("quorum unavailable; mutation outcome may be uncertain")
}

func (c *Coordinator) finish(key string, results chan result, ctx context.Context, cancel context.CancelFunc, all []result, write bool) {
	defer cancel()
	for len(all) < c.options.N {
		select {
		case r := <-results:
			all = append(all, r)
		case <-ctx.Done():
			goto repair
		}
	}
repair:
	if write {
		return
	}
	merged, err := mergeResults(all)
	if err != nil || len(merged) == 0 {
		return
	}
	// Repair uses merge, never replacement: a concurrent write cannot be lost.
	repairCtx, stop := context.WithTimeout(context.Background(), c.options.Timeout)
	defer stop()
	done := make(chan struct{}, c.options.N)
	for _, node := range c.ring.Replicas(key, c.options.N) {
		go func() { c.call(repairCtx, node, key, merged, true); done <- struct{}{} }()
	}
	for range c.options.N {
		select {
		case <-done:
		case <-repairCtx.Done():
			return
		}
	}
}

type readResponse struct {
	Key      string           `json:"key"`
	Value    *string          `json:"value,omitempty"`
	Context  version.Clock    `json:"context"`
	Siblings []version.Record `json:"siblings"`
	Error    string           `json:"error,omitempty"`
}

func (c *Coordinator) Public(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	replicas := c.ring.Replicas(key, c.options.N)
	w.Header().Set("X-Receiving-Node", c.id)
	w.Header().Set("X-Owner-Node", replicas[0])
	w.Header().Set("X-Replica-Nodes", strings.Join(replicas, ","))
	if r.Method == "GET" {
		records, err := c.quorum(r.Context(), key, nil, false)
		if err != nil {
			failure(w, 503, err.Error())
			return
		}
		out := readResponse{Key: key, Context: version.Context(records), Siblings: records}
		live := []version.Record{}
		for _, record := range records {
			if record.Live(time.Now()) {
				live = append(live, record)
			}
		}
		if len(live) == 0 {
			out.Error = "key not found"
			respond(w, 404, out)
			return
		}
		// A live value concurrent with a delete/expiry is still a conflict.
		if len(records) > 1 {
			out.Error = "concurrent versions; resolve using returned context"
			respond(w, 409, out)
			return
		}
		out.Value = &live[0].Value
		respond(w, 200, out)
		return
	}
	var input struct {
		Value   *string       `json:"value"`
		TTL     int64         `json:"ttl_ms"`
		Context version.Clock `json:"context"`
	}
	if r.Method == "PUT" || r.ContentLength != 0 {
		if err := decode(w, r, &input); err != nil {
			failure(w, 400, "invalid JSON mutation")
			return
		}
	}
	if (r.Method == "PUT" && input.Value == nil) || input.TTL < 0 || input.TTL > int64(time.Duration(1<<63-1)/time.Millisecond) {
		failure(w, 400, "value required and ttl_ms must be a non-negative duration")
		return
	}
	if err := version.ValidateClock(input.Context); err != nil {
		failure(w, 400, err.Error())
		return
	}
	if input.Context == nil {
		existing, err := c.quorum(r.Context(), key, nil, false)
		if err != nil {
			failure(w, 503, err.Error())
			return
		}
		if len(existing) > 1 {
			respond(w, 409, readResponse{Key: key, Context: version.Context(existing), Siblings: existing, Error: "explicit context required to resolve siblings"})
			return
		}
		input.Context = version.Context(existing)
	}
	var expires time.Time
	if r.Method == "PUT" && input.TTL > 0 {
		expires = time.Now().Add(time.Duration(input.TTL) * time.Millisecond)
	}
	value := ""
	if r.Method == "PUT" {
		value = *input.Value
	}
	record, err := version.New(c.id, value, r.Method == "DELETE", expires, input.Context)
	if err != nil {
		failure(w, 500, err.Error())
		return
	}
	if _, err := c.quorum(r.Context(), key, []version.Record{record}, true); err != nil {
		failure(w, 503, err.Error())
		return
	}
	w.WriteHeader(204)
}
