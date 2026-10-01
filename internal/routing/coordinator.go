// Package routing forwards public KV requests to a single consistent-hash owner.
package routing

import (
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
)

type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type Coordinator struct {
	id           string
	nodes        []Node
	addresses    map[string]string
	ring         *ring.Ring
	virtualNodes int
	fingerprint  string
	local        http.Handler
	client       *http.Client
}

func New(id string, peers map[string]string, virtualNodes int, local http.Handler) (*Coordinator, error) {
	if _, ok := peers[id]; !ok {
		return nil, fmt.Errorf("node %q is absent from membership", id)
	}
	c := &Coordinator{id: id, virtualNodes: virtualNodes, addresses: make(map[string]string), local: local,
		client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	ids := make([]string, 0, len(peers))
	for node, address := range peers {
		u, err := url.Parse(address)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid address for %q: use an HTTP origin", node)
		}
		ids = append(ids, node)
		c.addresses[node] = strings.TrimRight(address, "/")
	}
	sort.Strings(ids)
	for _, node := range ids {
		c.nodes = append(c.nodes, Node{node, c.addresses[node]})
	}
	var err error
	c.ring, err = ring.New(ids, virtualNodes)
	if err != nil {
		return nil, err
	}
	encoded, _ := json.Marshal(struct {
		Nodes        []Node
		VirtualNodes int
	}{c.nodes, virtualNodes})
	sum := sha256.Sum256(encoded)
	c.fingerprint = hex.EncodeToString(sum[:])
	return c, nil
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (c *Coordinator) Placement(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	owner, _ := c.ring.Owner(key)
	respond(w, 200, struct {
		Mode          string          `json:"mode"`
		ReceivingNode string          `json:"receiving_node"`
		Owner         string          `json:"owner"`
		Nodes         []Node          `json:"nodes"`
		VirtualNodes  int             `json:"virtual_nodes"`
		KeyPosition   uint64          `json:"key_position,string"`
		Positions     []ring.Position `json:"positions"`
		Replicas      []string        `json:"replicas"`
	}{"single_owner", c.id, owner, c.nodes, c.virtualNodes, ring.KeyPosition(key), c.ring.Positions(), []string{owner}})
}

// Internal never forwards. Matching membership and owner checks catch accidental
// misconfiguration. The fingerprint is not authentication; use a trusted network.
func (c *Coordinator) Internal(w http.ResponseWriter, r *http.Request) {
	owner, _ := c.ring.Owner(r.PathValue("key"))
	if r.Header.Get("X-Cluster-Config") != c.fingerprint || owner != c.id {
		respond(w, http.StatusConflict, map[string]string{"error": "membership or owner mismatch"})
		return
	}
	c.serveLocal(w, r)
}

func (c *Coordinator) serveLocal(w http.ResponseWriter, r *http.Request) {
	copy := r.Clone(r.Context())
	copy.URL.Path = "/v1/kv/" + r.PathValue("key")
	copy.URL.RawPath = "/v1/kv/" + url.PathEscape(r.PathValue("key"))
	c.local.ServeHTTP(w, copy)
}

func (c *Coordinator) Public(w http.ResponseWriter, r *http.Request) {
	owner, _ := c.ring.Owner(r.PathValue("key"))
	w.Header().Set("X-Receiving-Node", c.id)
	w.Header().Set("X-Owner-Node", owner)
	if owner == c.id {
		c.serveLocal(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	target := c.addresses[owner] + "/internal/kv/" + url.PathEscape(r.PathValue("key"))
	request, err := http.NewRequestWithContext(ctx, r.Method, target, r.Body)
	if err != nil {
		respond(w, 503, map[string]string{"error": "could not build owner request"})
		return
	}
	request.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	request.Header.Set("X-Cluster-Config", c.fingerprint)
	response, err := c.client.Do(request)
	if err != nil {
		respond(w, 503, map[string]string{"error": "owner unavailable; write outcome may be uncertain"})
		return
	}
	defer response.Body.Close()
	w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, response.Body)
}
