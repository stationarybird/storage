// Package version implements causal records and deterministic sibling merging.
package version

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"
)

// Clock is an event vector: each mutation adds a unique component. This avoids
// reusing counters after crashes and never orders concurrent writes by wall time.
// It deliberately grows with history; causal metadata compaction is future work.
type Clock map[string]uint64

type Record struct {
	ID      string    `json:"id"`
	Clock   Clock     `json:"clock"`
	Value   string    `json:"value"`
	Deleted bool      `json:"deleted"`
	Expires time.Time `json:"expires,omitempty"`
}

func New(node, value string, deleted bool, expires time.Time, context Clock) (Record, error) {
	if err := ValidateClock(context); err != nil {
		return Record{}, err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return Record{}, err
	}
	id := node + ":" + hex.EncodeToString(random[:])
	clock := Context([]Record{{Clock: context}})
	clock[id] = 1
	return Record{ID: id, Clock: clock, Value: value, Deleted: deleted, Expires: expires}, nil
}

func ValidateClock(c Clock) error {
	for id, n := range c {
		if id == "" || n != 1 {
			return fmt.Errorf("invalid causal context")
		}
	}
	return nil
}

func Validate(r Record) error {
	if r.ID == "" || r.Clock[r.ID] != 1 {
		return fmt.Errorf("invalid record identity")
	}
	if r.Deleted && (r.Value != "" || !r.Expires.IsZero()) {
		return fmt.Errorf("invalid tombstone")
	}
	return ValidateClock(r.Clock)
}

func Context(records []Record) Clock {
	c := Clock{}
	for _, r := range records {
		for id, n := range r.Clock {
			if n > c[id] {
				c[id] = n
			}
		}
	}
	return c
}

func dominates(a, b Clock) bool {
	if len(a) <= len(b) {
		return false
	}
	for id, n := range b {
		if a[id] < n {
			return false
		}
	}
	return true
}

// Merge keeps causally maximal records. Tombstones and expired records are
// retained as causal barriers; filtering them out before merging resurrects data.
func Merge(groups ...[]Record) ([]Record, error) {
	byID := map[string]Record{}
	for _, group := range groups {
		for _, r := range group {
			if err := Validate(r); err != nil {
				return nil, err
			}
			if old, ok := byID[r.ID]; ok && (old.Value != r.Value || old.Deleted != r.Deleted || !old.Expires.Equal(r.Expires) || !reflect.DeepEqual(old.Clock, r.Clock)) {
				return nil, fmt.Errorf("record identity reused with different contents")
			}
			byID[r.ID] = r
		}
	}
	result := []Record{}
	for _, r := range byID {
		obsolete := false
		for _, other := range byID {
			if dominates(other.Clock, r.Clock) {
				obsolete = true
				break
			}
		}
		if !obsolete {
			result = append(result, r)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	// Give storage and callers independent slices and maps.
	data, _ := json.Marshal(result)
	var copy []Record
	_ = json.Unmarshal(data, &copy)
	return copy, nil
}

func (r Record) Live(now time.Time) bool {
	return !r.Deleted && (r.Expires.IsZero() || now.Before(r.Expires))
}
