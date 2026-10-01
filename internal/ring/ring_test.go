package ring

import (
	"fmt"
	"reflect"
	"testing"
)

func mustRing(t *testing.T, ids []string, virtualNodes int) *Ring {
	t.Helper()
	r, err := New(ids, virtualNodes)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestClockwiseAndWraparound(t *testing.T) {
	r := &Ring{points: []point{{position: 20}, {position: 50}, {position: 80}}}
	for _, tc := range []struct {
		position uint64
		want     int
	}{
		{0, 0}, {20, 0}, {37, 1}, {50, 1}, {79, 2}, {80, 2}, {92, 0},
	} {
		if got := r.start(tc.position); got != tc.want {
			t.Fatalf("position %d: got %d, want %d", tc.position, got, tc.want)
		}
	}
}

func TestDeterministicDistinctReplicas(t *testing.T) {
	a := mustRing(t, []string{"a", "b", "c"}, 64)
	b := mustRing(t, []string{"c", "a", "b"}, 64)
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		replicas := a.Replicas(key, 5)
		if !reflect.DeepEqual(replicas, b.Replicas(key, 5)) {
			t.Fatal("placement depends on membership order")
		}
		seen := map[string]bool{}
		for _, id := range replicas {
			if seen[id] {
				t.Fatal("duplicate physical replica")
			}
			seen[id] = true
		}
		if len(seen) != 3 {
			t.Fatal("missing replicas")
		}
		owner, ok := a.Owner(key)
		if !ok || owner != replicas[0] {
			t.Fatal("first replica is not owner")
		}
		if got := a.Replicas(key, 2); !reflect.DeepEqual(got, replicas[:2]) {
			t.Fatal("replica prefix changed")
		}
	}
}

func TestMembershipMovesOnlyAffectedKeys(t *testing.T) {
	old := mustRing(t, []string{"a", "b", "c"}, 64)
	added := mustRing(t, []string{"a", "b", "c", "d"}, 64)
	removed := mustRing(t, []string{"a", "c"}, 64)
	moved := 0
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		before, _ := old.Owner(key)
		after, _ := added.Owner(key)
		if before != after {
			moved++
			if after != "d" {
				t.Fatal("adding d moved key between existing nodes")
			}
		}
		afterRemoval, _ := removed.Owner(key)
		if before != "b" && before != afterRemoval {
			t.Fatal("removal moved an unaffected key")
		}
	}
	if moved == 0 || moved == 10000 {
		t.Fatalf("unexpected moved count: %d", moved)
	}
}

func TestEmptySingleAndValidation(t *testing.T) {
	empty := mustRing(t, nil, 1)
	if _, ok := empty.Owner("x"); ok {
		t.Fatal("empty ring has owner")
	}
	if len(empty.Replicas("x", 3)) != 0 {
		t.Fatal("empty ring has replicas")
	}
	single := mustRing(t, []string{"only"}, 16)
	if got := single.Replicas("", 3); !reflect.DeepEqual(got, []string{"only"}) {
		t.Fatalf("replicas: %v", got)
	}
	for _, count := range []int{0, -1} {
		if len(single.Replicas("x", count)) != 0 {
			t.Fatal("nonpositive replica count")
		}
	}
	for _, tc := range []struct {
		ids   []string
		count int
	}{
		{[]string{""}, 1}, {[]string{"a", "a"}, 1}, {nil, 0}, {nil, -1},
	} {
		if _, err := New(tc.ids, tc.count); err == nil {
			t.Fatal("accepted invalid configuration")
		}
	}
}
