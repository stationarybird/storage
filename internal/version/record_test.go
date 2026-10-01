package version

import (
	"reflect"
	"testing"
	"time"
)

func TestCausalMerge(t *testing.T) {
	a, _ := New("a", "first", false, time.Time{}, nil)
	b, _ := New("b", "second", false, time.Time{}, a.Clock)
	c, _ := New("c", "concurrent", false, time.Time{}, a.Clock)
	merged, err := Merge([]Record{a, b, c, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(merged) != 2 {
		t.Fatalf("lost sibling: %+v", merged)
	}
	reverse, err := Merge([]Record{c, b, a})
	if err != nil || !reflect.DeepEqual(merged, reverse) {
		t.Fatal("merge not commutative")
	}
	d, _ := New("a", "", true, time.Time{}, Context(merged))
	merged, err = Merge(merged, []Record{d, a})
	if err != nil || len(merged) != 1 || !merged[0].Deleted {
		t.Fatal("delete did not dominate observed versions")
	}
	resolved, _ := New("b", "new", false, time.Time{}, d.Clock)
	merged, err = Merge(merged, []Record{resolved})
	if err != nil || len(merged) != 1 || merged[0].Deleted {
		t.Fatal("cannot recreate deleted key")
	}
}

func TestExpiryKeepsCausalBarrier(t *testing.T) {
	a, _ := New("a", "old", false, time.Time{}, nil)
	b, _ := New("a", "expired", false, time.Now().Add(-time.Hour), a.Clock)
	merged, err := Merge([]Record{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(merged) != 1 || merged[0].Live(time.Now()) {
		t.Fatal("expired update resurrected old value")
	}
	merged[0].Clock["mutated"] = 1
	if _, ok := b.Clock["mutated"]; ok {
		t.Fatal("merge aliases clock")
	}
}
