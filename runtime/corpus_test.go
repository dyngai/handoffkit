package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// A written ref resolves to its value; an unwritten ref reports ok == false.
func TestCorpus_RefResolvesAndMissingIsAbsent(t *testing.T) {
	c := NewCorpus(nil) // LastWriteWins
	ref := sketch.MemoryRef{Namespace: "docs", Key: "report"}

	if _, ok, _ := c.Get(context.Background(), ref); ok {
		t.Fatal("an unwritten ref should report ok=false")
	}
	if err := c.Merge(context.Background(), ref, "full text"); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	v, ok, err := c.Get(context.Background(), ref)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if v.(string) != "full text" {
		t.Fatalf("Get = %q, want %q", v, "full text")
	}
}

func TestCorpus_GetReturnsContextError(t *testing.T) {
	c := NewCorpus(nil)
	ref := sketch.MemoryRef{Namespace: "docs", Key: "report"}
	if err := c.Merge(context.Background(), ref, "full text"); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if v, ok, err := c.Get(ctx, ref); !errors.Is(err, context.Canceled) || ok || v != nil {
		t.Fatalf("Get with canceled context = (%v, %v, %v), want nil/false/context.Canceled", v, ok, err)
	}

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if _, _, err := c.Get(expired, ref); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get with expired context err = %v, want context.DeadlineExceeded", err)
	}
}

func TestCorpus_MergeReturnsContextErrorAndDoesNotMutate(t *testing.T) {
	c := NewCorpus(nil)
	ref := sketch.MemoryRef{Namespace: "docs", Key: "report"}
	missing := sketch.MemoryRef{Namespace: "docs", Key: "missing"}
	if err := c.Merge(context.Background(), ref, "original"); err != nil {
		t.Fatalf("initial Merge: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Merge(ctx, ref, "mutated"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Merge with canceled context err = %v, want context.Canceled", err)
	}
	if err := c.Merge(ctx, missing, "inserted"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Merge missing with canceled context err = %v, want context.Canceled", err)
	}

	v, ok, err := c.Get(context.Background(), ref)
	if err != nil || !ok {
		t.Fatalf("Get after canceled Merge: ok=%v err=%v", ok, err)
	}
	if v != "original" {
		t.Fatalf("canceled Merge mutated stored value: got %q, want original", v)
	}
	if _, ok, err := c.Get(context.Background(), missing); err != nil || ok {
		t.Fatalf("canceled Merge inserted missing ref: ok=%v err=%v", ok, err)
	}
}

func TestCorpus_MemoryRefKeyFieldsCannotCollide(t *testing.T) {
	c := NewCorpus(nil)
	a := sketch.MemoryRef{Namespace: "docs", Key: "report\x00draft"}
	b := sketch.MemoryRef{Namespace: "docs\x00report", Key: "draft"}

	if err := c.Merge(context.Background(), a, "value-a"); err != nil {
		t.Fatalf("Merge(a): %v", err)
	}
	if err := c.Merge(context.Background(), b, "value-b"); err != nil {
		t.Fatalf("Merge(b): %v", err)
	}

	av, ok, err := c.Get(context.Background(), a)
	if err != nil || !ok {
		t.Fatalf("Get(a): ok=%v err=%v", ok, err)
	}
	bv, ok, err := c.Get(context.Background(), b)
	if err != nil || !ok {
		t.Fatalf("Get(b): ok=%v err=%v", ok, err)
	}
	if av != "value-a" {
		t.Fatalf("Get(a) = %q, want value-a", av)
	}
	if bv != "value-b" {
		t.Fatalf("Get(b) = %q, want value-b", bv)
	}
}

// The conflict-free claim: with a commutative MergeFunc the final value does not
// depend on the order writes arrived in. Two corpora fed the same set of writes
// in different orders converge to the same union.
func TestCorpus_UnionMergeIsCommutative(t *testing.T) {
	ref := sketch.MemoryRef{Namespace: "tags", Key: "set"}
	want := []string{"x", "y", "z"}

	a := NewCorpus(UnionStrings)
	for _, s := range []string{"x", "y", "z"} {
		if err := a.Merge(context.Background(), ref, s); err != nil {
			t.Fatalf("a.Merge(%q): %v", s, err)
		}
	}
	b := NewCorpus(UnionStrings)
	for _, s := range []string{"z", "x", "y"} { // different order, same set
		if err := b.Merge(context.Background(), ref, s); err != nil {
			t.Fatalf("b.Merge(%q): %v", s, err)
		}
	}

	av, _, _ := a.Get(context.Background(), ref)
	bv, _, _ := b.Get(context.Background(), ref)
	if !reflect.DeepEqual(av, want) {
		t.Fatalf("order x,y,z converged to %v, want %v", av, want)
	}
	if !reflect.DeepEqual(bv, want) {
		t.Fatalf("order z,x,y converged to %v, want %v", bv, want)
	}
	if !reflect.DeepEqual(av, bv) {
		t.Fatalf("merge is not commutative: %v != %v (order changed the result)", av, bv)
	}
}

// Concurrent Merges of the same key are conflict-free under -race: every writer's
// value is present and none is lost, with no corruption.
func TestCorpus_ConcurrentUnionMergeIsConflictFree(t *testing.T) {
	const writers = 16
	c := NewCorpus(UnionStrings)
	ref := sketch.MemoryRef{Namespace: "tags", Key: "set"}

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = c.Merge(context.Background(), ref, fmt.Sprintf("w%02d", i))
		}(i)
	}
	wg.Wait()

	v, ok, _ := c.Get(context.Background(), ref)
	if !ok {
		t.Fatal("ref absent after concurrent merges")
	}
	got := v.([]string)
	if len(got) != writers {
		t.Fatalf("got %d values, want %d (a concurrent write was lost)", len(got), writers)
	}
}

func TestCorpus_StringSliceDoesNotLeakThroughGet(t *testing.T) {
	c := NewCorpus(nil)
	ref := sketch.MemoryRef{Namespace: "tags", Key: "set"}
	if err := c.Merge(context.Background(), ref, []string{"a", "b"}); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	v, ok, err := c.Get(context.Background(), ref)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	got := v.([]string)
	got[0] = "mutated"

	v, _, _ = c.Get(context.Background(), ref)
	if got := v.([]string); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("stored slice mutated through Get result: %v", got)
	}
}

func TestCorpus_StringSliceDoesNotLeakThroughMergeDelta(t *testing.T) {
	c := NewCorpus(nil)
	ref := sketch.MemoryRef{Namespace: "tags", Key: "set"}
	delta := []string{"a", "b"}
	if err := c.Merge(context.Background(), ref, delta); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	delta[0] = "mutated"

	v, _, _ := c.Get(context.Background(), ref)
	if got := v.([]string); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("stored slice mutated through original delta: %v", got)
	}
}

func TestCorpus_MergeFuncCannotMutateExistingOnError(t *testing.T) {
	boom := errors.New("boom")
	c := NewCorpus(func(existing any, ok bool, delta any) (any, error) {
		if ok {
			existing.([]string)[0] = "mutated"
			return nil, boom
		}
		return delta, nil
	})
	ref := sketch.MemoryRef{Namespace: "tags", Key: "set"}
	if err := c.Merge(context.Background(), ref, []string{"a", "b"}); err != nil {
		t.Fatalf("initial Merge: %v", err)
	}
	if err := c.Merge(context.Background(), ref, "c"); !errors.Is(err, boom) {
		t.Fatalf("second Merge err = %v, want boom", err)
	}

	v, _, _ := c.Get(context.Background(), ref)
	if got := v.([]string); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("stored slice mutated by failed merge: %v", got)
	}
}
