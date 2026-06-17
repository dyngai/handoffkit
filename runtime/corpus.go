package runtime

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/dyngai/handoffkit/sketch"
)

// MergeFunc reconciles a delta into the value stored for a key. existing is the
// current value and ok reports whether the key was already present.
//
// For concurrent Merges to be conflict-free it must be COMMUTATIVE and
// IDEMPOTENT: merge(merge(v, a), b) and merge(merge(v, b), a) must agree, so two
// agents writing the same key cannot corrupt each other regardless of arrival
// order. This is the contract that lets the Corpus be shared state without a
// per-agent lock dance (sketch/sketch.go: Corpus).
type MergeFunc func(existing any, ok bool, delta any) (merged any, err error)

// LastWriteWins replaces the stored value with the delta. It is order-dependent
// (NOT commutative), so it is correct only for a key with a single writer (for
// example a per-handoff artifact key minted by one agent). Do not use it for a
// key multiple agents Merge concurrently; use a commutative policy such as
// UnionStrings there.
func LastWriteWins(_ any, _ bool, delta any) (any, error) { return delta, nil }

// UnionStrings merges grow-only string sets (a G-Set): the result is the union
// of the existing set and the delta, sorted for a stable Get. It accepts a
// string or a []string on either side. It is commutative and idempotent, so
// concurrent Merges of the same key are conflict-free regardless of order, the
// canonical "shared knowledge, reconciled without locks" case.
func UnionStrings(existing any, ok bool, delta any) (any, error) {
	set := make(map[string]struct{})
	add := func(v any) error {
		switch t := v.(type) {
		case nil:
		case string:
			set[t] = struct{}{}
		case []string:
			for _, s := range t {
				set[s] = struct{}{}
			}
		default:
			return fmt.Errorf("handoffkit: UnionStrings needs string or []string, got %T", v)
		}
		return nil
	}
	if ok {
		if err := add(existing); err != nil {
			return nil, err
		}
	}
	if err := add(delta); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out, nil
}

// MemCorpus is an in-memory sketch.Corpus: a namespaced key/value store whose
// concurrent writes are reconciled by a MergeFunc. It is the repo's one
// deliberate use of shared state, the blackboard the design keeps for knowledge
// (knowledge stays put and is referenced, not channelled as prose). It is not a
// distributed CRDT; it is the smallest thing that makes a MemoryRef resolve and
// makes the conflict-free-merge contract concrete and testable.
type MemCorpus struct {
	mu    sync.RWMutex
	merge MergeFunc
	data  map[string]any
}

// NewCorpus returns a MemCorpus using merge to reconcile writes. A nil merge
// defaults to LastWriteWins (single-writer keys).
func NewCorpus(merge MergeFunc) *MemCorpus {
	if merge == nil {
		merge = LastWriteWins
	}
	return &MemCorpus{merge: merge, data: make(map[string]any)}
}

// refKey flattens a MemoryRef to a map key. The NUL separator cannot appear in a
// normal namespace/key, so distinct refs cannot collide.
func refKey(ref sketch.MemoryRef) string {
	return ref.Namespace + "\x00" + ref.Key
}

// cloneCorpusValue copies known mutable values before they cross the Corpus
// boundary. Unknown values keep their normal interface semantics.
func cloneCorpusValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return append([]byte(nil), t...)
	case []string:
		return append([]string(nil), t...)
	case []sketch.Turn:
		return append([]sketch.Turn(nil), t...)
	case []sketch.MemoryRef:
		return append([]sketch.MemoryRef(nil), t...)
	case sketch.HandoffContext:
		t.Thread = append([]sketch.Turn(nil), t.Thread...)
		t.Refs = append([]sketch.MemoryRef(nil), t.Refs...)
		return t
	default:
		return v
	}
}

// Get returns the value stored at ref. ok is false if the ref was never written.
func (c *MemCorpus) Get(_ context.Context, ref sketch.MemoryRef) (any, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.data[refKey(ref)]
	if !ok {
		return nil, false, nil
	}
	return cloneCorpusValue(v), true, nil
}

// Merge reconciles delta into ref via the corpus MergeFunc. Concurrent Merges
// are serialized under the lock; the conflict-free property comes from the
// MergeFunc being commutative, not from the lock (the lock only guards the map).
func (c *MemCorpus) Merge(_ context.Context, ref sketch.MemoryRef, delta any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := refKey(ref)
	existing, ok := c.data[k]
	merged, err := c.merge(cloneCorpusValue(existing), ok, cloneCorpusValue(delta))
	if err != nil {
		return err
	}
	c.data[k] = cloneCorpusValue(merged)
	return nil
}

var _ sketch.Corpus = (*MemCorpus)(nil)
