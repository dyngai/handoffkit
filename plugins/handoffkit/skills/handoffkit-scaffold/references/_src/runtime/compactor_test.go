package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/dyngai/handoffkit/sketch"
)

// The headline property: a huge output produces a handoff whose Summary is
// within budget, while the full output stays resolvable through the ref. Bounded
// AND lossless-at-a-pointer, the answer to the lossy-handoff open problem.
func TestCompactor_BoundsSummaryAndKeepsFullOutputResolvable(t *testing.T) {
	const maxSummary = 200
	full := strings.Repeat("A", 5000)

	corpus := NewCorpus(nil)
	c := NewCompactor(corpus, CompactPolicy{MaxSummaryBytes: maxSummary}, nil)
	ref := sketch.MemoryRef{Namespace: "handoff", Key: "step-1"}

	hc, err := c.Compact(context.Background(), ref, WorkingState{Output: full})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// The Summary is bounded.
	if len(hc.Summary) > maxSummary {
		t.Fatalf("Summary is %d bytes, want <= %d (handoff not bounded)", len(hc.Summary), maxSummary)
	}
	// The handoff references the full output.
	if len(hc.Refs) != 1 || hc.Refs[0] != ref {
		t.Fatalf("Refs = %v, want [%v]", hc.Refs, ref)
	}
	// And that ref resolves to the WHOLE output, not the truncated summary.
	v, ok, _ := corpus.Get(context.Background(), ref)
	if !ok || v.(string) != full {
		t.Fatalf("corpus did not retain the full output (ok=%v, len=%d, want %d)", ok, len(v.(string)), len(full))
	}
}

// An output already within budget is carried verbatim (no needless truncation),
// and the ref is still present for a uniform resolution path.
func TestCompactor_SmallOutputPassesThrough(t *testing.T) {
	corpus := NewCorpus(nil)
	c := NewCompactor(corpus, CompactPolicy{MaxSummaryBytes: 1000}, nil)
	ref := sketch.MemoryRef{Namespace: "handoff", Key: "step-1"}

	hc, err := c.Compact(context.Background(), ref, WorkingState{Output: "short"})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if hc.Summary != "short" {
		t.Fatalf("Summary = %q, want %q (small output should pass through)", hc.Summary, "short")
	}
	if len(hc.Refs) != 1 {
		t.Fatalf("Refs = %v, want one ref even for a small output", hc.Refs)
	}
}

// Only the last KeepThreadTurns turns survive verbatim, in order.
func TestCompactor_KeepsTrailingThreadTurns(t *testing.T) {
	corpus := NewCorpus(nil)
	c := NewCompactor(corpus, CompactPolicy{MaxSummaryBytes: 100, KeepThreadTurns: 2}, nil)
	ref := sketch.MemoryRef{Namespace: "handoff", Key: "step-1"}

	thread := []sketch.Turn{
		{Role: "user", Content: "t0"},
		{Role: "assistant", Content: "t1"},
		{Role: "user", Content: "t2"},
		{Role: "assistant", Content: "t3"},
		{Role: "user", Content: "t4"},
	}
	hc, err := c.Compact(context.Background(), ref, WorkingState{Output: "out", Thread: thread})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if len(hc.Thread) != 2 {
		t.Fatalf("kept %d turns, want 2", len(hc.Thread))
	}
	if hc.Thread[0].Content != "t3" || hc.Thread[1].Content != "t4" {
		t.Fatalf("kept turns = %q,%q, want t3,t4 (must keep the TRAILING turns)", hc.Thread[0].Content, hc.Thread[1].Content)
	}
}

// A Compactor with no corpus cannot offload, so Compact errors rather than
// silently dropping the full output.
func TestCompactor_NilCorpusErrors(t *testing.T) {
	c := NewCompactor(nil, CompactPolicy{MaxSummaryBytes: 100}, nil)
	_, err := c.Compact(context.Background(), sketch.MemoryRef{Namespace: "h", Key: "k"}, WorkingState{Output: "x"})
	if err == nil {
		t.Fatal("Compact with a nil corpus should error")
	}
}
