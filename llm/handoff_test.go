package llm

import (
	"context"
	"strings"
	"testing"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

// With no Compactor the handoff carries the full output as Summary and no refs:
// the lossy-but-heavy default behavior is preserved.
func TestBuildHandoff_NoCompactorShipsFullOutput(t *testing.T) {
	out := strings.Repeat("Z", 3000)
	hc, err := buildHandoff(context.Background(), nil, "agent", 1, sketch.HandoffContext{}, out)
	if err != nil {
		t.Fatalf("buildHandoff: %v", err)
	}
	if hc.Summary != out {
		t.Fatalf("Summary len = %d, want full output len %d", len(hc.Summary), len(out))
	}
	if len(hc.Refs) != 0 {
		t.Fatalf("Refs = %v, want none without a compactor", hc.Refs)
	}
}

// With a Compactor the handoff is bounded, the full output is offloaded to the
// corpus and resolvable, and inbound (prior) refs accumulate ahead of this
// step's ref.
func TestBuildHandoff_CompactorBoundsAndAccumulatesRefs(t *testing.T) {
	const budget = 200
	out := strings.Repeat("Z", 3000)

	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: budget}, nil)
	prior := []sketch.MemoryRef{{Namespace: "handoff", Key: "upstream-1"}}

	hc, err := buildHandoff(context.Background(), comp, "agent", 7, sketch.HandoffContext{Refs: prior}, out)
	if err != nil {
		t.Fatalf("buildHandoff: %v", err)
	}

	// Bounded summary.
	if len(hc.Summary) > budget {
		t.Fatalf("Summary is %d bytes, want <= %d (compactor not applied)", len(hc.Summary), budget)
	}
	// Prior ref carried, then this step's ref appended.
	if len(hc.Refs) != 2 {
		t.Fatalf("Refs = %v, want 2 (prior + this step)", hc.Refs)
	}
	if hc.Refs[0] != prior[0] {
		t.Fatalf("Refs[0] = %v, want the carried prior ref %v", hc.Refs[0], prior[0])
	}
	thisRef := hc.Refs[1]
	if thisRef.Namespace != "handoff" || thisRef.Key != "agent-7" {
		t.Fatalf("this step's ref = %v, want handoff/agent-7", thisRef)
	}
	// The full output is resolvable from the corpus.
	v, ok, _ := corpus.Get(context.Background(), thisRef)
	if !ok || v.(string) != out {
		t.Fatalf("corpus did not retain the full output (ok=%v)", ok)
	}
}

// The accumulated refs must not alias the caller's inbound slice.
func TestBuildHandoff_DoesNotAliasPriorRefs(t *testing.T) {
	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: 50}, nil)
	prior := []sketch.MemoryRef{{Namespace: "handoff", Key: "upstream-1"}}

	hc, err := buildHandoff(context.Background(), comp, "agent", 1, sketch.HandoffContext{Refs: prior}, "output")
	if err != nil {
		t.Fatalf("buildHandoff: %v", err)
	}
	// Mutating the returned refs must not corrupt the caller's prior slice.
	hc.Refs[0] = sketch.MemoryRef{Namespace: "x", Key: "y"}
	if prior[0].Key != "upstream-1" {
		t.Fatal("buildHandoff aliased the caller's prior refs slice")
	}
}

func TestBuildPromptIncludesThread(t *testing.T) {
	prompt := buildPrompt(sketch.Msg{
		From:    "planner",
		Payload: "write the final",
		Ctx: sketch.HandoffContext{
			Summary: "outline summary",
			Thread: []sketch.Turn{
				{Role: "user", Content: "original task"},
				{Role: "assistant", Content: "draft outline"},
			},
		},
	})

	for _, want := range []string{
		"Context handed from planner:\noutline summary",
		"Recent thread handed from planner:",
		"user:\noriginal task",
		"assistant:\ndraft outline",
		"Task:\nwrite the final",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestBuildPromptSkipsDuplicateSummaryPayload(t *testing.T) {
	prompt := buildPrompt(sketch.Msg{
		From:    "planner",
		Payload: "bounded summary",
		Ctx:     sketch.HandoffContext{Summary: "bounded summary"},
	})
	if strings.Contains(prompt, "Task:") {
		t.Fatalf("duplicate summary payload should not be appended as task:\n%s", prompt)
	}
}

func TestBuildPromptSkipsStaleFullPayloadForCompactedHandoff(t *testing.T) {
	full := strings.Repeat("FULL ", 200)
	prompt := buildPrompt(sketch.Msg{
		From:    "planner",
		Payload: full,
		Ctx: sketch.HandoffContext{
			Summary: "bounded summary",
			Refs:    []sketch.MemoryRef{{Namespace: "handoff", Key: "planner-1"}},
		},
	})
	if strings.Contains(prompt, full) {
		t.Fatal("prompt included stale full Payload despite compacted handoff context")
	}
	if !strings.Contains(prompt, "bounded summary") {
		t.Fatal("prompt dropped the bounded summary")
	}
}

func TestOutboundPayload_CompactedRoutedHandoffIsBounded(t *testing.T) {
	const budget = 80
	full := strings.Repeat("Z", 3000)
	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: budget}, nil)

	hc, err := buildHandoff(context.Background(), comp, "agent", 1, sketch.HandoffContext{}, full)
	if err != nil {
		t.Fatalf("buildHandoff: %v", err)
	}
	routed := outboundPayload(comp, "next", full, hc)
	if len(routed) > budget {
		t.Fatalf("routed payload len = %d, want <= %d", len(routed), budget)
	}
	if routed == full {
		t.Fatal("compacted routed handoff still carried the full output in Payload")
	}
	terminal := outboundPayload(comp, "", full, hc)
	if terminal != full {
		t.Fatal("terminal output should preserve the full model output")
	}
}
