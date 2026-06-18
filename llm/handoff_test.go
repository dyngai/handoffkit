package llm

import (
	"context"
	"strings"
	"testing"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

// With no Compactor the handoff carries the full output as Summary and preserves
// inbound thread/refs: the lossy-but-heavy default behavior keeps the handoff
// trail intact while making the new output the current summary.
func TestBuildHandoff_NoCompactorShipsFullOutputAndPriorTrail(t *testing.T) {
	out := strings.Repeat("Z", 3000)
	prior := sketch.HandoffContext{
		Summary: "prior summary",
		Thread:  []sketch.Turn{{Role: "user", Content: "original task"}},
		Refs:    []sketch.MemoryRef{{Namespace: "handoff", Key: "upstream-1"}},
	}
	hc, err := buildHandoff(context.Background(), nil, "agent", 1, prior, out)
	if err != nil {
		t.Fatalf("buildHandoff: %v", err)
	}
	if hc.Summary != out {
		t.Fatalf("Summary len = %d, want full output len %d", len(hc.Summary), len(out))
	}
	if len(hc.Thread) != 1 || hc.Thread[0] != prior.Thread[0] {
		t.Fatalf("Thread = %v, want preserved prior thread %v", hc.Thread, prior.Thread)
	}
	if len(hc.Refs) != 1 || hc.Refs[0] != prior.Refs[0] {
		t.Fatalf("Refs = %v, want preserved prior refs %v", hc.Refs, prior.Refs)
	}
	hc.Thread[0] = sketch.Turn{Role: "assistant", Content: "mutated"}
	hc.Refs[0] = sketch.MemoryRef{Namespace: "x", Key: "y"}
	if prior.Thread[0].Content != "original task" || prior.Refs[0].Key != "upstream-1" {
		t.Fatal("buildHandoff aliased prior thread/refs without a compactor")
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
	if thisRef.Namespace != "handoff" || !strings.HasPrefix(thisRef.Key, "agent-7-") {
		t.Fatalf("this step's ref = %v, want readable handoff/agent-7-*", thisRef)
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

func TestBuildHandoff_CompactorRefsDoNotCollideAcrossRecreatedAgents(t *testing.T) {
	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: 20}, nil)

	first, err := buildHandoff(context.Background(), comp, "agent", 1, sketch.HandoffContext{}, "first output")
	if err != nil {
		t.Fatalf("first buildHandoff: %v", err)
	}
	second, err := buildHandoff(context.Background(), comp, "agent", 1, sketch.HandoffContext{}, "second output")
	if err != nil {
		t.Fatalf("second buildHandoff: %v", err)
	}

	if len(first.Refs) != 1 || len(second.Refs) != 1 {
		t.Fatalf("Refs = %v and %v, want one ref per handoff", first.Refs, second.Refs)
	}
	if first.Refs[0] == second.Refs[0] {
		t.Fatalf("refs collided for recreated addr/seq: %v", first.Refs[0])
	}
	for _, ref := range []sketch.MemoryRef{first.Refs[0], second.Refs[0]} {
		if ref.Namespace != "handoff" || !strings.HasPrefix(ref.Key, "agent-1-") {
			t.Fatalf("ref = %v, want readable handoff/agent-1-*", ref)
		}
	}
	v, ok, _ := corpus.Get(context.Background(), first.Refs[0])
	if !ok || v.(string) != "first output" {
		t.Fatalf("first ref resolved to %q (ok=%v), want first output", v, ok)
	}
	v, ok, _ = corpus.Get(context.Background(), second.Refs[0])
	if !ok || v.(string) != "second output" {
		t.Fatalf("second ref resolved to %q (ok=%v), want second output", v, ok)
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

func TestBuildPromptSkipsCompactedSummaryPayload(t *testing.T) {
	summary := "FULL FULL FULL ...[truncated; full text in corpus]"
	prompt := buildPrompt(sketch.Msg{
		From:    "planner",
		Payload: summary,
		Ctx: sketch.HandoffContext{
			Summary: summary,
			Refs:    []sketch.MemoryRef{{Namespace: "handoff", Key: "planner-1"}},
		},
	})
	if strings.Contains(prompt, "Task:") {
		t.Fatal("prompt appended compacted summary payload as task")
	}
	if !strings.Contains(prompt, summary) {
		t.Fatal("prompt dropped the bounded summary")
	}
}

func TestBuildPromptIncludesDistinctPayloadWithCompactedContext(t *testing.T) {
	prompt := buildPrompt(sketch.Msg{
		From:    "planner",
		Payload: "write a fresh final answer from this context",
		Ctx: sketch.HandoffContext{
			Summary: "outline summary ...[truncated; full text in corpus]",
			Refs:    []sketch.MemoryRef{{Namespace: "handoff", Key: "planner-1"}},
		},
	})
	if !strings.Contains(prompt, "Task:\nwrite a fresh final answer from this context") {
		t.Fatalf("prompt dropped distinct payload despite compacted context:\n%s", prompt)
	}
}

func TestBuildPromptIncludesDistinctPayloadWithTruncatedSummaryPrefix(t *testing.T) {
	prompt := buildPrompt(sketch.Msg{
		From:    "planner",
		Payload: "outline summary for a new task the user just asked",
		Ctx: sketch.HandoffContext{
			Summary: "outline summary ...[truncated; full text in corpus]",
			Refs:    []sketch.MemoryRef{{Namespace: "handoff", Key: "planner-1"}},
		},
	})
	if !strings.Contains(prompt, "Task:\noutline summary for a new task the user just asked") {
		t.Fatalf("prompt dropped distinct payload with truncated summary prefix:\n%s", prompt)
	}
}

func TestBuildPromptIncludesPayloadThatRepeatsPriorThreadTurn(t *testing.T) {
	const repeatedTask = "run the same task again"
	prompt := buildPrompt(sketch.Msg{
		From:    "planner",
		Payload: repeatedTask,
		Ctx: sketch.HandoffContext{
			Thread: []sketch.Turn{{Role: "user", Content: repeatedTask}},
		},
	})
	if !strings.Contains(prompt, "Task:\n"+repeatedTask) {
		t.Fatalf("prompt dropped a legitimate repeated task:\n%s", prompt)
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
	routed := outboundPayload(comp, "next", full, hc, false)
	if len(routed) > budget {
		t.Fatalf("routed payload len = %d, want <= %d", len(routed), budget)
	}
	if routed == full {
		t.Fatal("compacted routed handoff still carried the full output in Payload")
	}
	terminal := outboundPayload(comp, "", full, hc, false)
	if terminal != full {
		t.Fatal("terminal output should preserve the full model output")
	}
}

func TestOutboundPayload_CompactedRoutedFinalCanKeepFullPayload(t *testing.T) {
	const budget = 80
	full := strings.Repeat("final answer ", 300)
	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: budget}, nil)

	hc, err := buildHandoff(context.Background(), comp, "writer", 1, sketch.HandoffContext{}, full)
	if err != nil {
		t.Fatalf("buildHandoff: %v", err)
	}
	if len(hc.Summary) > budget {
		t.Fatalf("Summary is %d bytes, want <= %d", len(hc.Summary), budget)
	}
	payload := outboundPayload(comp, "out", full, hc, true)
	if payload != full {
		t.Fatalf("full-output routed payload was truncated to len %d, want full len %d", len(payload), len(full))
	}
}
