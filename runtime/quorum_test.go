package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

func requireNoStepOutput(t *testing.T, out []sketch.Msg, label string) {
	t.Helper()
	if len(out) != 0 {
		t.Fatalf("%s emitted unexpectedly: %#v", label, out)
	}
}

// need == 1 is a race: the quorum emits the first arrival and drops the rest of
// the round, then resets for an independent next round.
func TestQuorumAgent_FirstWinsAndDropsStragglers(t *testing.T) {
	q := NewQuorumAgent("q", nil, "out", 1, 3, nil)
	ctx := context.Background()

	out, err := q.Step(ctx, sketch.Msg{Payload: "first"})
	if err != nil {
		t.Fatalf("first step: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "first" || out[0].From != "q" || out[0].To != "out" {
		t.Fatalf("first output = %#v, want one routed first payload", out)
	}

	out, err = q.Step(ctx, sketch.Msg{Payload: "second"})
	if err != nil {
		t.Fatalf("second step: %v", err)
	}
	requireNoStepOutput(t, out, "second straggler")

	out, err = q.Step(ctx, sketch.Msg{Payload: "third"})
	if err != nil {
		t.Fatalf("third step: %v", err)
	}
	requireNoStepOutput(t, out, "third straggler")

	// The round reset: a fresh batch of three races independently.
	out, err = q.Step(ctx, sketch.Msg{Payload: "fourth"})
	if err != nil {
		t.Fatalf("fourth step: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "fourth" {
		t.Fatalf("second round output = %#v, want fourth", out)
	}

	out, err = q.Step(ctx, sketch.Msg{Payload: "fifth"})
	if err != nil {
		t.Fatalf("fifth step: %v", err)
	}
	requireNoStepOutput(t, out, "fifth straggler")

	out, err = q.Step(ctx, sketch.Msg{Payload: "sixth"})
	if err != nil {
		t.Fatalf("sixth step: %v", err)
	}
	requireNoStepOutput(t, out, "sixth straggler")
}

// need == 2 of total == 3 combines the first two arrivals and ignores the third,
// then resets.
func TestQuorumAgent_NofMCombinesFirstNThenResets(t *testing.T) {
	combine := func(batch []sketch.Msg) sketch.Msg {
		parts := make([]string, len(batch))
		for i, m := range batch {
			parts[i] = m.Payload
		}
		return sketch.Msg{Payload: strings.Join(parts, "|")}
	}

	q := NewQuorumAgent("q", nil, "out", 2, 3, combine)
	ctx := context.Background()

	out, err := q.Step(ctx, sketch.Msg{Payload: "a"})
	if err != nil {
		t.Fatalf("a step: %v", err)
	}
	requireNoStepOutput(t, out, "first of quorum")

	out, err = q.Step(ctx, sketch.Msg{Payload: "b"})
	if err != nil {
		t.Fatalf("b step: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "a|b" || out[0].From != "q" || out[0].To != "out" {
		t.Fatalf("quorum output = %#v, want one routed a|b payload", out)
	}

	out, err = q.Step(ctx, sketch.Msg{Payload: "c"})
	if err != nil {
		t.Fatalf("c step: %v", err)
	}
	requireNoStepOutput(t, out, "ignored straggler")

	out, err = q.Step(ctx, sketch.Msg{Payload: "d"})
	if err != nil {
		t.Fatalf("d step: %v", err)
	}
	requireNoStepOutput(t, out, "first of second quorum")

	out, err = q.Step(ctx, sketch.Msg{Payload: "e"})
	if err != nil {
		t.Fatalf("e step: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "d|e" {
		t.Fatalf("second round output = %#v, want d|e", out)
	}

	out, err = q.Step(ctx, sketch.Msg{Payload: "f"})
	if err != nil {
		t.Fatalf("f step: %v", err)
	}
	requireNoStepOutput(t, out, "second round straggler")
}

func TestQuorumAgent_CorrelationIDKeepsInterleavedRoundsSeparate(t *testing.T) {
	combine := func(batch []sketch.Msg) sketch.Msg {
		parts := make([]string, len(batch))
		for i, m := range batch {
			parts[i] = m.Payload
		}
		return sketch.Msg{Payload: strings.Join(parts, "|")}
	}

	q := NewQuorumAgent("q", nil, "out", 2, 3, combine)
	ctx := context.Background()

	out, err := q.Step(ctx, sketch.Msg{CorrelationID: "r1", Payload: "r1-a"})
	if err != nil {
		t.Fatalf("r1-a step: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("r1-a emitted early: %#v", out)
	}

	out, err = q.Step(ctx, sketch.Msg{CorrelationID: "r1", Payload: "r1-b"})
	if err != nil {
		t.Fatalf("r1-b step: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "r1-a|r1-b" || out[0].CorrelationID != "r1" {
		t.Fatalf("r1 quorum = %#v, want r1-a|r1-b with correlation id r1", out)
	}

	// Round 2 starts before round 1's final straggler arrives. Without a
	// correlation key this valid r2 reply is drained as r1's straggler.
	out, err = q.Step(ctx, sketch.Msg{CorrelationID: "r2", Payload: "r2-a"})
	if err != nil {
		t.Fatalf("r2-a step: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("r2-a emitted early: %#v", out)
	}

	out, err = q.Step(ctx, sketch.Msg{CorrelationID: "r1", Payload: "r1-c"})
	if err != nil {
		t.Fatalf("r1-c step: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("r1 straggler emitted: %#v", out)
	}

	out, err = q.Step(ctx, sketch.Msg{CorrelationID: "r2", Payload: "r2-b"})
	if err != nil {
		t.Fatalf("r2-b step: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "r2-a|r2-b" || out[0].CorrelationID != "r2" {
		t.Fatalf("r2 quorum = %#v, want r2-a|r2-b with correlation id r2", out)
	}
}

// need is clamped to >= 1 and total to >= need, and a nil combine joins the
// quorum payloads with newlines.
func TestQuorumAgent_Defaults(t *testing.T) {
	in := NewMailbox(1)
	out := NewMailbox(1)
	q := NewQuorumAgent("q", in, "out", 0, 0, nil) // need<1 -> 1; total<need -> 1
	r := NewRouter()
	r.Register("out", out)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, q, r, time.Second)
	}()

	mustSend(t, ctx, in, "solo")
	if got := recvPayload(t, ctx, out); got != "solo" {
		t.Fatalf("need=0 should clamp to 1 and fire per message: got %q, want solo", got)
	}

	cancel()
	wg.Wait()
}
