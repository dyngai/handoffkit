package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/dyngai/handoffkit/sketch"
)

// The barrier stays closed until every dependency arrives, fires with the whole
// batch, then resets for the next batch.
func TestJoinAgent_WaitsForAllThenResets(t *testing.T) {
	combine := func(batch []sketch.Msg) sketch.Msg {
		parts := make([]string, len(batch))
		for i, m := range batch {
			parts[i] = m.Payload
		}
		return sketch.Msg{Payload: strings.Join(parts, "|")}
	}

	join := NewJoinAgent("join", nil, "out", 3, combine)
	ctx := context.Background()

	// Two of three dependencies: the barrier must stay closed (no output yet).
	out, err := join.Step(ctx, sketch.Msg{Payload: "a"})
	if err != nil {
		t.Fatalf("step a: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("step a emitted early: %#v", out)
	}
	out, err = join.Step(ctx, sketch.Msg{Payload: "b"})
	if err != nil {
		t.Fatalf("step b: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("step b emitted early: %#v", out)
	}

	// Third arrives: the barrier fires with the whole batch.
	out, err = join.Step(ctx, sketch.Msg{Payload: "c"})
	if err != nil {
		t.Fatalf("step c: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "a|b|c" {
		t.Fatalf("first batch = %#v, want one message with payload a|b|c", out)
	}

	// It resets: a second batch of three joins independently.
	for _, payload := range []string{"d", "e"} {
		out, err = join.Step(ctx, sketch.Msg{Payload: payload})
		if err != nil {
			t.Fatalf("step %s: %v", payload, err)
		}
		if len(out) != 0 {
			t.Fatalf("step %s emitted early: %#v", payload, out)
		}
	}
	out, err = join.Step(ctx, sketch.Msg{Payload: "f"})
	if err != nil {
		t.Fatalf("step f: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "d|e|f" {
		t.Fatalf("second batch = %#v, want one message with payload d|e|f", out)
	}
}

// need is clamped to >= 1, and a nil combine joins payloads with newlines.
func TestJoinAgent_Defaults(t *testing.T) {
	join := NewJoinAgent("join", nil, "out", 0, nil) // need<1 -> 1; nil combine
	ctx := context.Background()

	out, err := join.Step(ctx, sketch.Msg{Payload: "solo"})
	if err != nil {
		t.Fatalf("step solo: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "solo" {
		t.Fatalf("need=0 should fire per message: got %#v, want one message with payload solo", out)
	}
}

func TestJoinAgent_CorrelationIDKeepsInterleavedRoundsSeparate(t *testing.T) {
	combine := func(batch []sketch.Msg) sketch.Msg {
		parts := make([]string, len(batch))
		for i, m := range batch {
			parts[i] = m.Payload
		}
		return sketch.Msg{Payload: strings.Join(parts, "|")}
	}

	join := NewJoinAgent("join", nil, "out", 2, combine)
	ctx := context.Background()

	out, err := join.Step(ctx, sketch.Msg{CorrelationID: "r1", Payload: "r1-a"})
	if err != nil {
		t.Fatalf("r1-a step: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("r1-a emitted early: %#v", out)
	}

	out, err = join.Step(ctx, sketch.Msg{CorrelationID: "r2", Payload: "r2-a"})
	if err != nil {
		t.Fatalf("r2-a step: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("r2-a emitted early: %#v", out)
	}

	out, err = join.Step(ctx, sketch.Msg{CorrelationID: "r1", Payload: "r1-b"})
	if err != nil {
		t.Fatalf("r1-b step: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "r1-a|r1-b" || out[0].CorrelationID != "r1" {
		t.Fatalf("r1 join = %#v, want r1-a|r1-b with correlation id r1", out)
	}

	out, err = join.Step(ctx, sketch.Msg{CorrelationID: "r2", Payload: "r2-b"})
	if err != nil {
		t.Fatalf("r2-b step: %v", err)
	}
	if len(out) != 1 || out[0].Payload != "r2-a|r2-b" || out[0].CorrelationID != "r2" {
		t.Fatalf("r2 join = %#v, want r2-a|r2-b with correlation id r2", out)
	}
}

func TestJoinAgent_EmitsExpectedEnvelopeAndPayload(t *testing.T) {
	combine := func(batch []sketch.Msg) sketch.Msg {
		return sketch.Msg{
			From:    "combine-from",
			To:      "combine-to",
			Payload: batch[0].Payload + "+" + batch[1].Payload,
		}
	}

	join := NewJoinAgent("join", nil, "out", 2, combine)
	ctx := context.Background()

	out, err := join.Step(ctx, sketch.Msg{
		From:          "worker-a",
		To:            "join",
		CorrelationID: "round",
		Payload:       "a",
	})
	if err != nil {
		t.Fatalf("first step: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("first step emitted early: %#v", out)
	}

	out, err = join.Step(ctx, sketch.Msg{
		From:          "worker-b",
		To:            "join",
		CorrelationID: "round",
		Payload:       "b",
	})
	if err != nil {
		t.Fatalf("second step: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("second step emitted %#v, want exactly one message", out)
	}
	got := out[0]
	if got.From != "join" || got.To != "out" || got.Payload != "a+b" {
		t.Fatalf("emitted envelope = %#v, want From join, To out, Payload a+b", got)
	}
}

func mustSend(t *testing.T, ctx context.Context, mb sketch.Mailbox, payload string) {
	t.Helper()
	if err := mb.Send(ctx, sketch.Msg{Payload: payload}); err != nil {
		t.Fatalf("send %q: %v", payload, err)
	}
}

func recvPayload(t *testing.T, ctx context.Context, mb *ChanMailbox) string {
	t.Helper()
	m, ok, err := mb.Recv(ctx)
	if err != nil || !ok {
		t.Fatalf("recv: ok=%v err=%v", ok, err)
	}
	return m.Payload
}
