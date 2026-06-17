package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// assertNoEmission fails if anything arrives on out within a short window.
func assertNoEmission(t *testing.T, out *ChanMailbox) {
	t.Helper()
	select {
	case m := <-out.C():
		t.Fatalf("quorum emitted again in the same round: %q", m.Payload)
	case <-time.After(80 * time.Millisecond):
	}
}

// need == 1 is a race: the quorum emits the first arrival and drops the rest of
// the round, then resets for an independent next round.
func TestQuorumAgent_FirstWinsAndDropsStragglers(t *testing.T) {
	in := NewMailbox(3)
	out := NewMailbox(3)
	q := NewQuorumAgent("q", in, "out", 1, 3, nil)
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

	mustSend(t, ctx, in, "first")
	mustSend(t, ctx, in, "second")
	mustSend(t, ctx, in, "third")
	if got := recvPayload(t, ctx, out); got != "first" {
		t.Fatalf("quorum emitted %q, want first (the race winner)", got)
	}
	assertNoEmission(t, out) // stragglers second/third are dropped

	// The round reset: a fresh batch of three races independently.
	mustSend(t, ctx, in, "fourth")
	mustSend(t, ctx, in, "fifth")
	mustSend(t, ctx, in, "sixth")
	if got := recvPayload(t, ctx, out); got != "fourth" {
		t.Fatalf("second round emitted %q, want fourth", got)
	}
	assertNoEmission(t, out)

	cancel()
	wg.Wait()
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

	in := NewMailbox(3)
	out := NewMailbox(3)
	q := NewQuorumAgent("q", in, "out", 2, 3, combine)
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

	mustSend(t, ctx, in, "a")
	mustSend(t, ctx, in, "b")
	mustSend(t, ctx, in, "c")
	if got := recvPayload(t, ctx, out); got != "a|b" {
		t.Fatalf("quorum emitted %q, want a|b (first two of three)", got)
	}
	assertNoEmission(t, out) // c is the ignored straggler

	mustSend(t, ctx, in, "d")
	mustSend(t, ctx, in, "e")
	mustSend(t, ctx, in, "f")
	if got := recvPayload(t, ctx, out); got != "d|e" {
		t.Fatalf("second round emitted %q, want d|e", got)
	}

	cancel()
	wg.Wait()
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
