package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

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

	in := NewMailbox(3)
	out := NewMailbox(2)
	join := NewJoinAgent("join", in, "out", 3, combine)
	r := NewRouter()
	r.Register("out", out)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, join, r, time.Second)
	}()

	// Two of three dependencies: the barrier must stay closed (no output yet).
	mustSend(t, ctx, in, "a")
	mustSend(t, ctx, in, "b")
	select {
	case <-out.C():
		t.Fatal("join emitted before all dependencies arrived")
	case <-time.After(50 * time.Millisecond):
		// still waiting, as required
	}

	// Third arrives → the barrier fires with the whole batch.
	mustSend(t, ctx, in, "c")
	if got := recvPayload(t, ctx, out); got != "a|b|c" {
		t.Fatalf("first batch = %q, want a|b|c", got)
	}

	// It resets: a second batch of three joins independently.
	mustSend(t, ctx, in, "d")
	mustSend(t, ctx, in, "e")
	mustSend(t, ctx, in, "f")
	if got := recvPayload(t, ctx, out); got != "d|e|f" {
		t.Fatalf("second batch = %q, want d|e|f", got)
	}

	cancel()
	wg.Wait()
}

// need is clamped to >= 1, and a nil combine joins payloads with newlines.
func TestJoinAgent_Defaults(t *testing.T) {
	in := NewMailbox(1)
	out := NewMailbox(1)
	join := NewJoinAgent("join", in, "out", 0, nil) // need<1 -> 1; nil combine
	r := NewRouter()
	r.Register("out", out)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, join, r, time.Second)
	}()

	mustSend(t, ctx, in, "solo")
	if got := recvPayload(t, ctx, out); got != "solo" {
		t.Fatalf("need=0 should fire per message: got %q, want solo", got)
	}
	cancel()
	wg.Wait()
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
