package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// RunTraced reports every message each agent saw (recv) and communicated (send),
// across a two-stage pipeline a -> b -> done.
func TestRunTraced_CapturesRecvAndSend(t *testing.T) {
	a := &ackAgent{addr: "a", inbox: NewMailbox(1), next: "b"}
	b := &ackAgent{addr: "b", inbox: NewMailbox(1), next: "done"}
	done := NewMailbox(1)

	r := NewRouter()
	r.Register("a", a.inbox)
	r.Register("b", b.inbox)
	r.Register("done", done)

	var mu sync.Mutex
	var events []TraceEvent
	trace := func(ev TraceEvent) { mu.Lock(); events = append(events, ev); mu.Unlock() }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, ag := range []sketch.Agent{a, b} {
		wg.Add(1)
		go func(ag sketch.Agent) {
			defer wg.Done()
			_ = RunTraced(ctx, ag, r, time.Second, trace)
		}(ag)
	}

	if err := a.inbox.Send(ctx, sketch.Msg{From: "user", To: "a", Payload: "hi"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	// Wait for the message to reach the terminal mailbox, then stop the agents.
	_, err := NewSelector().Run(ctx, sketch.Select{Cases: []sketch.Case{
		{Mailbox: done, OnRecv: func(sketch.Msg) error { return nil }},
		{After: 3 * time.Second, OnAfter: func() error { return fmt.Errorf("timed out") }},
	}})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	// Expect exactly: a recv, a send→b, b recv, b send→done.
	var recv, send int
	byAgent := map[sketch.Address]map[TraceDir]sketch.Msg{}
	for _, ev := range events {
		if byAgent[ev.Agent] == nil {
			byAgent[ev.Agent] = map[TraceDir]sketch.Msg{}
		}
		byAgent[ev.Agent][ev.Dir] = ev.Msg
		switch ev.Dir {
		case TraceRecv:
			recv++
		case TraceSend:
			send++
		}
	}
	if recv != 2 || send != 2 {
		t.Fatalf("trace counts: recv=%d send=%d, want 2 and 2 (events=%+v)", recv, send, events)
	}
	if _, ok := byAgent["a"][TraceRecv]; !ok {
		t.Fatal("missing recv event for agent a")
	}
	if to := byAgent["a"][TraceSend].To; to != "b" {
		t.Fatalf("a send.To = %q, want b", to)
	}
	if got := byAgent["b"][TraceRecv].Payload; got != "hi" {
		t.Fatalf("b saw %q, want the payload a forwarded (\"hi\")", got)
	}
	if to := byAgent["b"][TraceSend].To; to != "done" {
		t.Fatalf("b send.To = %q, want done", to)
	}
}
