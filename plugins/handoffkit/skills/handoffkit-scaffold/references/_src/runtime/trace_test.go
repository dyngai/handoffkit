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

func TestRunTraced_TraceSnapshotsDoNotAliasMessageContext(t *testing.T) {
	source := &contextEmitterAgent{addr: "source", inbox: NewMailbox(1), next: "sink"}
	sink := &contextMutatingAgent{addr: "sink", inbox: NewMailbox(1), next: "done"}
	done := NewMailbox(1)

	r := NewRouter()
	r.Register("source", source.inbox)
	r.Register("sink", sink.inbox)
	r.Register("done", done)

	var mu sync.Mutex
	var events []TraceEvent
	trace := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, ag := range []sketch.Agent{source, sink} {
		wg.Add(1)
		go func(ag sketch.Agent) {
			defer wg.Done()
			_ = RunTraced(ctx, ag, r, time.Second, trace)
		}(ag)
	}

	if err := source.inbox.Send(ctx, sketch.Msg{From: "user", To: "source", Payload: "start"}); err != nil {
		t.Fatalf("send: %v", err)
	}
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

	var sourceSend, sinkRecv *TraceEvent
	for i := range events {
		ev := &events[i]
		switch {
		case ev.Agent == "source" && ev.Dir == TraceSend:
			sourceSend = ev
		case ev.Agent == "sink" && ev.Dir == TraceRecv:
			sinkRecv = ev
		}
	}
	if sourceSend == nil {
		t.Fatalf("missing source send event (events=%+v)", events)
	}
	if sinkRecv == nil {
		t.Fatalf("missing sink recv event (events=%+v)", events)
	}
	for name, ev := range map[string]*TraceEvent{
		"source send": sourceSend,
		"sink recv":   sinkRecv,
	} {
		if got := ev.Msg.Ctx.Thread[0].Content; got != "original thread" {
			t.Fatalf("%s thread content = %q, want original thread", name, got)
		}
		if got := ev.Msg.Ctx.Refs[0].Key; got != "original-ref" {
			t.Fatalf("%s ref key = %q, want original-ref", name, got)
		}
	}
}

type contextEmitterAgent struct {
	addr  sketch.Address
	inbox sketch.Mailbox
	next  sketch.Address
}

func (a *contextEmitterAgent) Address() sketch.Address { return a.addr }
func (a *contextEmitterAgent) Inbox() sketch.Mailbox   { return a.inbox }
func (a *contextEmitterAgent) Step(_ context.Context, _ sketch.Msg) ([]sketch.Msg, error) {
	return []sketch.Msg{{
		To:      a.next,
		Payload: "handoff",
		Ctx: sketch.HandoffContext{
			Summary: "summary",
			Thread:  []sketch.Turn{{Role: "assistant", Content: "original thread"}},
			Refs:    []sketch.MemoryRef{{Namespace: "ns", Key: "original-ref"}},
		},
	}}, nil
}

type contextMutatingAgent struct {
	addr  sketch.Address
	inbox sketch.Mailbox
	next  sketch.Address
}

func (a *contextMutatingAgent) Address() sketch.Address { return a.addr }
func (a *contextMutatingAgent) Inbox() sketch.Mailbox   { return a.inbox }
func (a *contextMutatingAgent) Step(_ context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	in.Ctx.Thread[0].Content = "mutated thread"
	in.Ctx.Refs[0].Key = "mutated-ref"
	return []sketch.Msg{{To: a.next, Payload: "done"}}, nil
}
