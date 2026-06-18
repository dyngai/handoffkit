package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// The broadcast guarantee: every subscriber receives every published event, in
// order, the pub/sub counterpart to the pool's exactly-once.
func TestBroker_EverySubscriberGetsEveryEvent(t *testing.T) {
	const nSubs, nEvents = 3, 4

	b := NewBroker()
	subs := make([]*ChanMailbox, nSubs)
	for i := range subs {
		subs[i] = NewMailbox(nEvents) // buffered so Publish never blocks here
		b.Subscribe(subs[i])
	}

	for i := 0; i < nEvents; i++ {
		if err := b.Publish(context.Background(), sketch.Msg{Payload: fmt.Sprintf("e%d", i)}); err != nil {
			t.Fatalf("Publish(%d): %v", i, err)
		}
	}

	for s, mb := range subs {
		for i := 0; i < nEvents; i++ {
			m, ok, err := mb.Recv(context.Background())
			if err != nil || !ok {
				t.Fatalf("sub %d recv %d: ok=%v err=%v", s, i, ok, err)
			}
			if want := fmt.Sprintf("e%d", i); m.Payload != want {
				t.Fatalf("sub %d event %d = %q, want %q (order not preserved)", s, i, m.Payload, want)
			}
		}
	}
}

// Publish with no subscribers is a no-op, not an error.
func TestBroker_NoSubscribers(t *testing.T) {
	if err := NewBroker().Publish(context.Background(), sketch.Msg{Payload: "x"}); err != nil {
		t.Fatalf("publish with no subscribers should be a no-op, got %v", err)
	}
}

func TestBroker_PublishClonesHandoffContextForSubscribers(t *testing.T) {
	b := NewBroker()
	sub0 := NewMailbox(1)
	sub1 := NewMailbox(1)
	b.Subscribe(sub0)
	b.Subscribe(sub1)

	msg := sketch.Msg{
		Payload: "e",
		Ctx: sketch.HandoffContext{
			Summary: "summary",
			Thread: []sketch.Turn{
				{Role: "user", Content: "original thread 0"},
				{Role: "assistant", Content: "original thread 1"},
			},
			Refs: []sketch.MemoryRef{
				{Namespace: "ns", Key: "original-ref-0"},
				{Namespace: "ns", Key: "original-ref-1"},
			},
		},
	}

	if err := b.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got0, ok, err := sub0.Recv(context.Background())
	if err != nil || !ok {
		t.Fatalf("sub0 recv: ok=%v err=%v", ok, err)
	}
	got1, ok, err := sub1.Recv(context.Background())
	if err != nil || !ok {
		t.Fatalf("sub1 recv: ok=%v err=%v", ok, err)
	}

	if &got0.Ctx.Thread[0] == &got1.Ctx.Thread[0] {
		t.Fatal("subscriber messages share Ctx.Thread backing storage")
	}
	if &got0.Ctx.Refs[0] == &got1.Ctx.Refs[0] {
		t.Fatal("subscriber messages share Ctx.Refs backing storage")
	}

	got0.Ctx.Thread[0].Content = "mutated thread"
	got0.Ctx.Refs[0].Key = "mutated-ref"
	if got := got1.Ctx.Thread[0].Content; got != "original thread 0" {
		t.Fatalf("mutating sub0 Thread changed sub1 Thread to %q", got)
	}
	if got := got1.Ctx.Refs[0].Key; got != "original-ref-0" {
		t.Fatalf("mutating sub0 Refs changed sub1 Refs to %q", got)
	}
}

// Backpressure: Publish is synchronous fan-out, so an undrained subscriber
// blocks the publisher until it is released.
func TestBroker_Backpressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b := NewBroker()
	fast := NewMailbox(1) // buffered: accepts its copy immediately
	slow := newBlockingMailbox()
	b.Subscribe(fast)
	b.Subscribe(slow)

	done := make(chan error, 1)
	go func() { done <- b.Publish(ctx, sketch.Msg{Payload: "e"}) }()

	if m := <-slow.entered; m.Payload != "e" {
		t.Fatalf("slow subscriber got %q, want e", m.Payload)
	}
	// Publish cannot complete while the slow subscriber has not released Send.
	select {
	case err := <-done:
		t.Fatalf("Publish returned before slow subscriber released Send (err=%v), no backpressure", err)
	default:
	}

	// Release it; Publish must now complete.
	slow.releaseSend()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish after release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not complete after the slow subscriber released Send")
	}
}

// A Publish blocked on a slow subscriber returns the context error when the
// context is cancelled, backpressure that honors cancellation rather than
// hanging forever (exercises ChanMailbox.Send's ctx.Done() arm).
func TestBroker_PublishCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewBroker()
	slow := newBlockingMailbox()
	b.Subscribe(slow)

	done := make(chan error, 1)
	go func() { done <- b.Publish(ctx, sketch.Msg{Payload: "e"}) }()

	if m := <-slow.entered; m.Payload != "e" {
		t.Fatalf("slow subscriber got %q, want e", m.Payload)
	}
	// Confirm it is blocked before cancelling.
	select {
	case err := <-done:
		t.Fatalf("Publish returned before cancellation (err=%v)", err)
	default:
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return after cancellation")
	}
}

type blockingMailbox struct {
	entered chan sketch.Msg
	release chan struct{}
	once    sync.Once
}

func newBlockingMailbox() *blockingMailbox {
	return &blockingMailbox{
		entered: make(chan sketch.Msg, 1),
		release: make(chan struct{}),
	}
}

func (m *blockingMailbox) Send(ctx context.Context, msg sketch.Msg) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case m.entered <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-m.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *blockingMailbox) Recv(context.Context) (sketch.Msg, bool, error) {
	return sketch.Msg{}, false, nil
}

func (m *blockingMailbox) Close() error {
	m.releaseSend()
	return nil
}

func (m *blockingMailbox) releaseSend() {
	m.once.Do(func() { close(m.release) })
}

// ackAgent acks every event it receives, so a broadcast's delivery count can be
// asserted by fan-in. Mirrors examples/pubsub.
type ackAgent struct {
	addr  sketch.Address
	inbox sketch.Mailbox
	next  sketch.Address
}

func (a *ackAgent) Address() sketch.Address { return a.addr }
func (a *ackAgent) Inbox() sketch.Mailbox   { return a.inbox }
func (a *ackAgent) Step(_ context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	return []sketch.Msg{{From: a.addr, To: a.next, Payload: in.Payload}}, nil
}

// failingMailbox always fails on Send; used to prove broadcast fan-out attempts
// every subscriber even when one fails.
type failingMailbox struct{ err error }

func (f failingMailbox) Send(context.Context, sketch.Msg) error { return f.err }
func (failingMailbox) Recv(context.Context) (sketch.Msg, bool, error) {
	return sketch.Msg{}, false, nil
}
func (failingMailbox) Close() error { return nil }

// A failing subscriber in the middle must not deny the others: Publish attempts
// all subscribers and returns the joined error.
func TestBroker_PublishDeliversToAllDespiteOneFailure(t *testing.T) {
	b := NewBroker()
	ok0 := NewMailbox(1)
	fail := failingMailbox{err: errors.New("boom")}
	ok2 := NewMailbox(1)
	b.Subscribe(ok0)
	b.Subscribe(fail)
	b.Subscribe(ok2)

	err := b.Publish(context.Background(), sketch.Msg{Payload: "e"})
	if err == nil {
		t.Fatal("expected an error from the failing subscriber")
	}
	for name, mb := range map[string]*ChanMailbox{"ok0": ok0, "ok2": ok2} {
		select {
		case m := <-mb.C():
			if m.Payload != "e" {
				t.Fatalf("%s received %q, want e", name, m.Payload)
			}
		default:
			t.Fatalf("%s did not receive the broadcast (fan-out stopped at the failing subscriber)", name)
		}
	}
}

// A nil mailbox passed to Subscribe is ignored, so Publish does not panic.
func TestBroker_SubscribeNilIgnored(t *testing.T) {
	b := NewBroker()
	ok := NewMailbox(1)
	b.Subscribe(nil) // must be ignored, not stored
	b.Subscribe(ok)

	if err := b.Publish(context.Background(), sketch.Msg{Payload: "e"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case m := <-ok.C():
		if m.Payload != "e" {
			t.Fatalf("subscriber got %q, want e", m.Payload)
		}
	default:
		t.Fatal("subscriber did not receive the broadcast")
	}
}

// The concurrent path the unit tests don't cover: N subscribers running as
// goroutines on the agent loop, M events broadcast through the Broker, and
// exactly N*M acks collected via Select. The deterministic version of the
// examples/pubsub run, and a -race check on the fan-out.
func TestBroker_ConcurrentBroadcast(t *testing.T) {
	const nSubs, nEvents = 4, 5

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	broker := NewBroker()
	router := NewRouter()
	acks := NewMailbox(nSubs * nEvents)
	router.Register("acks", acks)

	var wg sync.WaitGroup
	for i := 0; i < nSubs; i++ {
		inbox := NewMailbox(nEvents) // buffered so Publish never blocks
		s := &ackAgent{addr: sketch.Address(fmt.Sprintf("sub-%d", i)), inbox: inbox, next: "acks"}
		broker.Subscribe(inbox)
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			_ = Run(ctx, a, router, time.Second)
		}(s)
	}

	for i := 0; i < nEvents; i++ {
		if err := broker.Publish(ctx, sketch.Msg{Payload: fmt.Sprintf("e%d", i)}); err != nil {
			t.Fatalf("Publish(%d): %v", i, err)
		}
	}

	// Collect exactly nSubs*nEvents acks. Handlers run in this goroutine, so the
	// counter needs no lock.
	want := nSubs * nEvents
	sel := NewSelector()
	got := 0
	for got < want {
		_, err := sel.Run(ctx, sketch.Select{Cases: []sketch.Case{
			{Mailbox: acks, OnRecv: func(sketch.Msg) error { got++; return nil }},
			{After: 5 * time.Second, OnAfter: func() error { return fmt.Errorf("timed out after %d/%d acks", got, want) }},
		}})
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
	}
	cancel()
	wg.Wait()

	if got != want {
		t.Fatalf("got %d acks, want %d (every event must reach every subscriber)", got, want)
	}
}
