package runtime

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// spawnAck registers an ackAgent under parent and returns it, failing the test
// on a spawn error. The inbox is buffered so a router delivery never blocks the
// test goroutine.
func spawnAck(t *testing.T, n *Nursery, parent, addr, next sketch.Address) *ackAgent {
	t.Helper()
	a := &ackAgent{addr: addr, inbox: NewMailbox(4), next: next}
	if _, err := n.Spawn(context.Background(), parent, a); err != nil {
		t.Fatalf("spawn %q under %q: %v", addr, parent, err)
	}
	return a
}

// The depth/lineage guard: with maxDepth 1 a coordinator may spawn leaf workers,
// but a worker may not spawn further (that would reach depth 2).
func TestNursery_DepthGuardCapsLineage(t *testing.T) {
	n := NewNursery(context.Background(), 1)

	spawnAck(t, n, "", "coord", "")       // depth 0
	spawnAck(t, n, "coord", "w", "coord") // depth 1, allowed

	if d, _ := n.Depth("w"); d != 1 {
		t.Fatalf("worker depth = %d, want 1", d)
	}

	// A worker spawning a grandchild would reach depth 2 > maxDepth 1: rejected.
	grand := &ackAgent{addr: "g", inbox: NewMailbox(1)}
	if _, err := n.Spawn(context.Background(), "w", grand); err == nil {
		t.Fatal("spawn at depth 2 should be rejected by the depth/lineage guard")
	}
	if _, ok := n.Depth("g"); ok {
		t.Fatal("a rejected spawn must not be registered in the tree")
	}
}

// Spawn rejects an unknown parent and a duplicate address.
func TestNursery_SpawnRejectsUnknownParentAndDuplicate(t *testing.T) {
	n := NewNursery(context.Background(), 2)

	if _, err := n.Spawn(context.Background(), "ghost", &ackAgent{addr: "x", inbox: NewMailbox(1)}); err == nil {
		t.Fatal("spawn under an unregistered parent should error")
	}
	spawnAck(t, n, "", "root", "")
	if _, err := n.Spawn(context.Background(), "", &ackAgent{addr: "root", inbox: NewMailbox(1)}); err == nil {
		t.Fatal("spawning a duplicate address should error")
	}
}

// Route enforces who-may-message-whom: parent<->child edges and external seeding
// are deliverable; a sibling-to-sibling lateral message is not.
func TestNursery_RouteEnforcesParentChildTopology(t *testing.T) {
	n := NewNursery(context.Background(), 1)
	spawnAck(t, n, "", "coord", "")
	a := spawnAck(t, n, "coord", "a", "coord")
	spawnAck(t, n, "coord", "b", "coord")
	coordBox := n.router.boxes["coord"].(*ChanMailbox)

	ctx := context.Background()

	// External seeding into the root (From == "").
	if err := n.Route(ctx, sketch.Msg{To: "coord", Payload: "seed"}); err != nil {
		t.Fatalf("external seed to root: %v", err)
	}
	// Parent -> child.
	if err := n.Route(ctx, sketch.Msg{From: "coord", To: "a", Payload: "task"}); err != nil {
		t.Fatalf("coord -> a (parent->child): %v", err)
	}
	// Child -> parent.
	if err := n.Route(ctx, sketch.Msg{From: "a", To: "coord", Payload: "reply"}); err != nil {
		t.Fatalf("a -> coord (child->parent): %v", err)
	}
	// Sibling -> sibling: lateral delegation, denied.
	if err := n.Route(ctx, sketch.Msg{From: "a", To: "b", Payload: "lateral"}); err == nil {
		t.Fatal("a -> b (sibling lateral) should be a topology violation")
	}
	// External seeding is root-only, not a way to bypass child topology.
	if err := n.Route(ctx, sketch.Msg{To: "a", Payload: "direct seed"}); err == nil {
		t.Fatal("external seed to a child should be rejected")
	}
	// Unregistered sender impersonating an agent: denied.
	if err := n.Route(ctx, sketch.Msg{From: "nobody", To: "coord", Payload: "x"}); err == nil {
		t.Fatal("message from an unregistered sender should error")
	}
	// Unknown destination: denied.
	if err := n.Route(ctx, sketch.Msg{From: "coord", To: "ghost", Payload: "x"}); err == nil {
		t.Fatal("message to an unknown destination should error")
	}

	// The allowed messages actually landed in coord's and a's inboxes.
	if m, ok := tryRecv(coordBox); !ok || m.Payload != "seed" {
		t.Fatalf("coord first message = (%q, %v), want (seed, true)", m.Payload, ok)
	}
	if m, ok := tryRecv(coordBox); !ok || m.Payload != "reply" {
		t.Fatalf("coord second message = (%q, %v), want (reply, true)", m.Payload, ok)
	}
	if m, ok := tryRecv(a.inbox); !ok || m.Payload != "task" {
		t.Fatalf("a message = (%q, %v), want (task, true)", m.Payload, ok)
	}
}

func TestRunStampsActualAgentAddressBeforeRouting(t *testing.T) {
	n := NewNursery(context.Background(), 1)
	coord := spawnAck(t, n, "", "coord", "")
	attacker := &spoofAgent{
		addr:  "a",
		inbox: NewMailbox(1),
		out: []sketch.Msg{{
			From:    "coord",
			To:      "b",
			Payload: "spoofed sibling send",
		}},
	}
	victim := spawnAck(t, n, "coord", "b", "coord")
	if _, err := n.Spawn(context.Background(), "coord", attacker); err != nil {
		t.Fatalf("spawn attacker: %v", err)
	}

	actx, ok := n.Context("a")
	if !ok {
		t.Fatal("attacker has no supervised context")
	}
	if err := n.Route(context.Background(), sketch.Msg{From: "coord", To: "a", Payload: "go"}); err != nil {
		t.Fatalf("seed attacker: %v", err)
	}
	err := Run(actx, attacker, n, time.Second)
	if err == nil {
		t.Fatal("Run should reject spoofed sibling output after stamping actual sender")
	}
	if _, ok := tryRecv(victim.inbox); ok {
		t.Fatal("victim received a spoofed sibling message")
	}
	if _, ok := tryRecv(coord.inbox); ok {
		t.Fatal("coord unexpectedly received a message")
	}
}

func TestRunStampsBlankFromBeforeRouting(t *testing.T) {
	n := NewNursery(context.Background(), 1)
	spawnAck(t, n, "", "coord", "")
	attacker := &spoofAgent{
		addr:  "a",
		inbox: NewMailbox(1),
		out: []sketch.Msg{{
			To:      "b",
			Payload: "blank-from sibling send",
		}},
	}
	victim := spawnAck(t, n, "coord", "b", "coord")
	if _, err := n.Spawn(context.Background(), "coord", attacker); err != nil {
		t.Fatalf("spawn attacker: %v", err)
	}

	actx, ok := n.Context("a")
	if !ok {
		t.Fatal("attacker has no supervised context")
	}
	if err := n.Route(context.Background(), sketch.Msg{From: "coord", To: "a", Payload: "go"}); err != nil {
		t.Fatalf("seed attacker: %v", err)
	}
	err := Run(actx, attacker, n, time.Second)
	if err == nil {
		t.Fatal("Run should not allow blank From to become external seeding")
	}
	if _, ok := tryRecv(victim.inbox); ok {
		t.Fatal("victim received a blank-from sibling message")
	}
}

type cancelBlockingMailbox struct {
	entered chan struct{}
	once    sync.Once
}

func newCancelBlockingMailbox() *cancelBlockingMailbox {
	return &cancelBlockingMailbox{entered: make(chan struct{})}
}

func (m *cancelBlockingMailbox) Send(ctx context.Context, _ sketch.Msg) error {
	m.once.Do(func() { close(m.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func (m *cancelBlockingMailbox) Recv(ctx context.Context) (sketch.Msg, bool, error) {
	<-ctx.Done()
	return sketch.Msg{}, false, ctx.Err()
}

func (m *cancelBlockingMailbox) Close() error { return nil }

type sendEnteredMailbox struct {
	sketch.Mailbox
	entered chan struct{}
	once    sync.Once
}

func newSendEnteredMailbox(mb sketch.Mailbox) *sendEnteredMailbox {
	return &sendEnteredMailbox{Mailbox: mb, entered: make(chan struct{})}
}

func (m *sendEnteredMailbox) Send(ctx context.Context, msg sketch.Msg) error {
	m.once.Do(func() { close(m.entered) })
	return m.Mailbox.Send(ctx, msg)
}

func TestNursery_RouteUsesCapturedDestinationMailbox(t *testing.T) {
	n := NewNursery(context.Background(), 1)
	coord := &ackAgent{addr: "coord", inbox: NewMailbox(1)}
	oldInbox := newCancelBlockingMailbox()
	oldWorker := &ackAgent{addr: "w", inbox: oldInbox}
	if _, err := n.Spawn(context.Background(), "", coord); err != nil {
		t.Fatalf("spawn coord: %v", err)
	}
	if _, err := n.Spawn(context.Background(), "coord", oldWorker); err != nil {
		t.Fatalf("spawn old worker: %v", err)
	}

	// Hold the address router so the old implementation can validate topology,
	// drop the nursery lock, and then stall before it re-looks-up m.To.
	n.router.mu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- n.Route(context.Background(), sketch.Msg{From: "coord", To: "w", Payload: "stale"})
	}()

	n.mu.Lock()
	for i := 0; i < 100; i++ {
		n.mu.Unlock()
		goruntime.Gosched()
		select {
		case <-oldInbox.entered:
			i = 100
		default:
		}
		n.mu.Lock()
	}
	oldNode := n.nodes["w"]
	newInbox := NewMailbox(1)
	n.nodes["w"] = &node{
		parent:   "coord",
		depth:    1,
		children: make(map[sketch.Address]struct{}),
		inbox:    newInbox,
		ctx:      oldNode.ctx,
		cancel:   oldNode.cancel,
	}
	n.router.boxes["w"] = newInbox
	n.mu.Unlock()
	n.router.mu.Unlock()

	oldNode.cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Route returned nil after the original destination was cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Route did not unblock after the original destination was cancelled")
	}
	if m, ok := tryRecv(newInbox); ok {
		t.Fatalf("stale route delivered to rebound destination: %#v", m)
	}
}

// Cancel unwinds the whole subtree: a supervised worker's run loop exits when its
// coordinator is cancelled, and the worker leaves the topology so later messages
// to it are rejected.
func TestNursery_CancelUnwindsSubtree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n := NewNursery(ctx, 1)
	coord := spawnAck(t, n, "", "coord", "")
	worker := spawnAck(t, n, "coord", "w", "coord")

	wctx, ok := n.Context("w")
	if !ok {
		t.Fatal("worker has no supervised context")
	}

	// Run the worker under its supervised context; with a long idle it blocks on
	// the inbox until the context is cancelled.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(wctx, worker, n, time.Hour)
		close(done)
	}()

	if err := n.Route(context.Background(), sketch.Msg{From: "coord", To: "w", Payload: "running"}); err != nil {
		t.Fatalf("route to running worker: %v", err)
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ackCancel()
	m, ok, err := coord.inbox.Recv(ackCtx)
	if err != nil || !ok {
		t.Fatalf("worker did not ack before cancellation: ok=%v err=%v", ok, err)
	}
	if m.Payload != "running" {
		t.Fatalf("worker ack payload = %q, want running", m.Payload)
	}

	// Cancelling the coordinator unwinds the subtree, so the worker exits.
	if err := n.Cancel(context.Background(), "coord"); err != nil {
		t.Fatalf("Cancel(coord): %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker run loop did not exit after the subtree was cancelled")
	}
	wg.Wait()

	// Both are gone from the topology and the delivery table.
	if _, ok := n.Depth("w"); ok {
		t.Fatal("worker still in the tree after subtree cancel")
	}
	if _, ok := n.Depth("coord"); ok {
		t.Fatal("coordinator still in the tree after cancel")
	}
	if err := n.Route(context.Background(), sketch.Msg{From: "", To: "w", Payload: "x"}); err == nil {
		t.Fatal("routing to a cancelled agent should error (it left the topology)")
	}

	// Cancel of an unknown address errors.
	if err := n.Cancel(context.Background(), "coord"); err == nil {
		t.Fatal("re-cancelling an already-removed address should error")
	}
}

func TestNursery_ContextReturnsFalseForCancelledContext(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	n := NewNursery(root, 1)
	spawnAck(t, n, "", "coord", "")

	if _, ok := n.Context("coord"); !ok {
		t.Fatal("coord context should exist before cancellation")
	}
	cancel()
	if _, ok := n.Context("coord"); ok {
		t.Fatal("Context should report ok=false after the supervised context is cancelled")
	}
}

func TestNursery_RouteDoesNotDeliverToRootCanceledDestination(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	n := NewNursery(root, 1)
	spawnAck(t, n, "", "coord", "")
	worker := spawnAck(t, n, "coord", "w", "coord")
	cancel()

	err := n.Route(context.Background(), sketch.Msg{From: "coord", To: "w", Payload: "after cancel"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Route after root cancellation: err = %v, want context.Canceled", err)
	}
	if m, ok := tryRecv(worker.inbox); ok {
		t.Fatalf("Route delivered to root-canceled destination: %#v", m)
	}
}

type spoofAgent struct {
	addr  sketch.Address
	inbox sketch.Mailbox
	out   []sketch.Msg
}

func (a *spoofAgent) Address() sketch.Address { return a.addr }
func (a *spoofAgent) Inbox() sketch.Mailbox   { return a.inbox }
func (a *spoofAgent) Step(context.Context, sketch.Msg) ([]sketch.Msg, error) {
	out := a.out
	a.out = nil
	return out, nil
}

// A Route already blocked in the destination mailbox must still return when the
// destination is cancelled. This covers both unbuffered rendezvous mailboxes and
// buffered mailboxes whose queue is full.
func TestNursery_RouteUnblocksWhenDestinationCancelled(t *testing.T) {
	for _, tt := range []struct {
		name    string
		buffer  int
		prefill bool
	}{
		{name: "unbuffered", buffer: 0},
		{name: "full buffered", buffer: 1, prefill: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNursery(context.Background(), 1)
			coord := &ackAgent{addr: "coord", inbox: NewMailbox(1)}
			baseWorkerInbox := NewMailbox(tt.buffer)
			workerInbox := newSendEnteredMailbox(baseWorkerInbox)
			worker := &ackAgent{addr: "w", inbox: workerInbox}

			if _, err := n.Spawn(context.Background(), "", coord); err != nil {
				t.Fatalf("spawn coord: %v", err)
			}
			if _, err := n.Spawn(context.Background(), "coord", worker); err != nil {
				t.Fatalf("spawn worker: %v", err)
			}
			if tt.prefill {
				if err := baseWorkerInbox.Send(context.Background(), sketch.Msg{From: "coord", To: "w", Payload: "prefill"}); err != nil {
					t.Fatalf("prefill worker inbox: %v", err)
				}
			}

			done := make(chan error, 1)
			go func() {
				done <- n.Route(context.Background(), sketch.Msg{From: "coord", To: "w", Payload: "blocked"})
			}()

			select {
			case <-workerInbox.entered:
			case err := <-done:
				t.Fatalf("Route returned before entering destination Send (err=%v)", err)
			case <-time.After(2 * time.Second):
				t.Fatal("Route never entered destination Send")
			}
			select {
			case err := <-done:
				t.Fatalf("Route returned before Cancel (err=%v); test did not exercise a blocked send", err)
			default:
			}

			if err := n.Cancel(context.Background(), "w"); err != nil {
				t.Fatalf("Cancel(w): %v", err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Route err = %v, want context.Canceled", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Route stayed blocked after destination Cancel")
			}
		})
	}
}
