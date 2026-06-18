package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// forwardAgent emits one message addressed to its inbound payload (treated as a
// destination address), so a test can steer an output to a live or a dead
// address by choosing the payload.
type forwardAgent struct {
	addr  sketch.Address
	inbox sketch.Mailbox
}

func (a *forwardAgent) Address() sketch.Address { return a.addr }
func (a *forwardAgent) Inbox() sketch.Mailbox   { return a.inbox }
func (a *forwardAgent) Step(_ context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	return []sketch.Msg{{From: a.addr, To: sketch.Address(in.Payload), Payload: "fwd:" + in.Payload}}, nil
}

// An undeliverable Route is captured by the sink with its reason, and Route
// returns nil (the message was captured, not lost).
func TestDeadLetters_UnknownDestinationCaptured(t *testing.T) {
	dlq := NewDeadLetters(1)
	disp := WithDeadLetters(NewRouter(), dlq)

	err := disp.Route(context.Background(), sketch.Msg{From: "a", To: "ghost", Payload: "x"})
	if err != nil {
		t.Fatalf("Route should return nil once captured, got %v", err)
	}
	select {
	case dl := <-dlq.C():
		if dl.Msg.To != "ghost" || dl.Msg.Payload != "x" {
			t.Fatalf("captured the wrong message: %+v", dl.Msg)
		}
		if dl.Reason == "" {
			t.Fatal("dead letter has no reason")
		}
	default:
		t.Fatal("undeliverable message was not captured in the sink")
	}
}

// A successful Route is delivered and nothing is dead-lettered.
func TestDeadLetters_SuccessfulRouteNotCaptured(t *testing.T) {
	r := NewRouter()
	box := NewMailbox(1)
	r.Register("real", box)
	dlq := NewDeadLetters(1)

	if err := WithDeadLetters(r, dlq).Route(context.Background(), sketch.Msg{To: "real", Payload: "ok"}); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if m, ok := tryRecv(box); !ok || m.Payload != "ok" {
		t.Fatalf("message not delivered: (%q, %v)", m.Payload, ok)
	}
	select {
	case dl := <-dlq.C():
		t.Fatalf("a delivered message was dead-lettered: %+v", dl)
	default:
	}
}

// Composition with the Nursery: a topology violation (lateral sibling message)
// is dead-lettered with the violation as the reason.
func TestDeadLetters_NurseryTopologyViolationCaptured(t *testing.T) {
	n := NewNursery(context.Background(), 1)
	if _, err := n.Spawn(context.Background(), "", &forwardAgent{addr: "coord", inbox: NewMailbox(1)}); err != nil {
		t.Fatalf("spawn coord: %v", err)
	}
	for _, name := range []sketch.Address{"a", "b"} {
		if _, err := n.Spawn(context.Background(), "coord", &forwardAgent{addr: name, inbox: NewMailbox(1)}); err != nil {
			t.Fatalf("spawn %s: %v", name, err)
		}
	}
	dlq := NewDeadLetters(1)
	disp := WithDeadLetters(n, dlq)

	// a -> b is a sibling lateral message: a topology violation.
	if err := disp.Route(context.Background(), sketch.Msg{From: "a", To: "b", Payload: "lateral"}); err != nil {
		t.Fatalf("Route should be captured, got %v", err)
	}
	select {
	case dl := <-dlq.C():
		if dl.Msg.From != "a" || dl.Msg.To != "b" {
			t.Fatalf("captured wrong message: %+v", dl.Msg)
		}
	default:
		t.Fatal("topology violation was not dead-lettered")
	}
}

// If the sink itself fails (closed), the error is surfaced (joined), never
// swallowed.
func TestDeadLetters_SinkFailureSurfaced(t *testing.T) {
	dlq := NewDeadLetters(1)
	_ = dlq.Close()
	disp := WithDeadLetters(NewRouter(), dlq)

	err := disp.Route(context.Background(), sketch.Msg{To: "ghost", Payload: "x"})
	if err == nil {
		t.Fatal("a failed deliver AND a failed sink must return an error")
	}
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want it to include ErrClosed", err)
	}
}

// Context shutdown from the inner dispatcher is not a route failure to capture:
// callers such as Run need to see it and treat it as clean shutdown.
func TestDeadLetters_ContextShutdownErrorsArePreserved(t *testing.T) {
	for _, wantErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(wantErr.Error(), func(t *testing.T) {
			dlq := NewDeadLetters(1)
			disp := WithDeadLetters(errorDispatcher{err: wantErr}, dlq)

			err := disp.Route(context.Background(), sketch.Msg{To: "worker", Payload: "stop"})
			if !errors.Is(err, wantErr) {
				t.Fatalf("Route err = %v, want %v", err, wantErr)
			}
			select {
			case dl := <-dlq.C():
				t.Fatalf("shutdown error was dead-lettered: %+v", dl)
			default:
			}
		})
	}
}

func TestChanDeadLetters_DeadCanceledContextWinsOverReadyBuffer(t *testing.T) {
	dlq := NewDeadLetters(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := dlq.Dead(ctx, DeadLetter{Msg: sketch.Msg{Payload: "canceled"}, Reason: "test"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dead with canceled context: err = %v, want context.Canceled", err)
	}
	select {
	case dl := <-dlq.C():
		t.Fatalf("Dead enqueued into a ready buffer despite an already-canceled context: %+v", dl)
	default:
	}
	if err := dlq.Dead(context.Background(), DeadLetter{Msg: sketch.Msg{Payload: "after"}, Reason: "test"}); err != nil {
		t.Fatalf("Dead after canceled attempt: %v", err)
	}
}

// End to end: an agent whose output targets a dead address keeps running (the
// message is dead-lettered, not fatal) and goes on to deliver its next output.
func TestDeadLetters_AgentSurvivesUndeliverableOutput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r := NewRouter()
	sinkBox := NewMailbox(1)
	r.Register("sink", sinkBox)
	dlq := NewDeadLetters(2)
	disp := WithDeadLetters(r, dlq)

	agentIn := NewMailbox(2)
	fa := &forwardAgent{addr: "fwd", inbox: agentIn}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(ctx, fa, disp, time.Second)
	}()

	mustSend(t, ctx, agentIn, "ghost") // emits to an unregistered address
	mustSend(t, ctx, agentIn, "sink")  // emits to a live address

	// The second output landed: the agent survived the undeliverable first.
	if got := recvPayload(t, ctx, sinkBox); got != "fwd:sink" {
		t.Fatalf("sink got %q, want fwd:sink (agent died on the undeliverable output?)", got)
	}
	// And the first was captured, not lost.
	select {
	case dl := <-dlq.C():
		if dl.Msg.To != "ghost" {
			t.Fatalf("dead letter targeted %q, want ghost", dl.Msg.To)
		}
	case <-time.After(time.Second):
		t.Fatal("the undeliverable output was not dead-lettered")
	}

	cancel()
	wg.Wait()
}
