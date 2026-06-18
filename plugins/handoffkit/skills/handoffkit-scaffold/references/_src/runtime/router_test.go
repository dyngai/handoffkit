package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// Routing to a nil mailbox returns an error rather than panicking at Send.
func TestRouter_RouteNilMailboxReturnsError(t *testing.T) {
	r := NewRouter()
	r.Register("a", nil)
	if err := r.Route(context.Background(), sketch.Msg{To: "a", Payload: "x"}); err == nil {
		t.Fatal("Route to a nil mailbox should return an error, not panic")
	}
}

// tryRecv is non-blocking: an empty or closed mailbox reports not-ready, a
// buffered one yields its message.
func TestTryRecv(t *testing.T) {
	mb := NewMailbox(1)
	if _, ok := tryRecv(mb); ok {
		t.Fatal("tryRecv on an empty mailbox returned ok=true")
	}

	if err := mb.Send(context.Background(), sketch.Msg{Payload: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if m, ok := tryRecv(mb); !ok || m.Payload != "x" {
		t.Fatalf("tryRecv = (%q, %v), want (x, true)", m.Payload, ok)
	}

	_ = mb.Close()
	if _, ok := tryRecv(mb); ok {
		t.Fatal("tryRecv on a closed mailbox returned ok=true")
	}
}

// Route on a nil *Router returns an error instead of panicking.
func TestRouter_RouteNilReceiverReturnsError(t *testing.T) {
	var r *Router
	if err := r.Route(context.Background(), sketch.Msg{To: "x"}); err == nil {
		t.Fatal("Route on a nil Router should return an error, not panic")
	}
}

// A zero-value Router (built without NewRouter) lazy-inits its map on Register
// instead of panicking on a nil map.
func TestRouter_RegisterZeroValueRouter(t *testing.T) {
	var r Router
	mb := NewMailbox(1)
	r.Register("a", mb) // must not panic on the nil boxes map
	if err := r.Route(context.Background(), sketch.Msg{To: "a", Payload: "x"}); err != nil {
		t.Fatalf("Route after zero-value Register: %v", err)
	}
	if m, _, _ := mb.Recv(context.Background()); m.Payload != "x" {
		t.Fatalf("delivered %q, want x", m.Payload)
	}
}

func TestRunRejectsNonPositiveIdle(t *testing.T) {
	a := &ackAgent{addr: "a", inbox: NewMailbox(0)}
	r := NewRouter()
	r.Register("a", a.inbox)

	for _, idle := range []time.Duration{0, -time.Nanosecond} {
		if err := Run(context.Background(), a, r, idle); !errors.Is(err, errInvalidIdle) {
			t.Fatalf("Run idle %s err = %v, want errInvalidIdle", idle, err)
		}
		if err := RunTraced(context.Background(), a, r, idle, nil); !errors.Is(err, errInvalidIdle) {
			t.Fatalf("RunTraced idle %s err = %v, want errInvalidIdle", idle, err)
		}
	}
}

func TestRunTreatsStepContextErrorsAsCleanShutdown(t *testing.T) {
	for _, wantErr := range []error{context.Canceled, context.DeadlineExceeded} {
		for name, run := range map[string]func(context.Context, sketch.Agent, Dispatcher, time.Duration) error{
			"Run": Run,
			"RunTraced": func(ctx context.Context, a sketch.Agent, r Dispatcher, idle time.Duration) error {
				return RunTraced(ctx, a, r, idle, nil)
			},
		} {
			t.Run(name+"/"+wantErr.Error(), func(t *testing.T) {
				a := &stepErrorAgent{addr: "a", inbox: NewMailbox(1), err: wantErr}
				if err := a.inbox.Send(context.Background(), sketch.Msg{To: "a", Payload: "work"}); err != nil {
					t.Fatalf("Send: %v", err)
				}
				if err := run(context.Background(), a, NewRouter(), time.Second); err != nil {
					t.Fatalf("%s returned %v, want nil clean shutdown", name, err)
				}
			})
		}
	}
}

func TestRunTreatsRouteContextErrorsAsCleanShutdown(t *testing.T) {
	for _, wantErr := range []error{context.Canceled, context.DeadlineExceeded} {
		for name, run := range map[string]func(context.Context, sketch.Agent, Dispatcher, time.Duration) error{
			"Run": Run,
			"RunTraced": func(ctx context.Context, a sketch.Agent, r Dispatcher, idle time.Duration) error {
				return RunTraced(ctx, a, r, idle, nil)
			},
		} {
			t.Run(name+"/"+wantErr.Error(), func(t *testing.T) {
				a := &routeOnceAgent{addr: "a", inbox: NewMailbox(1), to: "b"}
				if err := a.inbox.Send(context.Background(), sketch.Msg{To: "a", Payload: "work"}); err != nil {
					t.Fatalf("Send: %v", err)
				}
				r := errorDispatcher{err: wantErr}
				if err := run(context.Background(), a, r, time.Second); err != nil {
					t.Fatalf("%s returned %v, want nil clean shutdown", name, err)
				}
			})
		}
	}
}

type stepErrorAgent struct {
	addr  sketch.Address
	inbox sketch.Mailbox
	err   error
}

func (a *stepErrorAgent) Address() sketch.Address { return a.addr }
func (a *stepErrorAgent) Inbox() sketch.Mailbox   { return a.inbox }
func (a *stepErrorAgent) Step(context.Context, sketch.Msg) ([]sketch.Msg, error) {
	return nil, a.err
}

type routeOnceAgent struct {
	addr  sketch.Address
	inbox sketch.Mailbox
	to    sketch.Address
}

func (a *routeOnceAgent) Address() sketch.Address { return a.addr }
func (a *routeOnceAgent) Inbox() sketch.Mailbox   { return a.inbox }
func (a *routeOnceAgent) Step(_ context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	return []sketch.Msg{{To: a.to, Payload: in.Payload}}, nil
}

type errorDispatcher struct {
	err error
}

func (d errorDispatcher) Route(context.Context, sketch.Msg) error {
	return d.err
}
