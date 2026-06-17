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
