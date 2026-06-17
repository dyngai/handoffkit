package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// A negative buffer is clamped to 0 (unbuffered) rather than panicking in
// make(). Proven by behavior: an unbuffered Send with no receiver blocks until
// the context deadline, if it had been given a positive buffer, Send would
// succeed immediately.
func TestNewMailbox_NegativeBufferClampedToZero(t *testing.T) {
	mb := NewMailbox(-5) // must not panic

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := mb.Send(ctx, sketch.Msg{Payload: "x"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send on unbuffered mailbox with no receiver: err = %v, want context.DeadlineExceeded", err)
	}
}

// Close is idempotent: a double close must not panic.
func TestChanMailbox_DoubleCloseNoPanic(t *testing.T) {
	mb := NewMailbox(1)
	if err := mb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := mb.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// Send after Close returns ErrClosed instead of panicking with send-on-closed.
func TestChanMailbox_SendAfterCloseReturnsError(t *testing.T) {
	mb := NewMailbox(1)
	if err := mb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := mb.Send(context.Background(), sketch.Msg{Payload: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after Close: err = %v, want ErrClosed", err)
	}
}

// Send tolerates a nil context (treated as context.Background()) and does not
// mislabel it as ErrClosed.
func TestChanMailbox_SendNilContext(t *testing.T) {
	mb := NewMailbox(1)
	if err := mb.Send(nil, sketch.Msg{Payload: "x"}); err != nil {
		t.Fatalf("Send(nil ctx): %v", err)
	}
	if m, ok, err := mb.Recv(context.Background()); err != nil || !ok || m.Payload != "x" {
		t.Fatalf("Recv = (%q, %v, %v)", m.Payload, ok, err)
	}
}

// Recv tolerates a nil context (treated as context.Background()) without panic.
func TestChanMailbox_RecvNilContext(t *testing.T) {
	mb := NewMailbox(1)
	if err := mb.Send(context.Background(), sketch.Msg{Payload: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if m, ok, err := mb.Recv(nil); err != nil || !ok || m.Payload != "x" {
		t.Fatalf("Recv(nil ctx) = (%q, %v, %v)", m.Payload, ok, err)
	}
}
