// Package runtime is a minimal, dependency-free reference implementation of the
// sketch interfaces: a channel-backed Mailbox, a reflect.Select-based
// Selector, and a Router + agent run loop. It is deliberately small, enough to
// make the sketch runnable and to demonstrate that "Mailbox = Go channel" and
// "Select = Go select" are literal, not metaphorical.
package runtime

import (
	"context"
	"errors"
	"sync"

	"github.com/dyngai/handoffkit/sketch"
)

// ChanMailbox is the channel-backed Mailbox, the literal "Mailbox = Go channel"
// claim made concrete. Buffer 0 yields an unbuffered rendezvous (Send blocks
// until a Recv is ready, giving backpressure for free); buffer > 0 decouples
// sender and receiver for fan-out worker pools.
type ChanMailbox struct {
	ch        chan sketch.Msg
	closeOnce sync.Once
}

// NewMailbox returns a mailbox with the given buffer (0 = unbuffered rendezvous).
// A negative buffer is clamped to 0 rather than panicking in make().
func NewMailbox(buffer int) *ChanMailbox {
	if buffer < 0 {
		buffer = 0
	}
	return &ChanMailbox{ch: make(chan sketch.Msg, buffer)}
}

// ErrClosed is returned by Send when the mailbox has been closed.
var ErrClosed = errors.New("handoffkit: send on closed mailbox")

// Send delivers msg, blocking per the buffering policy and honoring ctx. If the
// mailbox is closed (now, or concurrently mid-send) it returns ErrClosed rather
// than panicking, so a closed mailbox is a clean error, not a crash.
func (m *ChanMailbox) Send(ctx context.Context, msg sketch.Msg) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Only the channel send can panic here (send on closed); the nil-ctx guard
	// above keeps a nil context from panicking and being mislabeled ErrClosed.
	defer func() {
		if recover() != nil {
			err = ErrClosed
		}
	}()
	select {
	case m.ch <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Recv returns the next message; ok is false once the mailbox is closed/drained.
func (m *ChanMailbox) Recv(ctx context.Context) (sketch.Msg, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case msg, ok := <-m.ch:
		return msg, ok, nil
	case <-ctx.Done():
		return sketch.Msg{}, false, ctx.Err()
	}
}

// Close closes the mailbox. It is idempotent: extra calls are no-ops, so a
// double close cannot panic. Only the sender side should close; receivers see
// ok == false once the mailbox is drained.
func (m *ChanMailbox) Close() error {
	m.closeOnce.Do(func() { close(m.ch) })
	return nil
}

// C exposes the receive channel so the Selector can compose waits over it with
// reflect.Select. This accessor is the seam that makes `select`-style
// composition work across mailboxes.
func (m *ChanMailbox) C() <-chan sketch.Msg { return m.ch }

// Receiver is implemented by mailboxes whose channel the Selector can wait on.
type Receiver interface {
	C() <-chan sketch.Msg
}

var _ sketch.Mailbox = (*ChanMailbox)(nil)
