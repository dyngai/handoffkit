package runtime

import (
	"context"
	"errors"
	"sync"

	"github.com/dyngai/handoffkit/sketch"
)

// DeadLetter is an undeliverable message paired with why delivery failed: an
// unknown destination, a Nursery topology violation, a closed inbox, and so on.
// The original Msg is preserved verbatim so a monitoring agent (or a test) can
// inspect or replay it.
type DeadLetter struct {
	Msg    sketch.Msg
	Reason string
}

// DeadLetterSink receives messages that could not be delivered. It is a typed
// sink rather than a Msg Mailbox so the failure Reason travels with the message
// instead of being lost on an error return.
type DeadLetterSink interface {
	Dead(ctx context.Context, dl DeadLetter) error
}

// ChanDeadLetters is a channel-backed DeadLetterSink, the dead-letter counterpart
// to ChanMailbox. Drain it via C() to observe or replay failures. An undrained
// sink applies backpressure (Dead blocks on a full buffer), the same honest
// rendezvous semantics as a mailbox; size the buffer accordingly.
type ChanDeadLetters struct {
	ch        chan DeadLetter
	closeOnce sync.Once
}

// NewDeadLetters returns a sink with the given buffer (0 = unbuffered). A
// negative buffer is clamped to 0.
func NewDeadLetters(buffer int) *ChanDeadLetters {
	if buffer < 0 {
		buffer = 0
	}
	return &ChanDeadLetters{ch: make(chan DeadLetter, buffer)}
}

// Dead deposits dl, blocking per the buffer policy and honoring ctx. A closed
// sink returns ErrClosed rather than panicking.
func (d *ChanDeadLetters) Dead(ctx context.Context, dl DeadLetter) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	defer func() {
		if recover() != nil {
			err = ErrClosed
		}
	}()
	select {
	case d.ch <- dl:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// C exposes the receive channel so a monitor can drain dead letters (or a Select
// can wait on them).
func (d *ChanDeadLetters) C() <-chan DeadLetter { return d.ch }

// Close closes the sink. Idempotent: a double close is a no-op, not a panic.
func (d *ChanDeadLetters) Close() error {
	d.closeOnce.Do(func() { close(d.ch) })
	return nil
}

// deadLetterDispatcher wraps a Dispatcher so a failed Route is captured by a sink
// instead of propagating. See WithDeadLetters.
type deadLetterDispatcher struct {
	inner Dispatcher
	sink  DeadLetterSink
}

// WithDeadLetters wraps inner (a *Router, a *Nursery, or any Dispatcher) so that
// when delivery fails the message is deposited in sink with its failure reason
// and Route returns nil, the message is captured, not lost, and the agent's run
// loop continues instead of dying on one undeliverable output. If the sink
// ITSELF fails (closed, cancelled), Route returns the join of both errors so the
// failure is never swallowed silently. A nil sink is a pass-through (inner's
// error is returned unchanged).
func WithDeadLetters(inner Dispatcher, sink DeadLetterSink) Dispatcher {
	return &deadLetterDispatcher{inner: inner, sink: sink}
}

// Route delegates to the inner Dispatcher and dead-letters on failure.
func (d *deadLetterDispatcher) Route(ctx context.Context, m sketch.Msg) error {
	err := d.inner.Route(ctx, m)
	if err == nil {
		return nil
	}
	if isContextShutdown(err) {
		return err
	}
	if d.sink == nil {
		return err
	}
	if derr := d.sink.Dead(ctx, DeadLetter{Msg: m, Reason: err.Error()}); derr != nil {
		// Could neither deliver nor dead-letter: surface both, never silent.
		return errors.Join(err, derr)
	}
	return nil // captured in the sink; the agent continues
}

var (
	_ DeadLetterSink = (*ChanDeadLetters)(nil)
	_ Dispatcher     = (*deadLetterDispatcher)(nil)
)
