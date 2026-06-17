package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// Router maps addresses to mailboxes and delivers point-to-point. It does not
// enforce a topology ("who may message whom"); that is the Supervisor's job. A
// production Supervisor would layer topology and depth/lineage guards on top
// (see docs/primitives.md).
type Router struct {
	mu    sync.RWMutex
	boxes map[sketch.Address]sketch.Mailbox
}

// NewRouter returns an empty router.
func NewRouter() *Router {
	return &Router{boxes: make(map[sketch.Address]sketch.Mailbox)}
}

// Register binds an address to a mailbox.
func (r *Router) Register(addr sketch.Address, mb sketch.Mailbox) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.boxes == nil { // tolerate a zero-value Router built without NewRouter
		r.boxes = make(map[sketch.Address]sketch.Mailbox)
	}
	r.boxes[addr] = mb
}

// Unregister removes an address binding. It is a no-op if the address is
// unknown, so a Nursery can drop a cancelled agent's mailbox idempotently.
func (r *Router) Unregister(addr sketch.Address) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.boxes, addr)
}

// Dispatcher delivers a message to its destination, transferring ownership.
// *Router is the bare point-to-point delivery; *Nursery wraps it with a
// who-may-message-whom topology guard. runtime.Run takes a Dispatcher so an
// agent's outputs can be delivered through either.
type Dispatcher interface {
	Route(ctx context.Context, m sketch.Msg) error
}

// Route delivers m to its destination mailbox, transferring ownership.
func (r *Router) Route(ctx context.Context, m sketch.Msg) error {
	if r == nil {
		return fmt.Errorf("handoffkit: route on a nil Router")
	}
	r.mu.RLock()
	mb, ok := r.boxes[m.To]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("handoffkit: no mailbox registered for address %q", m.To)
	}
	if mb == nil {
		return fmt.Errorf("handoffkit: nil mailbox registered for address %q", m.To)
	}
	return mb.Send(ctx, m)
}

// errIdle ends an agent's run loop after an idle period with no inbound work.
var errIdle = errors.New("handoffkit: idle")

// errInvalidIdle rejects Run loops that would otherwise omit the idle timeout
// and wait forever when called with a non-cancellable context.
var errInvalidIdle = errors.New("handoffkit: idle duration must be positive")

// tryRecv does a non-blocking receive from a mailbox via its Receiver channel.
// ok is false if nothing is ready, the mailbox is closed, or it exposes no
// channel.
func tryRecv(mb sketch.Mailbox) (sketch.Msg, bool) {
	r, ok := mb.(Receiver)
	if !ok {
		return sketch.Msg{}, false
	}
	select {
	case msg, recvOK := <-r.C():
		if !recvOK {
			return sketch.Msg{}, false // closed
		}
		return msg, true
	default:
		return sketch.Msg{}, false
	}
}

// Run drives one agent's single-owner loop: it waits on the inbox (or ctx
// cancellation / idle timeout) via a Selector, Steps on each inbound message,
// and routes the outputs to their destinations. It returns nil on idle or
// cancellation, or the first error a Step/Route produces. idle must be positive;
// zero or negative durations are rejected because Select treats them as no
// timeout case.
func Run(ctx context.Context, a sketch.Agent, r Dispatcher, idle time.Duration) error {
	return RunTraced(ctx, a, r, idle, nil)
}

// RunTraced is Run with an optional Tracer invoked on every message the agent
// receives (TraceRecv) and emits (TraceSend), a complete record of what the
// agent saw and communicated. A nil tracer is the zero-overhead Run path.
func RunTraced(ctx context.Context, a sketch.Agent, r Dispatcher, idle time.Duration, trace Tracer) error {
	if idle <= 0 {
		return fmt.Errorf("%w: %s", errInvalidIdle, idle)
	}
	sel := NewSelector()
	for {
		var got sketch.Msg
		var have bool
		_, err := sel.Run(ctx, sketch.Select{Cases: []sketch.Case{
			{Mailbox: a.Inbox(), OnRecv: func(m sketch.Msg) error { got, have = m, true; return nil }},
			{After: idle, OnAfter: func() error { return errIdle }},
		}})
		switch {
		case errors.Is(err, errIdle):
			// The idle timer and a freshly-arrived message can both be ready in
			// the same Select; if the timer was chosen, drain one ready message
			// before exiting so it is not orphaned in the inbox.
			if msg, ok := tryRecv(a.Inbox()); ok {
				got, have = msg, true
			} else {
				return nil
			}
		case errors.Is(err, context.Canceled),
			errors.Is(err, context.DeadlineExceeded):
			return nil
		case err != nil:
			return err
		}
		if !have {
			return nil
		}
		if trace != nil {
			trace(TraceEvent{Agent: a.Address(), Dir: TraceRecv, Msg: got})
		}
		outs, err := a.Step(ctx, got)
		if err != nil {
			return err
		}
		for _, o := range outs {
			if trace != nil {
				// TraceSend records what the agent emitted; delivery (Route below)
				// is a separate step that may still fail.
				trace(TraceEvent{Agent: a.Address(), Dir: TraceSend, Msg: o})
			}
			if o.To == "" {
				continue // terminal output; nothing to route
			}
			if err := r.Route(ctx, o); err != nil {
				return err
			}
		}
	}
}
