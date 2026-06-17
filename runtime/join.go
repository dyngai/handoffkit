package runtime

import (
	"context"
	"strings"

	"github.com/dyngai/handoffkit/sketch"
)

// JoinAgent is a fan-in barrier: it buffers inbound messages and emits ONE
// combined message only after `need` have arrived, then resets for the next
// batch. Between arrivals its single-owner Run loop pauses (blocked on the
// inbox): "wait for all my dependencies" expressed as an agent that stays
// silent until its inbox has delivered enough. Because Run calls Step
// sequentially for a given agent, the buffer needs no lock.
type JoinAgent struct {
	addr    sketch.Address
	inbox   sketch.Mailbox
	next    sketch.Address
	need    int
	combine func([]sketch.Msg) sketch.Msg
	buf     []sketch.Msg
}

// NewJoinAgent builds a barrier that emits combine(batch) to next after every
// `need` inbound messages (need is clamped to >= 1). The emitted message's
// From/To are set to the join's address and next; combine controls its payload
// (and any Ctx). A nil combine joins the batch payloads with newlines.
func NewJoinAgent(addr sketch.Address, inbox sketch.Mailbox, next sketch.Address, need int, combine func([]sketch.Msg) sketch.Msg) *JoinAgent {
	if need < 1 {
		need = 1
	}
	if combine == nil {
		combine = func(batch []sketch.Msg) sketch.Msg {
			parts := make([]string, len(batch))
			for i, m := range batch {
				parts[i] = m.Payload
			}
			return sketch.Msg{Payload: strings.Join(parts, "\n")}
		}
	}
	return &JoinAgent{addr: addr, inbox: inbox, next: next, need: need, combine: combine}
}

// Address implements sketch.Agent.
func (a *JoinAgent) Address() sketch.Address { return a.addr }

// Inbox implements sketch.Agent.
func (a *JoinAgent) Inbox() sketch.Mailbox { return a.inbox }

// Step buffers the inbound message and, once `need` have accumulated, emits the
// combined batch and resets. While the barrier is unmet it emits nothing and the
// run loop re-blocks on the inbox.
func (a *JoinAgent) Step(_ context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	a.buf = append(a.buf, in)
	if len(a.buf) < a.need {
		return nil, nil
	}
	batch := a.buf
	// Run combine before clearing the buffer: if a user-supplied combine panics,
	// the batch is not silently dropped.
	out := a.combine(batch)
	a.buf = nil
	out.From = a.addr
	out.To = a.next
	return []sketch.Msg{out}, nil
}

var _ sketch.Agent = (*JoinAgent)(nil)
