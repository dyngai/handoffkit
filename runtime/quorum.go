package runtime

import (
	"context"
	"strings"

	"github.com/dyngai/handoffkit/sketch"
)

// QuorumAgent is a fan-in that proceeds on the FIRST `need` of `total` expected
// replies rather than waiting for all of them, the opposite end of the spectrum
// from JoinAgent's all-N barrier. It is the shape behind hedged/speculative
// requests and N-of-M voting: fire `total` agents at a problem and act on the
// earliest quorum, treating the stragglers as already-too-late.
//
// Per round it emits combine(first `need` arrivals) exactly ONCE, then drains
// the remaining `total - need` messages without emitting (so slow upstreams do
// not block and the next round starts aligned), and resets. need == 1 is
// first-result-wins (a race); need == total degenerates to a JoinAgent-style
// barrier (but still single-shot per round). Because Run calls Step sequentially
// for one agent, the buffer and counters need no lock.
type QuorumAgent struct {
	addr    sketch.Address
	inbox   sketch.Mailbox
	next    sketch.Address
	need    int
	total   int
	combine func([]sketch.Msg) sketch.Msg
	buf     []sketch.Msg
	seen    int
	emitted bool
}

// NewQuorumAgent builds a quorum that emits combine(first `need`) to next once
// per round of `total` messages. need is clamped to >= 1; total is clamped to >=
// need (so a round always contains at least the quorum). The emitted message's
// From/To are set to the quorum's address and next; combine controls its payload
// and Ctx. A nil combine joins the quorum payloads with newlines.
func NewQuorumAgent(addr sketch.Address, inbox sketch.Mailbox, next sketch.Address, need, total int, combine func([]sketch.Msg) sketch.Msg) *QuorumAgent {
	if need < 1 {
		need = 1
	}
	if total < need {
		total = need
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
	return &QuorumAgent{addr: addr, inbox: inbox, next: next, need: need, total: total, combine: combine}
}

// Address implements sketch.Agent.
func (a *QuorumAgent) Address() sketch.Address { return a.addr }

// Inbox implements sketch.Agent.
func (a *QuorumAgent) Inbox() sketch.Mailbox { return a.inbox }

// Step counts the arrival; emits the combined quorum once the `need`-th message
// of the round has arrived; and resets after the `total`-th so the next round is
// independent. Arrivals after the quorum is met are drained (counted) but not
// emitted, the stragglers a quorum deliberately ignores.
func (a *QuorumAgent) Step(_ context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	a.seen++

	var out []sketch.Msg
	if !a.emitted {
		a.buf = append(a.buf, in)
		if len(a.buf) >= a.need {
			// Run combine before clearing the buffer so a panicking user combine
			// does not silently drop the quorum.
			m := a.combine(a.buf)
			a.buf = nil
			a.emitted = true
			m.From = a.addr
			m.To = a.next
			out = []sketch.Msg{m}
		}
	}

	if a.seen >= a.total { // round complete: reset for the next one
		a.seen = 0
		a.emitted = false
		a.buf = nil
	}
	return out, nil
}

var _ sketch.Agent = (*QuorumAgent)(nil)
