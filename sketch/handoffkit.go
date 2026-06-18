// Package sketch is a reference API sketch for a message-passing actor runtime for
// LLM agents. It is deliberately interface-only: there is no scheduler, no
// transport, and no LLM here. The point is to pin down the vocabulary from the
// design docs (../docs) in compilable Go so the shapes are unambiguous.
//
// Read ../docs/primitives.md alongside this file. The big caveat (handoff is
// lossy because an agent's real state, its context window, cannot be shipped
// over a channel without serialization) is in ../docs/tradeoffs.md.
package sketch

import (
	"context"
	"time"
)

// Address uniquely identifies an agent (actor). Messages are routed to it.
type Address string

// MemoryRef is a pointer INTO the shared corpus, not inlined memory. Handoffs
// carry references, not copies, channelling the whole corpus as prose is the
// lossy/expensive trap the design avoids (tradeoffs.md §1, §2).
type MemoryRef struct {
	Namespace string
	Key       string
}

// Turn is one entry in an agent-facing transcript.
type Turn struct {
	Role    string // e.g. "user", "assistant", "tool"
	Content string
}

// HandoffContext is the owned state that travels WITH a task on a handoff.
// It is intentionally a lossy projection of the sender's private context
// window: a short prose summary plus references into the shared corpus.
// Minimizing the loss here is the open research problem (tradeoffs.md).
type HandoffContext struct {
	Summary string      // deliberately-lossy prose projection of working state
	Thread  []Turn      // optional recent turns the receiver needs verbatim
	Refs    []MemoryRef // cheap, shared, conflict-free pointers into the corpus
}

// Msg is the envelope handed between agents. Payload is the agent-facing
// content (prose/tokens); Ctx accompanies it on ownership transfer.
type Msg struct {
	From    Address
	To      Address
	Payload string
	Ctx     HandoffContext
}

// Mailbox is a typed conduit between agents, the channel analogue.
//
// Buffering policy is the key knob:
//   - Unbuffered: Send blocks until a Recv is ready (a rendezvous). Gives
//     backpressure for free, a producer cannot flood a consumer's context
//     window. This is the sane default for agents.
//   - Buffered: Send blocks only when full; decouples producer/consumer timing
//     for fan-out worker pools.
//
// Closing rules mirror Go: only the sender closes; Send on a closed mailbox is
// a programming error; Recv on a closed-and-drained mailbox returns ok == false.
type Mailbox interface {
	// Send delivers m, blocking according to the buffering policy and honoring
	// ctx cancellation. A nil error means the receiver has taken ownership.
	Send(ctx context.Context, m Msg) error
	// Recv returns the next message. ok is false once the mailbox is closed and
	// drained.
	Recv(ctx context.Context) (m Msg, ok bool, err error)
	Close() error
}

// Agent is an actor: an address, a private inbox, and a single-owner step
// function. Its real state, the context window, is private and never shipped
// wholesale; that privacy is what makes message passing safe without locks.
type Agent interface {
	Address() Address
	Inbox() Mailbox
	// Step consumes one inbound message and may emit outbound messages. It
	// returns when the agent has produced its output or is blocked waiting.
	// Implementations must honor ctx cancellation, an unbounded wait is a
	// budget leak, not just a hang (tradeoffs.md §4).
	Step(ctx context.Context, in Msg) (out []Msg, err error)
}

// Case is one arm of a Select: a source to wait on and what to do when it fires.
type Case struct {
	// Mailbox is the source for this case. Exactly one of Mailbox / Done /
	// After should be set per Case. Concrete Selector implementations may
	// require extra mailbox capabilities to wait efficiently; runtime.NewSelector
	// requires a mailbox that also implements runtime.Receiver.
	Mailbox Mailbox
	OnRecv  func(Msg) error

	// Done fires on cancellation (the context.Done() analogue).
	Done   <-chan struct{}
	OnDone func() error

	// After fires after this duration. A zero (or negative) After is treated as
	// unset (no timeout case), not an immediate timeout. A Select with no After
	// and no Done relies on ctx cancellation to avoid blocking forever
	// (tradeoffs.md §4).
	After   time.Duration
	OnAfter func() error
}

// Select is the composer, the reason to prefer this model over a shared
// scratchpad. It blocks on several sources at once (a peer's reply, a user
// interrupt, budget exhaustion, a timeout) and proceeds on the first ready one.
// You cannot select over multiple mutexes, nor cleanly over polled shared state.
type Select struct {
	Cases []Case
}

// Run blocks until one Case fires and its handler returns. The chosen index is
// returned for tracing.
type Selector interface {
	Run(ctx context.Context, s Select) (chosen int, err error)
}

// Supervisor owns the topology: who may message whom, and who may spawn whom.
// Topology is a safety lever, an unconstrained mesh produces spawn cascades
// and unbounded lineage. A sane default constrains it (e.g. a coordinator fans
// out to leaf workers that cannot delegate laterally; depth-capped). Richer
// topologies should sit behind an explicit depth/lineage guard.
type Supervisor interface {
	// Spawn registers a new agent under a parent, enforcing topology policy.
	// It returns an error if the spawn would violate the depth/lineage guard.
	Spawn(ctx context.Context, parent Address, a Agent) (Address, error)
	// Route enforces who-may-message-whom before delivery.
	Route(ctx context.Context, m Msg) error
	// Cancel unwinds an agent and its spawned subtree (context.Done analogue).
	Cancel(ctx context.Context, root Address) error
}

// Corpus is the shared knowledge substrate, the one place the blackboard wins.
// It is deliberately NOT a Mailbox: knowledge stays put and is referenced, not
// channelled as prose. Concurrent writers are reconciled by conflict-free merge
// (CRDT), which is the mutex's job done right: guard the small shared thing.
type Corpus interface {
	Get(ctx context.Context, ref MemoryRef) (value any, ok bool, err error)
	// Merge applies a conflict-free update. Concurrent Merges must commute so
	// two agents writing the same key cannot corrupt each other.
	Merge(ctx context.Context, ref MemoryRef, delta any) error
}
