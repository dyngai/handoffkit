package runtime

import (
	"context"
	"fmt"
	"sync"

	"github.com/dyngai/handoffkit/sketch"
)

// node is one agent's place in the supervision tree.
type node struct {
	parent   sketch.Address
	depth    int
	children map[sketch.Address]struct{}
	ctx      context.Context
	cancel   context.CancelFunc
}

// Nursery is a structured-concurrency Supervisor. It owns a tree of agents and
// turns the design's safety levers into running code:
//
//   - Spawn enforces a depth/lineage guard. A coordinator sits at depth 0; with
//     maxDepth == 1 it may fan out to leaf workers at depth 1 that cannot spawn
//     further (a worker spawning would reach depth 2 and is rejected). This is
//     the "depth-capped, no lateral delegation" default from primitives.md, made
//     mechanical rather than aspirational.
//   - Route enforces who-may-message-whom: only parent<->child edges are
//     deliverable, so a worker cannot message a sibling (lateral delegation) or
//     an arbitrary node across the tree. External seeding (From == "") is allowed
//     so a driver can inject work into the root.
//   - Cancel unwinds an agent and its whole spawned subtree by cancelling each
//     descendant's context (the supervised run loops exit) and dropping them from
//     the tree and the delivery table.
//
// A Nursery is the answer to "an unconstrained agent mesh produces spawn cascades
// and unbounded lineage": the topology is a value you can inspect and bound, not
// an emergent property of who happened to call whom. It is safe for concurrent
// use. It implements both sketch.Supervisor and runtime.Dispatcher, so an
// agent's outputs can be delivered through its topology guard by passing the
// Nursery to runtime.Run.
type Nursery struct {
	mu       sync.Mutex
	router   *Router
	nodes    map[sketch.Address]*node
	maxDepth int
	rootCtx  context.Context
}

// NewNursery returns a Nursery whose agents derive their contexts from ctx (a
// nil ctx is treated as context.Background()). maxDepth bounds lineage depth:
// the root is depth 0, so maxDepth == 1 is the canonical coordinator + leaf
// workers shape. A negative maxDepth is clamped to 0 (root-only, no children).
func NewNursery(ctx context.Context, maxDepth int) *Nursery {
	if ctx == nil {
		ctx = context.Background()
	}
	if maxDepth < 0 {
		maxDepth = 0
	}
	return &Nursery{
		router:   NewRouter(),
		nodes:    make(map[sketch.Address]*node),
		maxDepth: maxDepth,
		rootCtx:  ctx,
	}
}

// Spawn registers a under parent, enforcing the depth/lineage guard, and binds
// its inbox in the delivery table. Pass parent == "" to register a root (depth
// 0). It returns an error if parent is unknown, the address is already spawned,
// or the spawn would exceed maxDepth.
func (n *Nursery) Spawn(_ context.Context, parent sketch.Address, a sketch.Agent) (sketch.Address, error) {
	if a == nil {
		return "", fmt.Errorf("handoffkit: spawn of a nil agent")
	}
	addr := a.Address()
	if addr == "" {
		return "", fmt.Errorf("handoffkit: spawn of an agent with an empty address")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.nodes[addr]; exists {
		return "", fmt.Errorf("handoffkit: address %q already spawned", addr)
	}

	depth := 0
	var pnode *node
	if parent != "" {
		pnode = n.nodes[parent]
		if pnode == nil {
			return "", fmt.Errorf("handoffkit: parent %q is not registered", parent)
		}
		depth = pnode.depth + 1
	}
	if depth > n.maxDepth {
		return "", fmt.Errorf(
			"handoffkit: spawn of %q under %q would reach depth %d, exceeding the cap %d (depth/lineage guard)",
			addr, parent, depth, n.maxDepth)
	}

	cctx, cancel := context.WithCancel(n.rootCtx)
	n.nodes[addr] = &node{
		parent:   parent,
		depth:    depth,
		children: make(map[sketch.Address]struct{}),
		ctx:      cctx,
		cancel:   cancel,
	}
	if pnode != nil {
		pnode.children[addr] = struct{}{}
	}
	n.router.Register(addr, a.Inbox())
	return addr, nil
}

// Route enforces the topology, then delegates delivery to the inner Router. A
// message is deliverable only if its From/To form a parent<->child edge, or it
// is external seeding (From == ""). It errors on an unknown destination, an
// unknown non-empty sender, or a topology violation (e.g. a sibling-to-sibling
// lateral message).
func (n *Nursery) Route(ctx context.Context, m sketch.Msg) error {
	if n == nil {
		return fmt.Errorf("handoffkit: route on a nil Nursery")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	n.mu.Lock()
	to, toOK := n.nodes[m.To]
	from, fromOK := n.nodes[m.From]
	n.mu.Unlock()

	if !toOK {
		return fmt.Errorf("handoffkit: no agent registered for destination %q", m.To)
	}
	switch {
	case m.From == "":
		// External seeding: a driver injecting work into a (root) agent.
	case !fromOK:
		return fmt.Errorf("handoffkit: message from unregistered sender %q", m.From)
	case to.parent == m.From || from.parent == m.To:
		// Parent -> child, or child -> parent: the only allowed edges.
	default:
		return fmt.Errorf(
			"handoffkit: topology violation: %q may not message %q (only parent<->child edges are routable)",
			m.From, m.To)
	}

	// Deliver outside the lock: Send may block on an unbuffered/full mailbox.
	// Tie delivery to the destination's supervised context so Cancel can wake an
	// in-flight send even when the caller passed a non-cancellable context.
	routeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-to.ctx.Done():
			cancel()
		case <-routeCtx.Done():
		}
	}()
	return n.router.Route(routeCtx, m)
}

// Context returns the supervised context for addr, derived from the Nursery root
// and cancelled when addr (or an ancestor) is Cancel-ed. Pass it to runtime.Run
// so the run loop participates in subtree cancellation. ok is false for an
// unknown (or already-cancelled) address.
func (n *Nursery) Context(addr sketch.Address) (context.Context, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	nd, ok := n.nodes[addr]
	if !ok {
		return nil, false
	}
	return nd.ctx, true
}

// Depth returns the tree depth of addr (root == 0). ok is false if unknown.
func (n *Nursery) Depth(addr sketch.Address) (int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	nd, ok := n.nodes[addr]
	if !ok {
		return 0, false
	}
	return nd.depth, true
}

// Cancel unwinds root and its whole spawned subtree: each descendant's context
// is cancelled (its supervised run loop exits), and the agents are removed from
// the tree and the delivery table. It errors only if root is unknown.
func (n *Nursery) Cancel(_ context.Context, root sketch.Address) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.nodes[root]; !ok {
		return fmt.Errorf("handoffkit: cancel of unregistered address %q", root)
	}
	// Detach root from its parent so a parent's child set stays accurate; the
	// recursion then drops the subtree wholesale.
	if nd := n.nodes[root]; nd.parent != "" {
		if p := n.nodes[nd.parent]; p != nil {
			delete(p.children, root)
		}
	}
	n.cancelSubtreeLocked(root)
	return nil
}

// cancelSubtreeLocked cancels addr and every descendant depth-first. The caller
// holds n.mu.
func (n *Nursery) cancelSubtreeLocked(addr sketch.Address) {
	nd, ok := n.nodes[addr]
	if !ok {
		return
	}
	for child := range nd.children {
		n.cancelSubtreeLocked(child)
	}
	nd.cancel()
	delete(n.nodes, addr)
	n.router.Unregister(addr)
}

var (
	_ sketch.Supervisor = (*Nursery)(nil)
	_ Dispatcher        = (*Nursery)(nil)
)
