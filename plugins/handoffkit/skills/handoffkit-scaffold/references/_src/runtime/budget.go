package runtime

import "sync"

// Budget is a shared, thread-safe resource counter with a cancellation channel.
// The unit is caller-defined (tokens, dollar-cents, API calls, wall-clock
// milliseconds); what matters is that it turns "we are out of budget" into a
// first-class, selectable event instead of an ad-hoc check scattered through the
// agents.
//
// It is the runtime answer to the design's "an unbounded wait is a budget leak,
// not a hang" rule (tradeoffs.md §4). Done() closes the moment the budget is
// exhausted, so a Budget composes with the two primitives that already exist:
//
//   - as a Select case, an agent waits on "message OR cancellation OR budget
//     exhausted": Select{Cases: []Case{{Mailbox: in, ...}, {Done: b.Done(), ...}}}
//   - as a Nursery trigger, a watcher unwinds a whole subtree when the run runs
//     out of budget: go func(){ <-b.Done(); nursery.Cancel(ctx, root) }()
//
// Spend is safe to call from many agent goroutines; Done() closes exactly once.
type Budget struct {
	mu        sync.Mutex
	total     int
	spent     int
	done      chan struct{}
	closeOnce sync.Once
}

// NewBudget returns a Budget with the given total. A negative total is clamped to
// 0; a total of 0 is exhausted immediately (Done() is already closed), the
// explicit "no budget" state.
func NewBudget(total int) *Budget {
	if total < 0 {
		total = 0
	}
	b := &Budget{total: total, done: make(chan struct{})}
	if total == 0 {
		b.closeOnce.Do(func() { close(b.done) })
	}
	return b
}

// Spend records consumption of n units (a negative n is treated as 0) and
// returns the remaining budget (never below 0). When cumulative spend reaches or
// exceeds the total it closes Done(); spending past exhaustion is allowed and
// keeps Done() closed.
func (b *Budget) Spend(n int) (remaining int) {
	if n < 0 {
		n = 0
	}
	b.mu.Lock()
	b.spent += n
	remaining = b.total - b.spent
	if remaining < 0 {
		remaining = 0
	}
	exhausted := b.spent >= b.total
	b.mu.Unlock()

	if exhausted {
		b.closeOnce.Do(func() { close(b.done) })
	}
	return remaining
}

// Done returns a channel that is closed when the budget is exhausted. It is the
// seam that lets a Budget be a Select case or a Nursery cancellation trigger.
func (b *Budget) Done() <-chan struct{} { return b.done }

// Remaining returns the units left (never below 0).
func (b *Budget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.total - b.spent
	if r < 0 {
		r = 0
	}
	return r
}

// Spent returns the cumulative units consumed.
func (b *Budget) Spent() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spent
}

// Total returns the configured budget ceiling.
func (b *Budget) Total() int { return b.total }

// Exhausted reports whether the budget is spent (Done() has closed).
func (b *Budget) Exhausted() bool {
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}
