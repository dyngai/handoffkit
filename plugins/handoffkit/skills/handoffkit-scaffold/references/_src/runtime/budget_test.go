package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// Spend accumulates, Remaining/Spent track it, and crossing the total exhausts
// the budget (Remaining clamps to 0).
func TestBudget_SpendTracksAndClamps(t *testing.T) {
	b := NewBudget(100)
	if r := b.Spend(30); r != 70 {
		t.Fatalf("remaining after 30 = %d, want 70", r)
	}
	if b.Spent() != 30 || b.Remaining() != 70 {
		t.Fatalf("spent=%d remaining=%d, want 30/70", b.Spent(), b.Remaining())
	}
	if b.Exhausted() {
		t.Fatal("budget exhausted too early")
	}
	if r := b.Spend(80); r != 0 { // overspend: remaining clamps to 0
		t.Fatalf("remaining after overspend = %d, want 0", r)
	}
	if !b.Exhausted() {
		t.Fatal("budget should be exhausted after overspend")
	}
}

// The composition that justifies the primitive: an agent waiting on "message OR
// budget exhausted" via a Select proceeds on the budget the moment it is spent.
func TestBudget_ExhaustionFiresSelectCase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b := NewBudget(100)
	idle := NewMailbox(1) // empty: the mailbox case would block

	b.Spend(60) // not yet exhausted: the Select must NOT pick Done
	idx, err := NewSelector().Run(ctx, sketch.Select{Cases: []sketch.Case{
		{Done: b.Done(), OnDone: func() error { return errors.New("budget fired before exhaustion") }},
		{After: 30 * time.Millisecond, OnAfter: func() error { return nil }},
	}})
	if err != nil {
		t.Fatalf("pre-exhaustion select: %v", err)
	}
	if idx != 1 {
		t.Fatalf("pre-exhaustion chose case %d, want 1 (After)", idx)
	}

	b.Spend(50) // now spent=110 >= 100: exhausted, Done closes
	fired := false
	idx, err = NewSelector().Run(ctx, sketch.Select{Cases: []sketch.Case{
		{Mailbox: idle, OnRecv: func(sketch.Msg) error { return errors.New("mailbox should not fire") }},
		{Done: b.Done(), OnDone: func() error { fired = true; return nil }},
		{After: time.Second, OnAfter: func() error { return errors.New("budget Done did not fire") }},
	}})
	if err != nil {
		t.Fatalf("post-exhaustion select: %v", err)
	}
	if !fired || idx != 1 {
		t.Fatalf("budget Done did not win: fired=%v idx=%d", fired, idx)
	}
}

// A zero budget is exhausted from construction.
func TestBudget_ZeroIsExhaustedImmediately(t *testing.T) {
	b := NewBudget(0)
	if !b.Exhausted() {
		t.Fatal("a zero budget should be exhausted immediately")
	}
	select {
	case <-b.Done():
	default:
		t.Fatal("a zero budget's Done() should already be closed")
	}
}

// Concurrent Spend is race-clean and Done() closes exactly once.
func TestBudget_ConcurrentSpendClosesOnce(t *testing.T) {
	const writers = 32
	b := NewBudget(writers)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Spend(1)
		}()
	}
	wg.Wait()

	if b.Spent() != writers {
		t.Fatalf("spent = %d, want %d (a concurrent Spend was lost)", b.Spent(), writers)
	}
	if !b.Exhausted() || b.Remaining() != 0 {
		t.Fatalf("budget not exhausted after full spend: exhausted=%v remaining=%d", b.Exhausted(), b.Remaining())
	}
}

func TestBudget_RemainingZeroMeansDoneClosed(t *testing.T) {
	const attempts = 1000
	for i := 0; i < attempts; i++ {
		b := NewBudget(1)
		spent := make(chan struct{})
		go func() {
			b.Spend(1)
			close(spent)
		}()

		timeout := time.After(100 * time.Millisecond)
		for {
			if b.Remaining() == 0 {
				select {
				case <-b.Done():
				default:
					t.Fatal("Remaining reported zero before Done was closed")
				}
				<-spent
				break
			}
			select {
			case <-timeout:
				t.Fatal("timed out waiting for budget exhaustion")
			default:
			}
		}
	}
}
