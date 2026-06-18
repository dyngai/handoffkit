package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// A waiting mailbox message wins over a not-yet-elapsed timeout, and OnRecv
// receives the exact message.
func TestSelector_MailboxFires(t *testing.T) {
	mb := NewMailbox(1)
	if err := mb.Send(context.Background(), sketch.Msg{Payload: "hi"}); err != nil {
		t.Fatalf("seed Send: %v", err)
	}

	var got sketch.Msg
	idx, err := NewSelector().Run(context.Background(), sketch.Select{Cases: []sketch.Case{
		{Mailbox: mb, OnRecv: func(m sketch.Msg) error { got = m; return nil }},
		{After: time.Second, OnAfter: func() error { return errors.New("timeout should not fire") }},
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if idx != 0 {
		t.Fatalf("chosen case = %d, want 0", idx)
	}
	if got.Payload != "hi" {
		t.Fatalf("OnRecv payload = %q, want %q", got.Payload, "hi")
	}
}

// With no message waiting, the After case fires and OnAfter runs.
func TestSelector_AfterFires(t *testing.T) {
	mb := NewMailbox(1) // empty

	fired := false
	idx, err := NewSelector().Run(context.Background(), sketch.Select{Cases: []sketch.Case{
		{Mailbox: mb, OnRecv: func(sketch.Msg) error { return errors.New("mailbox should not fire") }},
		{After: 20 * time.Millisecond, OnAfter: func() error { fired = true; return nil }},
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if idx != 1 {
		t.Fatalf("chosen case = %d, want 1", idx)
	}
	if !fired {
		t.Fatal("OnAfter was not invoked")
	}
}

// A cancelled context selects the implicit final case: Run returns -1 and the
// context error, so no Select can block forever (tradeoffs.md §4).
func TestSelector_ContextCancelWins(t *testing.T) {
	mb := NewMailbox(1) // empty, would otherwise block
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	idx, err := NewSelector().Run(ctx, sketch.Select{Cases: []sketch.Case{
		{Mailbox: mb, OnRecv: func(sketch.Msg) error { return nil }},
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if idx != -1 {
		t.Fatalf("chosen case = %d, want -1 on cancellation", idx)
	}
}

func TestSelector_RejectsMultipleWaitSources(t *testing.T) {
	mb := NewMailbox(1)
	if err := mb.Send(context.Background(), sketch.Msg{Payload: "hi"}); err != nil {
		t.Fatalf("seed Send: %v", err)
	}
	done := make(chan struct{})
	close(done)

	var fired bool
	idx, err := NewSelector().Run(context.Background(), sketch.Select{Cases: []sketch.Case{
		{
			Mailbox: mb,
			OnRecv:  func(sketch.Msg) error { fired = true; return nil },
			Done:    done,
			OnDone:  func() error { fired = true; return nil },
			After:   time.Second,
			OnAfter: func() error { fired = true; return nil },
		},
	}})
	if err == nil {
		t.Fatal("expected an error for a case with multiple wait sources")
	}
	if !strings.Contains(err.Error(), "multiple wait sources") {
		t.Fatalf("err = %v, want multiple wait sources", err)
	}
	if idx != -1 {
		t.Fatalf("chosen case = %d, want -1 on error", idx)
	}
	if fired {
		t.Fatal("handler fired for an invalid case")
	}
}

func TestSelector_NoLiveCasesWithNilContextDoneErrors(t *testing.T) {
	done := make(chan error, 1)
	go func() {
		_, err := NewSelector().Run(context.Background(), sketch.Select{})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error for a select with no live cases")
		}
		if !strings.Contains(err.Error(), "no live cases") {
			t.Fatalf("err = %v, want no live cases", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run blocked with no live cases and a context whose Done channel is nil")
	}
}

// A ready Done channel fires its OnDone handler.
func TestSelector_DoneFires(t *testing.T) {
	done := make(chan struct{})
	close(done)

	fired := false
	idx, err := NewSelector().Run(context.Background(), sketch.Select{Cases: []sketch.Case{
		{Done: done, OnDone: func() error { fired = true; return nil }},
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if idx != 0 || !fired {
		t.Fatalf("Done case did not fire: idx=%d fired=%v", idx, fired)
	}
}

// An error returned by a case handler propagates out of Run.
func TestSelector_HandlerErrorPropagates(t *testing.T) {
	mb := NewMailbox(1)
	if err := mb.Send(context.Background(), sketch.Msg{Payload: "x"}); err != nil {
		t.Fatalf("seed Send: %v", err)
	}

	boom := errors.New("boom")
	_, err := NewSelector().Run(context.Background(), sketch.Select{Cases: []sketch.Case{
		{Mailbox: mb, OnRecv: func(sketch.Msg) error { return boom }},
	}})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

// nonReceiverMailbox satisfies sketch.Mailbox but NOT runtime.Receiver, so the
// Selector cannot wait on it and must report a clear error.
type nonReceiverMailbox struct{}

func (nonReceiverMailbox) Send(context.Context, sketch.Msg) error { return nil }
func (nonReceiverMailbox) Recv(context.Context) (sketch.Msg, bool, error) {
	return sketch.Msg{}, false, nil
}
func (nonReceiverMailbox) Close() error { return nil }

func TestSelector_NonReceiverMailboxErrors(t *testing.T) {
	idx, err := NewSelector().Run(context.Background(), sketch.Select{Cases: []sketch.Case{
		{Mailbox: nonReceiverMailbox{}, OnRecv: func(sketch.Msg) error { return nil }},
	}})
	if err == nil {
		t.Fatal("expected an error for a non-Receiver mailbox")
	}
	if !strings.Contains(err.Error(), "runtime.Receiver") {
		t.Fatalf("err = %v, want runtime.Receiver requirement", err)
	}
	if idx != -1 {
		t.Fatalf("chosen case = %d, want -1 on error", idx)
	}
}

// A closed mailbox must NOT fire OnRecv with a phantom zero-value message: the
// case fires (its index is returned) but no message is delivered, so a caller
// like runtime.Run sees "no message" and can treat it as inbox-closed.
func TestSelector_ClosedMailboxNoPhantomReceive(t *testing.T) {
	mb := NewMailbox(1)
	if err := mb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fired := false
	idx, err := NewSelector().Run(context.Background(), sketch.Select{Cases: []sketch.Case{
		{Mailbox: mb, OnRecv: func(sketch.Msg) error { fired = true; return nil }},
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fired {
		t.Fatal("OnRecv fired on a closed mailbox (phantom zero-value receive)")
	}
	if idx != 0 {
		t.Fatalf("chosen case = %d, want 0 (the closed mailbox case)", idx)
	}
}

// A nil context is treated as context.Background() instead of panicking.
func TestSelector_NilContext(t *testing.T) {
	mb := NewMailbox(1)
	if err := mb.Send(context.Background(), sketch.Msg{Payload: "x"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var got string
	_, err := NewSelector().Run(nil, sketch.Select{Cases: []sketch.Case{
		{Mailbox: mb, OnRecv: func(m sketch.Msg) error { got = m.Payload; return nil }},
	}})
	if err != nil {
		t.Fatalf("Run(nil ctx): %v", err)
	}
	if got != "x" {
		t.Fatalf("got %q, want x", got)
	}
}
