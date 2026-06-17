package runtime

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

type stdSelector struct{}

// NewSelector returns a Selector that composes waits with reflect.Select, the
// agent analogue of Go's `select`. ctx cancellation is added as an implicit
// final case. A context with a nil Done() (e.g. context.Background()) provides
// no cancellation, so to guarantee a Select cannot block forever, pass a
// cancellable context OR include an After/Done case. runtime.Run rejects
// non-positive idle durations and supplies an idle After, so its loop is safe.
// See tradeoffs.md §4.
func NewSelector() sketch.Selector { return stdSelector{} }

func (stdSelector) Run(ctx context.Context, sel sketch.Select) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Prioritize cancellation: if ctx is already cancelled, return rather than
	// run a ready case.
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	const (
		kindMailbox = iota
		kindDone
		kindAfter
		kindCtx
	)
	// meta snapshots each case's handler at build time so dispatch after the
	// blocking select does not re-read sel.Cases. The build loop below still reads
	// sel.Cases synchronously before blocking, so a Select must not be mutated
	// concurrently with Run, as with any shared value.
	type meta struct {
		orig    int
		kind    int
		onRecv  func(sketch.Msg) error
		onDone  func() error
		onAfter func() error
	}

	rcases := make([]reflect.SelectCase, 0, len(sel.Cases)+1)
	metas := make([]meta, 0, len(sel.Cases)+1)

	// Stop any timers on return so a losing After case doesn't leak a timer until
	// it fires; this matters in hot loops like runtime.Run that re-Select often.
	var timers []*time.Timer
	defer func() {
		for _, t := range timers {
			t.Stop()
		}
	}()

	for i, c := range sel.Cases {
		// A Case must set at most one effective wait source. An After of zero or
		// less is treated as unset.
		waitSources := 0
		if c.Mailbox != nil {
			waitSources++
		}
		if c.Done != nil {
			waitSources++
		}
		if c.After > 0 {
			waitSources++
		}
		if waitSources > 1 {
			return -1, fmt.Errorf("handoffkit: case %d sets multiple wait sources; set exactly one of Mailbox, Done, or After", i)
		}

		switch {
		case c.Mailbox != nil:
			rv, ok := c.Mailbox.(Receiver)
			if !ok {
				return -1, fmt.Errorf("handoffkit: mailbox in case %d is not a runtime.Receiver", i)
			}
			rcases = append(rcases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(rv.C())})
			metas = append(metas, meta{orig: i, kind: kindMailbox, onRecv: c.OnRecv})
		case c.Done != nil:
			rcases = append(rcases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.Done)})
			metas = append(metas, meta{orig: i, kind: kindDone, onDone: c.OnDone})
		case c.After > 0:
			tm := time.NewTimer(c.After)
			timers = append(timers, tm)
			rcases = append(rcases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tm.C)})
			metas = append(metas, meta{orig: i, kind: kindAfter, onAfter: c.OnAfter})
		}
	}

	// Implicit cancellation case. An unbounded wait is a budget leak, not a hang.
	if done := ctx.Done(); done != nil {
		rcases = append(rcases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)})
		metas = append(metas, meta{orig: -1, kind: kindCtx})
	}
	if len(rcases) == 0 {
		return -1, fmt.Errorf("handoffkit: select has no live cases and context has no Done channel")
	}

	chosen, recv, recvOK := reflect.Select(rcases)
	m := metas[chosen]
	switch m.kind {
	case kindCtx:
		return -1, ctx.Err()
	case kindMailbox:
		if !recvOK {
			// The mailbox channel is closed: deliver no message rather than a
			// phantom zero-value receive. The case still "fires" (we return its
			// index) but OnRecv does not run, so callers like runtime.Run see no
			// message and can treat it as "inbox closed".
			return m.orig, nil
		}
		if m.onRecv != nil {
			msg, _ := recv.Interface().(sketch.Msg)
			return m.orig, m.onRecv(msg)
		}
	case kindDone:
		if m.onDone != nil {
			return m.orig, m.onDone()
		}
	case kindAfter:
		if m.onAfter != nil {
			return m.orig, m.onAfter()
		}
	}
	return m.orig, nil
}
