package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dyngai/handoffkit/sketch"
)

// Broker is point-to-MANY: Publish fans each message out to every subscriber's
// mailbox, so each subscriber is woken for every event. It is the
// broadcast counterpart to Router (point-to-point: one address → one mailbox).
//
//	Router, handoff / dispatch: a message goes to exactly ONE recipient.
//	Broker, pub/sub awareness:  a message goes to EVERY subscriber.
//
// Publish is synchronous fan-out with per-subscriber backpressure: a slow
// subscriber blocks the publisher. Buffer subscriber mailboxes to decouple.
//
// Broadcast is the blackboard creeping back in, reach for it only where
// agents genuinely need ambient awareness, not for ordinary coordination.
//
// A Broker holds a mutex; do not copy it after first use.
type Broker struct {
	mu   sync.RWMutex
	subs []sketch.Mailbox
}

// NewBroker returns an empty broker.
func NewBroker() *Broker { return &Broker{} }

// Subscribe registers a mailbox to receive every published message. A nil
// mailbox is ignored: it could never receive anything and would panic Publish.
func (b *Broker) Subscribe(mb sketch.Mailbox) {
	if mb == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, mb)
}

// Publish delivers the message to every subscriber, in subscription order,
// honoring ctx. A failing subscriber does not stop delivery to the others (true
// fan-out over errors). Delivery is sequential, though, so a blocking subscriber
// (full mailbox, no receiver) stalls the rest until ctx; see the backpressure
// note above and buffer subscriber mailboxes to decouple. It returns the joined
// error (each tagged with the subscriber index), or nil if all succeed or there
// are none.
//
// It operates on a snapshot of the subscribers taken at entry, so a subscriber
// added concurrently may or may not receive this message.
//
// Each subscriber receives a shallow copy: the Msg struct is copied by value,
// but its slice fields (Ctx.Thread, Ctx.Refs) are shared. Subscribers must treat
// a received message as read-only.
func (b *Broker) Publish(ctx context.Context, msg sketch.Msg) error {
	b.mu.RLock()
	subs := make([]sketch.Mailbox, len(b.subs))
	copy(subs, b.subs)
	b.mu.RUnlock()

	var errs []error
	for i, mb := range subs {
		if err := mb.Send(ctx, msg); err != nil {
			errs = append(errs, fmt.Errorf("subscriber %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}
