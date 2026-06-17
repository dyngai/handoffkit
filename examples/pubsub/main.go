// Command pubsub demonstrates broadcast (pub/sub) awareness: one publisher emits
// events through a Broker, and every subscriber is woken for EVERY event, in
// contrast to the worker pool, where each task goes to exactly one worker.
//
// Broadcast DOWN via the Broker; ack fan-in UP via the Router + a Select. No
// API and no credentials, pure runtime machinery.
//
//	go run ./examples/pubsub
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

// subscriber reacts to every event it receives and acks back so the publisher
// can confirm delivery. Each is a single-owner actor woken via its inbox.
type subscriber struct {
	addr  sketch.Address
	inbox sketch.Mailbox
	next  sketch.Address
}

func (s *subscriber) Address() sketch.Address { return s.addr }
func (s *subscriber) Inbox() sketch.Mailbox   { return s.inbox }

func (s *subscriber) Step(_ context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	fmt.Printf("  %-9s reacted to: %s\n", s.addr, in.Payload)
	return []sketch.Msg{{From: s.addr, To: s.next, Payload: in.Payload}}, nil
}

func main() {
	events := []string{"deploy.started", "deploy.finished", "rollback.requested"}
	subNames := []sketch.Address{"logger", "metrics", "notifier"}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	broker := runtime.NewBroker()
	router := runtime.NewRouter()
	acks := runtime.NewMailbox(len(events) * len(subNames))
	router.Register("acks", acks)

	var wg sync.WaitGroup
	for _, name := range subNames {
		inbox := runtime.NewMailbox(len(events)) // buffered so Publish never blocks
		s := &subscriber{addr: name, inbox: inbox, next: "acks"}
		broker.Subscribe(inbox)
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			if err := runtime.Run(ctx, a, router, 5*time.Second); err != nil {
				fmt.Fprintf(os.Stderr, "%s stopped: %v\n", a.Address(), err)
			}
		}(s)
	}

	// Publish every event to ALL subscribers.
	for _, e := range events {
		fmt.Printf("publish: %s\n", e)
		if err := broker.Publish(ctx, sketch.Msg{From: "publisher", Payload: e}); err != nil {
			fmt.Fprintln(os.Stderr, "publish:", err)
			os.Exit(1)
		}
	}

	// Every event reaches every subscriber → expect len(events)*len(subs) acks.
	want := len(events) * len(subNames)
	sel := runtime.NewSelector()
	got := 0
	for got < want {
		_, err := sel.Run(ctx, sketch.Select{Cases: []sketch.Case{
			{Mailbox: acks, OnRecv: func(sketch.Msg) error { got++; return nil }},
			{After: 10 * time.Second, OnAfter: func() error { return fmt.Errorf("timed out waiting for acks") }},
		}})
		if err != nil {
			fmt.Fprintln(os.Stderr, "collect:", err)
			break
		}
	}
	cancel()
	wg.Wait()

	fmt.Printf("\n=== %d/%d deliveries (every event reached every subscriber) ===\n", got, want)
}
