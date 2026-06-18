// Command trace-all runs every coordination shape, pipeline, pool, broadcast,
// against the Codex backend with a tracer on, so you can SEE every message each
// agent saw and communicated across all features in one run.
//
// LOCAL, UNSUPPORTED demo behavior (uses the Codex CLI's ChatGPT token; run
// `codex login` if it is stale). Needs the Go 1.22+ toolchain on PATH:
//
//	PATH=/opt/homebrew/bin:$PATH go run ./examples/trace-all
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dyngai/handoffkit/llm"
	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

func main() {
	client, err := llm.LoadCodexClient()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// One tracer for the whole run; called from many goroutines, so guard it.
	var mu sync.Mutex
	trace := func(ev runtime.TraceEvent) {
		mu.Lock()
		defer mu.Unlock()
		if ev.Dir == runtime.TraceRecv {
			fmt.Printf("  [%-8s] saw  from %-10s: %s\n", ev.Agent, ev.Msg.From, truncate(ev.Msg.Payload, 90))
		} else {
			fmt.Printf("  [%-8s] sent to   %-10s: %s\n", ev.Agent, ev.Msg.To, truncate(ev.Msg.Payload, 90))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	fmt.Println("\n========== PIPELINE (planner -> writer -> out) ==========")
	if err := pipeline(ctx, client, trace); err != nil {
		fmt.Fprintln(os.Stderr, "pipeline failed:", err)
		os.Exit(1)
	}

	fmt.Println("\n========== POOL (2 workers share one queue, exactly-once) ==========")
	if err := pool(ctx, client, trace); err != nil {
		fmt.Fprintln(os.Stderr, "pool failed:", err)
		os.Exit(1)
	}

	fmt.Println("\n========== BROADCAST (Broker -> every subscriber) ==========")
	if err := broadcast(ctx, client, trace); err != nil {
		fmt.Fprintln(os.Stderr, "broadcast failed:", err)
		os.Exit(1)
	}

	fmt.Println("\nall features traced.")
}

func pipeline(ctx context.Context, c *llm.CodexClient, trace runtime.Tracer) error {
	plannerIn := runtime.NewMailbox(1)
	writerIn := runtime.NewMailbox(1)
	out := runtime.NewMailbox(1)
	planner := llm.NewCodexAgent("planner", c, "You are a planner. Produce a 2-line outline. No prose.", "writer", plannerIn)
	writer := llm.NewCodexAgent("writer", c, "You are a writer. Follow the outline. One short paragraph.", "out", writerIn)

	r := runtime.NewRouter()
	r.Register("planner", plannerIn)
	r.Register("writer", writerIn)
	r.Register("out", out)

	stop, agentErrs, wait := drive(ctx, trace, r, planner, writer)
	if err := plannerIn.Send(ctx, sketch.Msg{From: "user", To: "planner", Payload: "Why does message passing beat shared memory for agents?"}); err != nil {
		stop()
		return errors.Join(fmt.Errorf("send planner task: %w", err), wait())
	}
	if err := collectOne(ctx, out, agentErrs); err != nil {
		stop()
		return errors.Join(err, wait())
	}
	stop()
	return wait()
}

func pool(ctx context.Context, c *llm.CodexClient, trace runtime.Tracer) error {
	markers := []string{"ALPHA", "BETA"}
	queue := runtime.NewMailbox(len(markers))   // shared across workers, the tasklist
	results := runtime.NewMailbox(len(markers)) // fan-in
	r := runtime.NewRouter()
	r.Register("results", results)

	w0 := llm.NewCodexAgent("worker-0", c, "Reply with exactly the single uppercase word the user names, nothing else.", "results", queue)
	w1 := llm.NewCodexAgent("worker-1", c, "Reply with exactly the single uppercase word the user names, nothing else.", "results", queue)

	stop, agentErrs, wait := drive(ctx, trace, r, w0, w1)
	for _, m := range markers {
		if err := queue.Send(ctx, sketch.Msg{From: "dispatcher", To: "queue", Payload: "Reply with exactly: " + m}); err != nil {
			stop()
			return errors.Join(fmt.Errorf("send pool task %q: %w", m, err), wait())
		}
	}
	for range markers {
		if err := collectOne(ctx, results, agentErrs); err != nil {
			stop()
			return errors.Join(err, wait())
		}
	}
	stop()
	return wait()
}

func broadcast(ctx context.Context, c *llm.CodexClient, trace runtime.Tracer) error {
	broker := runtime.NewBroker()
	acks := runtime.NewMailbox(2)
	r := runtime.NewRouter()
	r.Register("acks", acks)

	var subs []sketch.Agent
	for i := 0; i < 2; i++ {
		inbox := runtime.NewMailbox(1)
		s := llm.NewCodexAgent(sketch.Address(fmt.Sprintf("sub-%d", i)), c, "Reply with exactly: ACK", "acks", inbox)
		broker.Subscribe(inbox)
		subs = append(subs, s)
	}

	stop, agentErrs, wait := drive(ctx, trace, r, subs...)
	if err := broker.Publish(ctx, sketch.Msg{From: "publisher", Payload: "Reply with exactly: ACK"}); err != nil {
		stop()
		return errors.Join(fmt.Errorf("publish broadcast: %w", err), wait())
	}
	for range subs {
		if err := collectOne(ctx, acks, agentErrs); err != nil {
			stop()
			return errors.Join(err, wait())
		}
	}
	stop()
	return wait()
}

// drive runs each agent on a child context (so a shape can be stopped before the
// next one starts) with the tracer attached.
func drive(ctx context.Context, trace runtime.Tracer, r *runtime.Router, agents ...sketch.Agent) (context.CancelFunc, <-chan error, func() error) {
	sctx, scancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	errs := make(chan error, len(agents))
	for _, a := range agents {
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			if err := runtime.RunTraced(sctx, a, r, 20*time.Second, trace); err != nil {
				errs <- fmt.Errorf("agent %s: %w", a.Address(), err)
			}
		}(a)
	}
	wait := func() error {
		wg.Wait()
		close(errs)
		var joined error
		for err := range errs {
			joined = errors.Join(joined, err)
		}
		return joined
	}
	return scancel, errs, wait
}

func collectOne(ctx context.Context, mb sketch.Mailbox, agentErrs <-chan error) error {
	recv, ok := mb.(runtime.Receiver)
	if !ok {
		return fmt.Errorf("mailbox is not a runtime receiver")
	}
	timer := time.NewTimer(2 * time.Minute)
	defer timer.Stop()
	select {
	case _, ok := <-recv.C():
		if !ok {
			return fmt.Errorf("mailbox closed")
		}
		return nil
	case err := <-agentErrs:
		return err
	case <-timer.C:
		return fmt.Errorf("timed out")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
