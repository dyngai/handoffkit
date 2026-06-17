//go:build integration

package llm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

// agentFactory builds a backend-specific Agent from the parts the coordination
// shapes need. It lets one set of integration assertions run against any
// backend (OpenAI SDK, Codex token, ...).
type agentFactory func(addr sketch.Address, system string, next sketch.Address, inbox sketch.Mailbox) sketch.Agent

// collectOne waits (via Select) for one message on mb, or fails on timeout.
func collectOne(t *testing.T, ctx context.Context, mb sketch.Mailbox) string {
	t.Helper()
	var got string
	_, err := runtime.NewSelector().Run(ctx, sketch.Select{Cases: []sketch.Case{
		{Mailbox: mb, OnRecv: func(m sketch.Msg) error { got = m.Payload; return nil }},
		{After: 2 * time.Minute, OnAfter: func() error { return fmt.Errorf("timed out waiting for a message") }},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// traceLogger returns a Tracer that logs every message an agent saw/sent via
// t.Logf, so `go test -v` prints the full per-message trace of each shape.
func traceLogger(t *testing.T) runtime.Tracer {
	return func(ev runtime.TraceEvent) {
		peer, verb := ev.Msg.From, "saw  from"
		if ev.Dir == runtime.TraceSend {
			peer, verb = ev.Msg.To, "sent to  "
		}
		t.Logf("    [%-8s] %s %-10s: %s", ev.Agent, verb, peer, oneLine(ev.Msg.Payload, 100))
	}
}

func oneLine(s string, n int) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// assertPipeline: dependent stages. A reference code injected with the task
// must survive task -> planner -> (handoff) -> writer -> out. Asserting the code
// appears in the final output proves information actually flowed through the
// topology, a dropped handoff or mis-route would lose it (whereas a "non-empty"
// check would still pass on a broken chain).
func assertPipeline(t *testing.T, f agentFactory) {
	t.Helper()
	tr := traceLogger(t)
	const code = "ZX9QK"

	plannerIn := runtime.NewMailbox(1)
	writerIn := runtime.NewMailbox(1)
	out := runtime.NewMailbox(1)

	planner := f("planner",
		"You are a planner. Produce a 2-line outline for the task. Include the reference code "+code+" verbatim on its own line.",
		"writer", plannerIn)
	writer := f("writer",
		"You are a writer. Write one short paragraph from the outline you are handed, then on a new line output the exact reference code that appears in that outline.",
		"out", writerIn)

	r := runtime.NewRouter()
	r.Register("planner", plannerIn)
	r.Register("writer", writerIn)
	r.Register("out", out)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	for _, a := range []sketch.Agent{planner, writer} {
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			if err := runtime.RunTraced(ctx, a, r, 20*time.Second, tr); err != nil {
				t.Errorf("agent %s: %v", a.Address(), err)
			}
		}(a)
	}

	if err := plannerIn.Send(ctx, sketch.Msg{From: "user", To: "planner", Payload: "Explain why message passing beats shared memory for agents."}); err != nil {
		t.Fatalf("send: %v", err)
	}
	final := collectOne(t, ctx, out)
	cancel()
	wg.Wait()

	if !strings.Contains(final, code) {
		t.Fatalf("pipeline: reference code %q did not survive task->planner->writer->out; final=%q", code, final)
	}
}

// assertPool: independent fan-out across 2 workers and 3 tasks (so one worker
// handles two, exercising load-balancing). Each task carries a distinct token
// the worker must transform (uppercase); every transformed token must come back
// exactly once. This proves exactly-once dispatch AND that each worker actually
// processed the specific task it was handed, not just that messages moved.
func assertPool(t *testing.T, f agentFactory) {
	t.Helper()
	tr := traceLogger(t)
	tokens := []string{"alpha7", "bravo3", "delta9"}

	queue := runtime.NewMailbox(len(tokens))
	results := runtime.NewMailbox(len(tokens))
	r := runtime.NewRouter()
	r.Register("results", results)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		w := f(sketch.Address(fmt.Sprintf("w%d", i)),
			"Uppercase the token the user gives you and reply with ONLY the uppercased token, nothing else.", "results", queue)
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			_ = runtime.RunTraced(ctx, a, r, 20*time.Second, tr)
		}(w)
	}

	for _, tok := range tokens {
		if err := queue.Send(ctx, sketch.Msg{From: "dispatcher", To: "queue", Payload: "Token: " + tok}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	seen := map[string]int{}
	for range tokens {
		up := strings.ToUpper(collectOne(t, ctx, results))
		for _, tok := range tokens {
			if strings.Contains(up, strings.ToUpper(tok)) {
				seen[tok]++
			}
		}
	}
	cancel()
	wg.Wait()

	for _, tok := range tokens {
		if seen[tok] != 1 {
			t.Fatalf("pool: token %q processed %d times across workers, want exactly once", tok, seen[tok])
		}
	}
}

// assertBroadcast: pub/sub. A code embedded in the broadcast must be echoed by
// EVERY subscriber. Checking each ack carries the code proves every subscriber
// actually saw the event's content, not merely that it got woken and replied
// (which a constant "ACK" would not distinguish from a blind ping).
func assertBroadcast(t *testing.T, f agentFactory) {
	t.Helper()
	tr := traceLogger(t)
	const nSubs = 5
	const code = "QK42"

	broker := runtime.NewBroker()
	acks := runtime.NewMailbox(nSubs)
	r := runtime.NewRouter()
	r.Register("acks", acks)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < nSubs; i++ {
		inbox := runtime.NewMailbox(1)
		s := f(sketch.Address(fmt.Sprintf("sub%d", i)),
			"Reply with ONLY the exact code the user sends, nothing else.", "acks", inbox)
		broker.Subscribe(inbox)
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			_ = runtime.RunTraced(ctx, a, r, 20*time.Second, tr)
		}(s)
	}

	if err := broker.Publish(ctx, sketch.Msg{From: "publisher", Payload: "Code: " + code}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	withCode := 0
	for i := 0; i < nSubs; i++ {
		if strings.Contains(strings.ToUpper(collectOne(t, ctx, acks)), code) {
			withCode++
		}
	}
	cancel()
	wg.Wait()

	if withCode != nSubs {
		t.Fatalf("broadcast: %d/%d subscribers echoed the broadcast code %q", withCode, nSubs, code)
	}
}

// assertBroadcastJoin: a broadcast triggers N workers; a downstream join agent
// DEPENDS on all N. It pauses, accumulating each worker's token, and only emits
// once every worker has reported, a barrier built purely by blocking on the
// inbox. The joined output must contain every worker's marker, proving the join
// waited for all of its dependencies rather than firing early.
func assertBroadcastJoin(t *testing.T, f agentFactory) {
	t.Helper()
	tr := traceLogger(t)
	const nWorkers = 3

	broker := runtime.NewBroker()
	joinIn := runtime.NewMailbox(nWorkers)
	out := runtime.NewMailbox(1)
	r := runtime.NewRouter()
	r.Register("join", joinIn)
	r.Register("out", out)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	markers := make([]string, nWorkers)
	for i := 0; i < nWorkers; i++ {
		marker := fmt.Sprintf("W%d", i)
		markers[i] = marker
		inbox := runtime.NewMailbox(1)
		w := f(sketch.Address(fmt.Sprintf("worker-%d", i)),
			"Reply with ONLY the exact token "+marker+", nothing else.", "join", inbox)
		broker.Subscribe(inbox)
		wg.Add(1)
		go func(a sketch.Agent) { defer wg.Done(); _ = runtime.RunTraced(ctx, a, r, 20*time.Second, tr) }(w)
	}

	// The join depends on all workers; it blocks until it has collected nWorkers.
	join := runtime.NewJoinAgent("join", joinIn, "out", nWorkers, func(batch []sketch.Msg) sketch.Msg {
		parts := make([]string, len(batch))
		for i, m := range batch {
			parts[i] = strings.ToUpper(strings.TrimSpace(m.Payload))
		}
		return sketch.Msg{Payload: strings.Join(parts, "|")}
	})
	wg.Add(1)
	go func() { defer wg.Done(); _ = runtime.RunTraced(ctx, join, r, 30*time.Second, tr) }()

	if err := broker.Publish(ctx, sketch.Msg{From: "trigger", Payload: "Report your token now."}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	joined := strings.ToUpper(collectOne(t, ctx, out))
	cancel()
	wg.Wait()

	for _, m := range markers {
		if !strings.Contains(joined, m) {
			t.Fatalf("join: %q missing from joined output %q, a dependency was not waited for", m, joined)
		}
	}
}

// runAllFeatures exercises every coordination shape against one backend.
func runAllFeatures(t *testing.T, f agentFactory) {
	t.Run("pipeline", func(t *testing.T) { assertPipeline(t, f) })
	t.Run("pool", func(t *testing.T) { assertPool(t, f) })
	t.Run("broadcast", func(t *testing.T) { assertBroadcast(t, f) })
	t.Run("broadcast-join", func(t *testing.T) { assertBroadcastJoin(t, f) })
}
