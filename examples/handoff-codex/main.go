// Command handoff-codex runs the same planner -> writer -> out handoff as
// examples/handoff, but backed by the Codex CLI's ChatGPT-account OAuth token
// (from ~/.codex/auth.json) via llm.CodexAgent, no OPENAI_API_KEY needed.
//
// This is a LOCAL, UNSUPPORTED convenience for running the demo off a ChatGPT
// plan (the token is short-lived, run `codex login` to refresh, and used
// outside its intended scope). It is deliberately left untracked.
package main

import (
	"context"
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

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	r := runtime.NewRouter()
	plannerIn := runtime.NewMailbox(1)
	writerIn := runtime.NewMailbox(1)
	out := runtime.NewMailbox(1)

	planner := llm.NewCodexAgent("planner", client,
		"You are a planner. Produce a tight numbered outline (max 4 points) for the task. Do not write the prose.",
		"writer", plannerIn)
	writer := llm.NewCodexAgent("writer", client,
		"You are a writer. Follow the handed-over outline exactly and produce the final answer. Be concise.",
		"out", writerIn)

	r.Register(planner.Address(), plannerIn)
	r.Register(writer.Address(), writerIn)
	r.Register("out", out)

	// Trace every message each agent saw and communicated. The tracer is called
	// from both agent goroutines, so guard the printing.
	var traceMu sync.Mutex
	trace := func(ev runtime.TraceEvent) {
		traceMu.Lock()
		defer traceMu.Unlock()
		if ev.Dir == runtime.TraceRecv {
			fmt.Printf("  [%-7s] saw  (from %-7s): %s\n", ev.Agent, ev.Msg.From, truncate(ev.Msg.Payload, 120))
		} else {
			fmt.Printf("  [%-7s] sent (to   %-7s): %s\n", ev.Agent, ev.Msg.To, truncate(ev.Msg.Payload, 120))
		}
	}

	var wg sync.WaitGroup
	for _, a := range []sketch.Agent{planner, writer} {
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			if err := runtime.RunTraced(ctx, a, r, 60*time.Second, trace); err != nil {
				fmt.Fprintf(os.Stderr, "agent %s stopped: %v\n", a.Address(), err)
			}
		}(a)
	}

	task := "Explain why message passing beats shared memory for coordinating concurrent agents."
	if err := plannerIn.Send(ctx, sketch.Msg{From: "user", To: "planner", Payload: task}); err != nil {
		fmt.Fprintln(os.Stderr, "send failed:", err)
		os.Exit(1)
	}

	var final string
	sel := runtime.NewSelector()
	_, serr := sel.Run(ctx, sketch.Select{Cases: []sketch.Case{
		{Mailbox: out, OnRecv: func(m sketch.Msg) error { final = m.Payload; return nil }},
		{After: 3 * time.Minute, OnAfter: func() error { return fmt.Errorf("timed out waiting for result") }},
	}})
	cancel()
	wg.Wait()

	if serr != nil {
		fmt.Fprintln(os.Stderr, "error:", serr)
		os.Exit(1)
	}
	fmt.Println("\n=== FINAL ANSWER (via Codex token / gpt-5.5) ===")
	fmt.Println(final)
}

// truncate collapses newlines and caps a payload for one-line trace output.
func truncate(s string, n int) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
