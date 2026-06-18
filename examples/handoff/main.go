// Command handoff runs two OpenAI-backed actors connected by mailboxes: a
// planner hands its outline off to a writer, which produces the final answer.
// It exercises the whole model end to end, channel-backed mailboxes, ownership
// handoff, address routing, and a Select that waits on "result OR timeout".
//
// Both agents run with a Compactor (WithCompactor), so each routed hop offloads
// its full output to a shared Corpus and hands off bounded prose plus a ref. The
// prose that travels stays bounded, yet nothing is lost: at the end this walks
// the refs the final handoff carried and recovers every hop's full text from the
// Corpus. (The prompt-growth difference is measured deterministically, with no
// API key, in examples/compaction.)
//
// Requires OPENAI_API_KEY in the environment:
//
//	OPENAI_API_KEY=sk-... go run ./examples/handoff
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dyngai/handoffkit/llm"
	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
	"github.com/openai/openai-go/v3"
)

func main() {
	if os.Getenv("OPENAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "set OPENAI_API_KEY to run this example")
		os.Exit(1)
	}

	client := openai.NewClient()
	model := string(openai.ChatModelGPT4o)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Wire the topology: planner -> writer -> out (a sink the main goroutine reads).
	r := runtime.NewRouter()
	plannerIn := runtime.NewMailbox(1)
	writerIn := runtime.NewMailbox(1)
	out := runtime.NewMailbox(1)

	// Shared knowledge substrate + a Compactor that bounds each handoff and
	// offloads the full output to the Corpus. A 600-byte budget lets a tight
	// outline pass through verbatim while bounding more verbose output.
	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: 600}, nil)

	planner := llm.NewOpenAIAgent(
		"planner", client, model,
		"You are a planner. Produce a tight numbered outline (max 4 points) for the task. Do not write the prose.",
		"writer", plannerIn,
	).WithCompactor(comp)
	writer := llm.NewOpenAIAgent(
		"writer", client, model,
		"You are a writer. Follow the handed-over outline exactly and produce the final answer. Be concise.",
		"out", writerIn,
	).WithCompactor(comp).WithFullOutputPayload()

	r.Register(planner.Address(), plannerIn)
	r.Register(writer.Address(), writerIn)
	r.Register("out", out)

	// Each agent runs as a goroutine on its single-owner loop.
	var wg sync.WaitGroup
	agents := []sketch.Agent{planner, writer}
	errCh := make(chan error, len(agents))
	for _, a := range agents {
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			if err := runtime.Run(ctx, a, r, 30*time.Second); err != nil {
				errCh <- fmt.Errorf("agent %s stopped: %w", a.Address(), err)
			}
		}(a)
	}

	// Inject the task into the planner, an ownership handoff into the system.
	task := "Explain why message passing beats shared memory for coordinating concurrent agents."
	if err := plannerIn.Send(ctx, sketch.Msg{From: "user", To: "planner", Payload: task}); err != nil {
		fmt.Fprintln(os.Stderr, "send failed:", err)
		os.Exit(1)
	}

	// Wait for the final result OR an agent error OR a timeout.
	var final sketch.Msg
	timer := time.NewTimer(80 * time.Second)
	defer timer.Stop()
	var err error
	select {
	case m, ok := <-out.C():
		if !ok {
			err = fmt.Errorf("out mailbox closed")
		} else {
			final = m
		}
	case err = <-errCh:
	case <-timer.C:
		err = fmt.Errorf("timed out waiting for result")
	case <-ctx.Done():
		err = ctx.Err()
	}
	cancel()
	wg.Wait()

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Println("=== FINAL ANSWER ===")
	fmt.Println(final.Payload)

	// The handoff that travelled was bounded, but nothing was lost: each hop's
	// full output is in the Corpus. Walk the refs the final handoff carried to
	// recover the complete trail.
	if len(final.Ctx.Refs) > 0 {
		fmt.Printf("\n=== HANDOFF TRAIL (recovered from the Corpus) ===\n")
		fmt.Printf("bounded handoff prose that travelled: %d bytes\n", len(final.Ctx.Summary))
		for _, ref := range final.Ctx.Refs {
			v, ok, _ := corpus.Get(context.Background(), ref)
			if !ok {
				fmt.Printf("  %s/%s: MISSING\n", ref.Namespace, ref.Key)
				continue
			}
			fmt.Printf("  %s/%s: %d bytes of full output retained\n", ref.Namespace, ref.Key, len(v.(string)))
		}
	}
}
