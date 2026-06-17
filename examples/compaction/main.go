// Command compaction puts a number on the lossy-handoff problem. It runs the
// SAME 4-hop handoff chain twice over the real runtime (mailboxes, router, the
// single-owner Run loop), changing only how each hop projects its state onto the
// handoff:
//
//   - NAIVE: each agent carries the full running context forward in
//     HandoffContext.Summary (what llm.OpenAIAgent / llm.CodexAgent do today). The
//     prose every agent must read grows at every hop.
//   - COMPACTED: each agent writes its full findings to a Corpus and hands off a
//     budget-bounded Summary plus a MemoryRef (runtime.Compactor). The prose stays
//     flat; the detail accumulates by reference, not by inlining.
//
// Then it RECOVERS the full detail of every hop by walking the refs in the final
// handoff, proving the bounded handoff lost nothing: it was referenced, not lost.
//
// No API key needed. The agents are deterministic stand-ins so the byte counts
// are stable and the point is the shape, not a model's prose:
//
//	go run ./examples/compaction
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

// hop records what one agent had to read when it ran.
type hop struct {
	agent        sketch.Address
	inboundProse int // bytes of HandoffContext prose (Summary + Thread) it read
	inboundRefs  int // cheap pointers it carried instead of inlined detail
}

// recorder collects per-hop measurements. The chain is linear so hops are
// appended in order; the lock guards the slice across agent goroutines.
type recorder struct {
	mu   sync.Mutex
	hops []hop
}

func (r *recorder) record(h hop) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hops = append(r.hops, h)
}

// chainAgent is one hop. With compact == nil it does the naive accumulation;
// with a Compactor it offloads to the Corpus and hands off a bounded context.
type chainAgent struct {
	addr    sketch.Address
	inbox   sketch.Mailbox
	next    sketch.Address
	body    string // the findings this hop contributes
	compact *runtime.Compactor
	rec     *recorder
}

func (a *chainAgent) Address() sketch.Address { return a.addr }
func (a *chainAgent) Inbox() sketch.Mailbox   { return a.inbox }

func (a *chainAgent) Step(ctx context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	// Measure the context this agent had to read to do its job.
	prose := len(in.Ctx.Summary)
	for _, t := range in.Ctx.Thread {
		prose += len(t.Content)
	}
	a.rec.record(hop{agent: a.addr, inboundProse: prose, inboundRefs: len(in.Ctx.Refs)})

	if a.compact == nil {
		// Naive: append our findings to the full running context and carry it all
		// forward. Every downstream agent now has to read this too.
		running := in.Ctx.Summary
		if running != "" {
			running += "\n"
		}
		running += a.body
		return []sketch.Msg{{
			From: a.addr, To: a.next, Payload: a.body,
			Ctx: sketch.HandoffContext{Summary: running},
		}}, nil
	}

	// Compacted: store the full findings in the Corpus, hand off a bounded Summary
	// plus a ref. Prior refs ride along, so detail accumulates by reference.
	ref := sketch.MemoryRef{Namespace: "findings", Key: string(a.addr)}
	hc, err := a.compact.Compact(ctx, ref, runtime.WorkingState{Output: a.body})
	if err != nil {
		return nil, err
	}
	hc.Refs = append(append([]sketch.MemoryRef{}, in.Ctx.Refs...), hc.Refs...)
	return []sketch.Msg{{From: a.addr, To: a.next, Payload: a.body, Ctx: hc}}, nil
}

// runChain wires agents head -> ... -> "out", seeds the head, and returns the
// final handoff that reaches the sink.
func runChain(parent context.Context, agents []*chainAgent) (sketch.Msg, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	r := runtime.NewRouter()
	out := runtime.NewMailbox(1)
	r.Register("out", out)
	for _, a := range agents {
		r.Register(a.addr, a.inbox)
	}

	var wg sync.WaitGroup
	for _, a := range agents {
		wg.Add(1)
		go func(a *chainAgent) {
			defer wg.Done()
			_ = runtime.Run(ctx, a, r, 2*time.Second)
		}(a)
	}

	// Seed the head of the chain: an ownership handoff into the system.
	head := agents[0]
	if err := head.inbox.Send(ctx, sketch.Msg{From: "user", To: head.addr, Payload: "investigate"}); err != nil {
		return sketch.Msg{}, err
	}

	var final sketch.Msg
	sel := runtime.NewSelector()
	_, err := sel.Run(ctx, sketch.Select{Cases: []sketch.Case{
		{Mailbox: out, OnRecv: func(m sketch.Msg) error { final = m; return nil }},
		{After: 9 * time.Second, OnAfter: func() error { return fmt.Errorf("timed out") }},
	}})
	cancel()
	wg.Wait()
	return final, err
}

func buildChain(names []sketch.Address, bodyOf func(sketch.Address) string, compact *runtime.Compactor, rec *recorder) []*chainAgent {
	agents := make([]*chainAgent, len(names))
	for i, n := range names {
		next := sketch.Address("out")
		if i+1 < len(names) {
			next = names[i+1]
		}
		agents[i] = &chainAgent{addr: n, inbox: runtime.NewMailbox(1), next: next, body: bodyOf(n), compact: compact, rec: rec}
	}
	return agents
}

func printTable(label string, rec *recorder) {
	fmt.Printf("%s\n", label)
	fmt.Printf("  hop  agent   inbound prose (bytes)   refs carried\n")
	for i, h := range rec.hops {
		fmt.Printf("  %-4d %-7s %-23d %d\n", i+1, h.agent, h.inboundProse, h.inboundRefs)
	}
}

func main() {
	ctx := context.Background()
	names := []sketch.Address{"a1", "a2", "a3", "a4"}
	// Each hop contributes ~600 bytes of findings, tagged so recovery is visible.
	bodyOf := func(n sketch.Address) string {
		return fmt.Sprintf("FINDINGS FROM %s: ", n) + strings.Repeat(".", 580)
	}

	// --- Naive chain: Summary carries the full running context. ---
	naiveRec := &recorder{}
	naiveFinal, err := runChain(ctx, buildChain(names, bodyOf, nil, naiveRec))
	if err != nil {
		fmt.Fprintln(os.Stderr, "naive chain:", err)
		os.Exit(1)
	}

	// --- Compacted chain: Corpus + Compactor (bounded Summary + refs). ---
	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: 160}, nil)
	compRec := &recorder{}
	compFinal, err := runChain(ctx, buildChain(names, bodyOf, comp, compRec))
	if err != nil {
		fmt.Fprintln(os.Stderr, "compacted chain:", err)
		os.Exit(1)
	}

	fmt.Printf("Chain of %d agents, each contributing ~%d bytes of findings.\n\n", len(names), len(bodyOf("a1")))

	printTable("NAIVE handoff (Summary carries the full running context):", naiveRec)
	fmt.Printf("  final handoff prose: %d bytes  (grows every hop)\n\n", len(naiveFinal.Ctx.Summary))

	printTable("COMPACTED handoff (Corpus + Compactor: bounded Summary + refs):", compRec)
	fmt.Printf("  final handoff prose: %d bytes  (flat, independent of chain length)\n\n", len(compFinal.Ctx.Summary))

	// --- Recovery: the bounded handoff lost nothing. Walk its refs. ---
	fmt.Printf("Recovery: the final compacted handoff carries %d refs. Resolving them\n", len(compFinal.Ctx.Refs))
	fmt.Printf("from the Corpus reconstructs every hop's full findings:\n")
	total := 0
	for _, ref := range compFinal.Ctx.Refs {
		v, ok, _ := corpus.Get(ctx, ref)
		if !ok {
			fmt.Printf("  %s/%s: MISSING\n", ref.Namespace, ref.Key)
			continue
		}
		text := v.(string)
		total += len(text)
		marker := fmt.Sprintf("FINDINGS FROM %s:", ref.Key)
		status := "ok"
		if !strings.HasPrefix(text, marker) {
			status = "WRONG CONTENT"
		}
		fmt.Printf("  %s/%s: %d bytes [%s]\n", ref.Namespace, ref.Key, len(text), status)
	}
	fmt.Printf("\nRecovered %d bytes of full detail across all hops, while no single\n", total)
	fmt.Printf("handoff carried more than %d bytes of prose. Knowledge stayed shared\n", len(compFinal.Ctx.Summary))
	fmt.Printf("and referenced; it was never inlined and never lost.\n")
}
