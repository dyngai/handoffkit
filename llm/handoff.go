package llm

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

// buildPrompt turns the owned message state into the user-facing text sent to
// an LLM. HandoffContext.Thread is included verbatim because callers use it for
// recent turns that should survive a handoff.
func buildPrompt(in sketch.Msg) string {
	var b strings.Builder
	if in.Ctx.Summary != "" {
		b.WriteString("Context handed from ")
		b.WriteString(string(in.From))
		b.WriteString(":\n")
		b.WriteString(in.Ctx.Summary)
		b.WriteString("\n\n")
	}
	if len(in.Ctx.Thread) > 0 {
		b.WriteString("Recent thread handed from ")
		b.WriteString(string(in.From))
		b.WriteString(":\n")
		for _, turn := range in.Ctx.Thread {
			role := strings.TrimSpace(turn.Role)
			if role == "" {
				role = "turn"
			}
			b.WriteString(role)
			b.WriteString(":\n")
			b.WriteString(turn.Content)
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	if in.Payload != "" && !payloadDuplicatesContextSummary(in.Payload, in.Ctx) {
		b.WriteString("Task:\n")
		b.WriteString(in.Payload)
	}
	return b.String()
}

func payloadDuplicatesContextSummary(payload string, hc sketch.HandoffContext) bool {
	return payload != "" && payload == hc.Summary
}

var handoffRefSeq atomic.Uint64

func handoffRef(addr sketch.Address, seq int) sketch.MemoryRef {
	return sketch.MemoryRef{
		Namespace: "handoff",
		Key:       fmt.Sprintf("%s-%d-%d", addr, seq, handoffRefSeq.Add(1)),
	}
}

// buildHandoff projects an agent's output onto the HandoffContext it ships.
//
// With no Compactor it carries the full output as Summary, the lossy-but-heavy
// default both agents started with: faithful, but a long A->B->C chain grows the
// prompt at every hop because each agent reads the previous full output.
//
// With a Compactor it writes the full output to the corpus under a per-step ref
// and ships a budget-bounded Summary plus the accumulated refs, so the prose a
// downstream agent must read stays flat regardless of chain length while the
// dropped detail stays resolvable via the corpus. priorRefs (the inbound
// handoff's refs) ride along so the corpus trail is complete at the end of the
// chain. See examples/compaction for the measured difference.
func buildHandoff(ctx context.Context, compact *runtime.Compactor, addr sketch.Address, seq int, prior sketch.HandoffContext, out string) (sketch.HandoffContext, error) {
	if compact == nil {
		return sketch.HandoffContext{
			Summary: out,
			Thread:  append([]sketch.Turn{}, prior.Thread...),
			Refs:    append([]sketch.MemoryRef{}, prior.Refs...),
		}, nil
	}
	ref := handoffRef(addr, seq)
	hc, err := compact.Compact(ctx, ref, runtime.WorkingState{Output: out, Thread: prior.Thread})
	if err != nil {
		return sketch.HandoffContext{}, err
	}
	// Accumulate prior refs (copied, so the outbound handoff does not alias the
	// inbound slice) ahead of this step's ref.
	hc.Refs = append(append([]sketch.MemoryRef{}, prior.Refs...), hc.Refs...)
	return hc, nil
}

// outboundPayload returns what travels in Msg.Payload. Compacted routed
// handoffs carry only bounded prose by default; terminal messages and
// fullPayload routes keep the complete output for callers that directly consume
// the message as the user-facing result.
func outboundPayload(compact *runtime.Compactor, next sketch.Address, full string, hc sketch.HandoffContext, fullPayload bool) string {
	if compact != nil && next != "" && !fullPayload {
		return hc.Summary
	}
	return full
}
