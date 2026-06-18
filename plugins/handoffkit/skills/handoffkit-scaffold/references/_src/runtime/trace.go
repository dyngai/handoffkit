package runtime

import "github.com/dyngai/handoffkit/sketch"

// TraceDir is the direction of a traced message relative to an agent.
type TraceDir string

const (
	TraceRecv TraceDir = "recv" // what the agent saw (an inbound message)
	TraceSend TraceDir = "send" // what the agent communicated (an outbound message)
)

// TraceEvent records one message crossing an agent's boundary in its run loop.
// Because all coordination is message-passing, the stream of TraceEvents is a
// COMPLETE record of WHICH messages every agent saw and communicated; there is
// no hidden shared channel to miss. It is not a total order: events carry no
// timestamp or sequence number, so only per-agent (call) order is implied, not
// a global ordering across concurrent agents. Msg owns copies of its slice
// fields, so later message mutations cannot rewrite the recorded event.
type TraceEvent struct {
	Agent sketch.Address
	Dir   TraceDir
	Msg   sketch.Msg
}

// Tracer observes every message an agent receives and emits. It runs inline on
// the agent's run loop, so it must be fast and must not panic: a slow Tracer
// stalls the agent and a panicking one crashes its goroutine. It is also called
// from each agent's goroutine, so it must be safe for concurrent use.
type Tracer func(TraceEvent)

func cloneTraceMsg(m sketch.Msg) sketch.Msg {
	m.Ctx = cloneTraceHandoffContext(m.Ctx)
	return m
}

func cloneTraceHandoffContext(ctx sketch.HandoffContext) sketch.HandoffContext {
	if ctx.Thread != nil {
		ctx.Thread = append([]sketch.Turn(nil), ctx.Thread...)
	}
	if ctx.Refs != nil {
		ctx.Refs = append([]sketch.MemoryRef(nil), ctx.Refs...)
	}
	return ctx
}
