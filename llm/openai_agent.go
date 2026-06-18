// Package llm provides sketch.Agent implementations backed by real LLM APIs.
// OpenAIAgent wraps one call to the OpenAI Responses API as a single actor Step.
package llm

import (
	"context"
	"fmt"
	"strings"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/responses"
)

// OpenAIAgent is an actor whose Step is one call to the OpenAI Responses API.
// Its private state is the model + system prompt (its "context window"). By
// default it ships the full output as a lossy prose Summary (the
// serialization-loss tradeoff made concrete, docs/tradeoffs.md §2); with
// WithCompactor it instead ships a bounded Summary plus corpus refs so a deep
// handoff chain stops growing the prompt.
type OpenAIAgent struct {
	addr    sketch.Address
	inbox   sketch.Mailbox
	client  openai.Client
	model   string
	system  string
	next    sketch.Address     // where to hand off the result; "" = terminal
	compact *runtime.Compactor // optional: bound + corpus-offload the handoff
	fullOut bool               // keep full output in Payload even for routed compacted messages
	seq     int                // per-step counter for unique corpus refs

	promptCorpus   sketch.Corpus // optional: resolve inbound Ctx.Refs into prompt text
	promptRefBytes int           // total resolved corpus bytes allowed in one prompt
}

// NewOpenAIAgent builds an OpenAI-backed actor. next is the address its result
// is handed to ("" means the agent produces a terminal, un-routed message).
func NewOpenAIAgent(addr sketch.Address, client openai.Client, model, system string, next sketch.Address, inbox sketch.Mailbox) *OpenAIAgent {
	return &OpenAIAgent{addr: addr, inbox: inbox, client: client, model: model, system: system, next: next, promptRefBytes: defaultPromptRefBytes}
}

// WithCompactor makes the agent project its output onto a bounded, corpus-backed
// handoff (via c) instead of shipping the full output as Summary. It returns the
// agent for chaining. Pass nil to keep the default full-output behavior.
func (a *OpenAIAgent) WithCompactor(c *runtime.Compactor) *OpenAIAgent {
	a.compact = c
	if c == nil {
		a.promptCorpus = nil
	} else {
		a.promptCorpus = c.Corpus()
	}
	return a
}

// WithPromptRefBytes sets the total byte budget for corpus ref content included
// in this agent's model prompt. It applies when WithCompactor supplies a corpus;
// pass 0 or less to disable inbound ref resolution.
func (a *OpenAIAgent) WithPromptRefBytes(max int) *OpenAIAgent {
	a.promptRefBytes = max
	return a
}

// WithFullOutputPayload keeps the complete model output in Msg.Payload even
// when the agent uses a Compactor and routes to another mailbox. The handoff
// context is still compacted; use this on a final routed agent whose mailbox
// output is the user-facing result.
func (a *OpenAIAgent) WithFullOutputPayload() *OpenAIAgent {
	a.fullOut = true
	return a
}

// Address implements sketch.Agent.
func (a *OpenAIAgent) Address() sketch.Address { return a.addr }

// Inbox implements sketch.Agent.
func (a *OpenAIAgent) Inbox() sketch.Mailbox { return a.inbox }

// Step runs one LLM call: it fuses this agent's private system prompt with the
// owned context handed to it (a lossy projection) and the task payload, then
// hands the result forward. Ownership transfers with the returned message; this
// agent does not touch the task again.
func (a *OpenAIAgent) Step(ctx context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	// Task and any handed-over context go in the user input; the system prompt
	// stays in Instructions so task/handoff text cannot override it.
	prompt, err := buildPromptWithCorpus(ctx, in, a.promptCorpus, a.promptRefBytes)
	if err != nil {
		return nil, err
	}

	resp, err := a.client.Responses.New(ctx, responses.ResponseNewParams{
		Model:        a.model,
		Instructions: openai.String(a.system),
		Input:        responses.ResponseNewParamsInputUnion{OfString: openai.String(prompt)},
		Store:        openai.Bool(false), // do not persist agent payloads/output server-side
	})
	if err != nil {
		return nil, err
	}
	if resp.Status != responses.ResponseStatusCompleted {
		return nil, fmt.Errorf("openai response not completed: status=%q", resp.Status)
	}
	out := resp.OutputText()
	if strings.TrimSpace(out) == "" {
		return nil, fmt.Errorf("openai agent %q produced empty output", a.addr)
	}

	// Project the output onto the handoff: full Summary by default, or a bounded
	// Summary + corpus refs when a Compactor is set (see buildHandoff).
	a.seq++
	hc, err := buildHandoff(ctx, a.compact, a.addr, a.seq, in.Ctx, out)
	if err != nil {
		return nil, err
	}
	return []sketch.Msg{{From: a.addr, To: a.next, Payload: outboundPayload(a.compact, a.next, out, hc, a.fullOut), Ctx: hc}}, nil
}

var _ sketch.Agent = (*OpenAIAgent)(nil)
