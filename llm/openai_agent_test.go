//go:build integration

package llm

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
	"github.com/openai/openai-go/v3"
)

// openaiTestModel is a cheap model sufficient for the integration assertions.
const openaiTestModel = "gpt-4o-mini"

func openAIClientOrSkip(t *testing.T) openai.Client {
	t.Helper()
	if os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("OPENAI_API_KEY not set; skipping OpenAI integration test")
	}
	return openai.NewClient()
}

// TestOpenAIAgentStep_Integration exercises a single Agent.Step round-trip.
func TestOpenAIAgentStep_Integration(t *testing.T) {
	client := openAIClientOrSkip(t)

	a := NewOpenAIAgent("probe", client, openaiTestModel,
		"You are a test fixture. Reply with exactly one word.", "", runtime.NewMailbox(1))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	out, err := a.Step(ctx, sketch.Msg{From: "user", To: "probe", Payload: "Reply with exactly: PONG"})
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("want 1 output message, got %d", len(out))
	}
	if !strings.Contains(strings.ToUpper(out[0].Payload), "PONG") {
		t.Fatalf("expected PONG in reply, got %q", out[0].Payload)
	}
}

// TestOpenAIAllFeatures_Integration runs every coordination shape (pipeline,
// pool, broadcast) against the OpenAI SDK backend.
func TestOpenAIAllFeatures_Integration(t *testing.T) {
	client := openAIClientOrSkip(t)
	runAllFeatures(t, func(addr sketch.Address, system string, next sketch.Address, inbox sketch.Mailbox) sketch.Agent {
		return NewOpenAIAgent(addr, client, openaiTestModel, system, next, inbox)
	})
}
