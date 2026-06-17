//go:build integration

package llm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/sketch"
)

// codexClientOrSkip loads a Codex client or skips. LoadCodexClient now fails on a
// missing or expired token, so the skip covers both.
func codexClientOrSkip(t *testing.T) *CodexClient {
	t.Helper()
	c, err := LoadCodexClient()
	if err != nil {
		t.Skipf("no usable Codex credentials: %v", err)
	}
	return c
}

// TestCodexComplete_Integration exercises a single Responses round-trip.
func TestCodexComplete_Integration(t *testing.T) {
	c := codexClientOrSkip(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	out, err := c.Complete(ctx,
		"You are a test fixture. Reply with exactly one word.", "Reply with exactly: PONG")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if !strings.Contains(strings.ToUpper(out), "PONG") {
		t.Fatalf("expected PONG, got %q", out)
	}
}

// TestCodexAllFeatures_Integration runs every coordination shape (pipeline,
// pool, broadcast) against the Codex-token-backed API.
func TestCodexAllFeatures_Integration(t *testing.T) {
	c := codexClientOrSkip(t)
	runAllFeatures(t, func(addr sketch.Address, system string, next sketch.Address, inbox sketch.Mailbox) sketch.Agent {
		return NewCodexAgent(addr, c, system, next, inbox)
	})
}
