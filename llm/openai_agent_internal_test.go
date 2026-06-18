package llm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

func TestOpenAIAgentWithCompactorIncludesCorpusRefsInPrompt(t *testing.T) {
	const hidden = "hidden corpus detail: OPENAI_TOKEN"
	var capturedPrompt string
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body struct {
			Input string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		capturedPrompt = body.Input
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{
			"id": "resp_test",
			"object": "response",
			"created_at": 0,
			"model": "gpt-4o-mini",
			"status": "completed",
			"output": [{
				"type": "message",
				"id": "msg_test",
				"status": "completed",
				"role": "assistant",
				"content": [{"type": "output_text", "text": "ok", "annotations": []}]
			}]
		}`)),
			Request: r,
		}, nil
	})}

	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: 32}, nil)
	ref := sketch.MemoryRef{Namespace: "handoff", Key: "planner-1"}
	if err := corpus.Merge(context.Background(), ref, hidden); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	client := openai.NewClient(
		option.WithBaseURL("https://openai.test/v1"),
		option.WithAPIKey("test"),
		option.WithHTTPClient(httpClient),
	)
	agent := NewOpenAIAgent("writer", client, "gpt-4o-mini", "system", "", runtime.NewMailbox(1)).
		WithCompactor(comp)

	_, err := agent.Step(context.Background(), sketch.Msg{
		From:    "planner",
		To:      "writer",
		Payload: "write final",
		Ctx: sketch.HandoffContext{
			Summary: "bounded summary",
			Refs:    []sketch.MemoryRef{ref},
		},
	})
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !strings.Contains(capturedPrompt, hidden) {
		t.Fatalf("prompt did not include referenced corpus content:\n%s", capturedPrompt)
	}
}
