package llm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// sseFrames builds an SSE body: one blank-line-separated "data: <event>" frame
// per argument.
func sseFrames(events ...string) string {
	var b strings.Builder
	for _, e := range events {
		b.WriteString("data: ")
		b.WriteString(e)
		b.WriteString("\n\n")
	}
	return b.String()
}

// Deltas are assembled and a response.completed marker yields success.
func TestParseResponsesStream_AssemblesUntilCompleted(t *testing.T) {
	body := sseFrames(
		`{"type":"response.created"}`,
		`{"type":"response.output_text.delta","delta":"Hello "}`,
		`{"type":"response.output_text.delta","delta":"world"}`,
		`{"type":"response.completed","response":{"status":"completed"}}`,
		`[DONE]`,
	)
	out, err := parseResponsesStream(strings.NewReader(body))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "Hello world" {
		t.Fatalf("out = %q, want %q", out, "Hello world")
	}
}

// A response.failed event is an error, not a partial success.
func TestParseResponsesStream_FailedEventIsError(t *testing.T) {
	body := sseFrames(
		`{"type":"response.output_text.delta","delta":"partial"}`,
		`{"type":"response.failed","error":{"message":"boom"}}`,
	)
	if _, err := parseResponsesStream(strings.NewReader(body)); err == nil {
		t.Fatal("expected an error for a response.failed event, got nil")
	}
}

// A stream that ends without a completion marker (truncated) is an error.
func TestParseResponsesStream_TruncatedIsError(t *testing.T) {
	body := sseFrames(
		`{"type":"response.output_text.delta","delta":"half"}`,
	)
	if _, err := parseResponsesStream(strings.NewReader(body)); err == nil {
		t.Fatal("expected an error for a stream without a completion marker, got nil")
	}
}

// An unparseable (non-[DONE]) frame is a protocol error, even if a completion
// marker follows.
func TestParseResponsesStream_MalformedFrameIsError(t *testing.T) {
	body := sseFrames(
		`{"type":"response.output_text.delta","delta":"ok"}`,
		`{corrupt json`,
		`{"type":"response.completed","response":{"status":"completed"}}`,
	)
	if _, err := parseResponsesStream(strings.NewReader(body)); err == nil {
		t.Fatal("expected an error for a malformed SSE frame, got nil")
	}
}

func TestParseResponsesStream_OutputLimit(t *testing.T) {
	const chunkSize = 64 << 10
	chunk := strings.Repeat("x", chunkSize)

	var within strings.Builder
	for i := 0; i < codexStreamMaxOutputBytes/chunkSize; i++ {
		within.WriteString(sseFrames(fmt.Sprintf(`{"type":"response.output_text.delta","delta":%q}`, chunk)))
	}
	within.WriteString(sseFrames(`{"type":"response.completed","response":{"status":"completed"}}`))

	out, err := parseResponsesStream(strings.NewReader(within.String()))
	if err != nil {
		t.Fatalf("parse at output limit: %v", err)
	}
	if len(out) != codexStreamMaxOutputBytes {
		t.Fatalf("output len = %d, want %d", len(out), codexStreamMaxOutputBytes)
	}

	var over strings.Builder
	for i := 0; i < codexStreamMaxOutputBytes/chunkSize; i++ {
		over.WriteString(sseFrames(fmt.Sprintf(`{"type":"response.output_text.delta","delta":%q}`, chunk)))
	}
	over.WriteString(sseFrames(
		`{"type":"response.output_text.delta","delta":"x"}`,
		`{"type":"response.completed","response":{"status":"completed"}}`,
	))
	_, err = parseResponsesStream(strings.NewReader(over.String()))
	if err == nil || !strings.Contains(err.Error(), "output exceeds") {
		t.Fatalf("parse over output limit err = %v, want output limit error", err)
	}
}

func TestParseResponsesStream_OversizedLineIsError(t *testing.T) {
	body := "data: " + strings.Repeat("x", codexStreamMaxLineBytes)

	_, err := parseResponsesStream(strings.NewReader(body))
	if err == nil || !strings.Contains(err.Error(), "line exceeds") {
		t.Fatalf("parse oversized line err = %v, want line limit error", err)
	}
}

func TestParseResponsesStream_OversizedFrameIsError(t *testing.T) {
	payload := strings.Repeat("x", 1024)
	var body strings.Builder
	for body.Len() <= codexStreamMaxFrameBytes+codexStreamMaxLineBytes {
		body.WriteString("data: ")
		body.WriteString(payload)
		body.WriteByte('\n')
	}
	body.WriteByte('\n')

	_, err := parseResponsesStream(strings.NewReader(body.String()))
	if err == nil || !strings.Contains(err.Error(), "event frame exceeds") {
		t.Fatalf("parse oversized frame err = %v, want frame limit error", err)
	}
}

func jwtWithExp(exp int64) string {
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, exp)))
	return "header." + payload + ".sig"
}

// tokenExpired decodes the JWT exp claim: future = valid, past = expired,
// undecodable = expired.
func TestTokenExpired(t *testing.T) {
	if tokenExpired(jwtWithExp(time.Now().Add(time.Hour).Unix())) {
		t.Fatal("a future-exp token was reported expired")
	}
	if !tokenExpired(jwtWithExp(time.Now().Add(-time.Hour).Unix())) {
		t.Fatal("a past-exp token was reported not expired")
	}
	if !tokenExpired("not-a-jwt") {
		t.Fatal("a malformed token was reported not expired")
	}
}

func writeCodexAuth(t *testing.T, dir, token, account string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir auth dir: %v", err)
	}
	body := fmt.Sprintf(`{"tokens":{"access_token":%q,"account_id":%q}}`, token, account)
	if err := os.WriteFile(filepath.Join(dir, "auth.json"), []byte(body), 0o600); err != nil {
		t.Fatalf("write auth.json: %v", err)
	}
}

func TestLoadCodexClientUsesCodexHome(t *testing.T) {
	token := jwtWithExp(time.Now().Add(time.Hour).Unix())
	dir := t.TempDir()
	writeCodexAuth(t, dir, token, "acct-home")
	t.Setenv("CODEX_HOME", dir)
	t.Setenv("CODEX_ACCESS_TOKEN", "")
	t.Setenv("CODEX_ACCOUNT_ID", "")

	c, err := LoadCodexClient()
	if err != nil {
		t.Fatalf("LoadCodexClient: %v", err)
	}
	if c.token != token || c.account != "acct-home" {
		t.Fatalf("loaded token/account = %q/%q, want env home auth.json", c.token, c.account)
	}
}

func TestLoadCodexClientUsesAccessTokenEnv(t *testing.T) {
	token := jwtWithExp(time.Now().Add(time.Hour).Unix())
	t.Setenv("CODEX_HOME", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("CODEX_ACCESS_TOKEN", token)
	t.Setenv("CODEX_ACCOUNT_ID", "acct-env")

	c, err := LoadCodexClient()
	if err != nil {
		t.Fatalf("LoadCodexClient: %v", err)
	}
	if c.token != token || c.account != "acct-env" {
		t.Fatalf("loaded token/account = %q/%q, want CODEX_ACCESS_TOKEN/CODEX_ACCOUNT_ID", c.token, c.account)
	}
}

func TestLoadCodexClientAccessTokenEnvMayOmitAccount(t *testing.T) {
	token := jwtWithExp(time.Now().Add(time.Hour).Unix())
	t.Setenv("CODEX_ACCESS_TOKEN", token)
	t.Setenv("CODEX_ACCOUNT_ID", "")

	c, err := LoadCodexClient()
	if err != nil {
		t.Fatalf("LoadCodexClient: %v", err)
	}
	if c.token != token || c.account != "" {
		t.Fatalf("loaded token/account = %q/%q, want token with no account", c.token, c.account)
	}
}

func TestLoadCodexClientAccessTokenEnvExpired(t *testing.T) {
	t.Setenv("CODEX_ACCESS_TOKEN", jwtWithExp(time.Now().Add(-time.Hour).Unix()))

	_, err := LoadCodexClient()
	if err == nil || !strings.Contains(err.Error(), "CODEX_ACCESS_TOKEN") || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("LoadCodexClient err = %v, want useful expired env-token error", err)
	}
}

func TestCodexCompleteNilClientReturnsError(t *testing.T) {
	var c *CodexClient
	_, err := c.Complete(context.Background(), "system", "user")
	if err == nil || !strings.Contains(err.Error(), "codex client is nil") {
		t.Fatalf("Complete err = %v, want nil-client error", err)
	}
}

func TestCodexCompleteNilHTTPReturnsError(t *testing.T) {
	c := &CodexClient{Model: DefaultCodexModel, Endpoint: CodexEndpoint}
	_, err := c.Complete(context.Background(), "system", "user")
	if err == nil || !strings.Contains(err.Error(), "HTTP is nil") {
		t.Fatalf("Complete err = %v, want nil-HTTP error", err)
	}
}

func TestCodexCompleteEmptyInstructionsReturnsLocalError(t *testing.T) {
	called := false
	c := &CodexClient{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			called = true
			t.Fatal("transport should not be called for empty instructions")
			return nil, nil
		})},
		Model:    DefaultCodexModel,
		Endpoint: "https://codex.test/responses",
	}

	_, err := c.Complete(context.Background(), " \n\t ", "user")
	if err == nil || !strings.Contains(err.Error(), "instructions are empty") {
		t.Fatalf("Complete err = %v, want empty-instructions error", err)
	}
	if called {
		t.Fatal("transport was called despite empty instructions")
	}
}

func TestCodexAgentWithFullOutputPayloadKeepsCompactedRoutedResult(t *testing.T) {
	full := strings.Repeat("final answer ", 300)

	const budget = 80
	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: budget}, nil)
	client := &CodexClient{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(sseFrames(
					fmt.Sprintf(`{"type":"response.output_text.delta","delta":%q}`, full),
					`{"type":"response.completed","response":{"status":"completed"}}`,
				))),
				Header:  make(http.Header),
				Request: r,
			}, nil
		})},
		Model:    DefaultCodexModel,
		Endpoint: "https://codex.test/responses",
	}
	agent := NewCodexAgent("writer", client, "system", "out", runtime.NewMailbox(1)).
		WithCompactor(comp).
		WithFullOutputPayload()

	out, err := agent.Step(context.Background(), sketch.Msg{From: "planner", To: "writer", Payload: "write final"})
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("Step emitted %d messages, want 1", len(out))
	}
	if out[0].Payload != full {
		t.Fatalf("Payload len = %d, want full len %d", len(out[0].Payload), len(full))
	}
	if len(out[0].Ctx.Summary) > budget {
		t.Fatalf("Summary is %d bytes, want <= %d", len(out[0].Ctx.Summary), budget)
	}
}

func TestCodexAgentWithCompactorIncludesCorpusRefsInPrompt(t *testing.T) {
	const hidden = "hidden corpus detail: CODEX_TOKEN"
	var capturedPrompt string
	client := &CodexClient{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			var body struct {
				Input []struct {
					Content []struct {
						Text string `json:"text"`
					} `json:"content"`
				} `json:"input"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			if len(body.Input) != 1 || len(body.Input[0].Content) != 1 {
				t.Fatalf("unexpected request input shape: %+v", body.Input)
			}
			capturedPrompt = body.Input[0].Content[0].Text
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(sseFrames(
					`{"type":"response.output_text.delta","delta":"ok"}`,
					`{"type":"response.completed","response":{"status":"completed"}}`,
				))),
				Header:  make(http.Header),
				Request: r,
			}, nil
		})},
		Model:    DefaultCodexModel,
		Endpoint: "https://codex.test/responses",
	}

	corpus := runtime.NewCorpus(nil)
	comp := runtime.NewCompactor(corpus, runtime.CompactPolicy{MaxSummaryBytes: 32}, nil)
	ref := sketch.MemoryRef{Namespace: "handoff", Key: "planner-1"}
	if err := corpus.Merge(context.Background(), ref, hidden); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	agent := NewCodexAgent("writer", client, "system", "", runtime.NewMailbox(1)).
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
