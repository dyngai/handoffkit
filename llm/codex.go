package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

// CodexEndpoint is the ChatGPT-account Responses backend the Codex CLI uses.
const CodexEndpoint = "https://chatgpt.com/backend-api/codex/responses"

// DefaultCodexModel is the model the backend expects for a ChatGPT-account
// Codex session. The backend validates it; it must match what your plan allows.
const DefaultCodexModel = "gpt-5.5"

const (
	codexStreamMaxLineBytes   = 1 << 20 // 1 MiB
	codexStreamMaxFrameBytes  = 2 << 20 // 2 MiB
	codexStreamMaxOutputBytes = 4 << 20 // 4 MiB
)

// CodexClient calls the OpenAI Responses backend using the Codex CLI's
// ChatGPT-account OAuth token (from ~/.codex/auth.json) instead of an API key.
//
// This is UNSUPPORTED and reverse-engineered: the token is short-lived (run
// `codex login` to refresh), rate-limited to your ChatGPT plan, and used
// outside its intended scope. The request shape (non-empty instructions, list
// input, store=false, stream=true, SSE response with empty content-type) was
// verified empirically against the live backend.
type CodexClient struct {
	HTTP     *http.Client
	token    string // bearer credential, unexported so %v/%+v cannot leak it
	account  string // chatgpt-account-id, unexported
	Model    string
	Endpoint string
}

// String redacts credentials so a CodexClient cannot leak its token via %v/%+v.
func (c *CodexClient) String() string {
	return fmt.Sprintf("CodexClient{Model:%q Endpoint:%q token:REDACTED account:REDACTED}", c.Model, c.Endpoint)
}

// GoString redacts credentials for %#v.
func (c *CodexClient) GoString() string { return c.String() }

// tokenExpired reports whether a JWT access token's exp claim is in the past, or
// the token cannot be decoded. Best-effort local check; the backend is still
// authoritative.
func tokenExpired(tok string) bool {
	parts := strings.Split(tok, ".")
	if len(parts) < 2 {
		return true
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return true
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(raw, &claims) != nil || claims.Exp == 0 {
		return true
	}
	return time.Now().Unix() >= claims.Exp
}

// LoadCodexClient reads Codex credentials from CODEX_ACCESS_TOKEN, or from
// auth.json under CODEX_HOME (default: ~/.codex). CODEX_ACCOUNT_ID is optional
// for env-token clients and is used when present.
func LoadCodexClient() (*CodexClient, error) {
	if tok := strings.TrimSpace(os.Getenv("CODEX_ACCESS_TOKEN")); tok != "" {
		if tokenExpired(tok) {
			return nil, fmt.Errorf("codex token in CODEX_ACCESS_TOKEN is expired")
		}
		return newCodexClient(tok, strings.TrimSpace(os.Getenv("CODEX_ACCOUNT_ID"))), nil
	}
	dir, err := codexHome()
	if err != nil {
		return nil, err
	}
	return LoadCodexClientFrom(filepath.Join(dir, "auth.json"))
}

func codexHome() (string, error) {
	if dir := strings.TrimSpace(os.Getenv("CODEX_HOME")); dir != "" {
		return dir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".codex"), nil
}

// LoadCodexClientFrom reads Codex credentials from an explicit auth.json path.
func LoadCodexClientFrom(path string) (*CodexClient, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s (run `codex login`): %w", path, err)
	}
	var a struct {
		Tokens struct {
			AccessToken string `json:"access_token"`
			AccountID   string `json:"account_id"`
		} `json:"tokens"`
	}
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, err
	}
	if a.Tokens.AccessToken == "" {
		return nil, fmt.Errorf("no codex access token in %s; run `codex login`", path)
	}
	if a.Tokens.AccountID == "" {
		return nil, fmt.Errorf("no codex account id in %s; run `codex login`", path)
	}
	if tokenExpired(a.Tokens.AccessToken) {
		return nil, fmt.Errorf("codex token in %s is expired; run `codex login`", path)
	}
	return newCodexClient(a.Tokens.AccessToken, a.Tokens.AccountID), nil
}

func newCodexClient(token, account string) *CodexClient {
	return &CodexClient{
		HTTP:     &http.Client{Timeout: 180 * time.Second},
		token:    token,
		account:  account,
		Model:    DefaultCodexModel,
		Endpoint: CodexEndpoint,
	}
}

// Complete runs one streamed Responses call and assembles the output text.
func (c *CodexClient) Complete(ctx context.Context, instructions, userText string) (string, error) {
	if c == nil {
		return "", fmt.Errorf("codex client is nil")
	}
	if c.HTTP == nil {
		return "", fmt.Errorf("codex client HTTP is nil")
	}
	if strings.TrimSpace(instructions) == "" {
		return "", fmt.Errorf("codex instructions are empty")
	}
	payload := map[string]any{
		"model":        c.Model,
		"instructions": instructions, // required by the backend
		"input": []any{map[string]any{
			"type":    "message",
			"role":    "user",
			"content": []any{map[string]any{"type": "input_text", "text": userText}},
		}},
		"store":  false,
		"stream": true, // required by the backend
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if c.account != "" {
		req.Header.Set("chatgpt-account-id", c.account)
	}
	req.Header.Set("OpenAI-Beta", "responses=experimental")
	req.Header.Set("originator", "codex_cli_rs")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2000))
		hint := ""
		if resp.StatusCode == http.StatusUnauthorized {
			hint = " (token expired? run `codex login`)"
		}
		return "", fmt.Errorf("codex backend %d%s: %s", resp.StatusCode, hint, strings.TrimSpace(string(msg)))
	}
	// The backend streams SSE with an empty content-type.
	return parseResponsesStream(resp.Body)
}

// parseResponsesStream reads an OpenAI Responses SSE stream and returns the
// assembled output text. It surfaces failure/incomplete events as errors and
// requires a response.completed event: a stream that ends without completion (a
// truncated read) is an error, not a partial success. Events are accumulated by
// SSE frame (blank-line separated, data: lines joined), and unmodelled event
// types are ignored.
func parseResponsesStream(r io.Reader) (string, error) {
	br := bufio.NewReader(r)

	var out strings.Builder
	var data strings.Builder // data: lines for the current SSE event
	var lineBuf []byte
	completed := false

	handle := func() error {
		defer data.Reset()
		raw := strings.TrimSpace(data.String())
		if raw == "" || raw == "[DONE]" {
			return nil
		}
		var ev struct {
			Type     string `json:"type"`
			Delta    string `json:"delta"`
			Response struct {
				Status string `json:"status"`
			} `json:"response"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal([]byte(raw), &ev) != nil {
			return fmt.Errorf("codex stream: unparseable event frame")
		}
		switch ev.Type {
		case "response.output_text.delta":
			if out.Len()+len(ev.Delta) > codexStreamMaxOutputBytes {
				return fmt.Errorf("codex stream output exceeds %d bytes", codexStreamMaxOutputBytes)
			}
			out.WriteString(ev.Delta)
		case "response.completed":
			completed = true
		case "response.failed", "response.error", "error":
			switch {
			case ev.Error != nil && ev.Error.Message != "":
				return fmt.Errorf("codex stream failed: %s", ev.Error.Message)
			case ev.Response.Status != "":
				return fmt.Errorf("codex stream failed: status %s", ev.Response.Status)
			default:
				return fmt.Errorf("codex stream failed")
			}
		case "response.incomplete":
			return fmt.Errorf("codex stream incomplete")
		}
		return nil
	}

	for {
		part, err := br.ReadSlice('\n')
		if len(lineBuf)+len(part) > codexStreamMaxLineBytes {
			return out.String(), fmt.Errorf("codex stream line exceeds %d bytes", codexStreamMaxLineBytes)
		}
		if err == bufio.ErrBufferFull {
			lineBuf = append(lineBuf, part...)
			continue
		}

		var line string
		if len(lineBuf) > 0 {
			lineBuf = append(lineBuf, part...)
			line = string(lineBuf)
			lineBuf = lineBuf[:0]
		} else {
			line = string(part)
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" { // SSE event boundary
			if e := handle(); e != nil {
				return out.String(), e
			}
		} else if strings.HasPrefix(trimmed, "data:") {
			payload := strings.TrimSpace(trimmed[len("data:"):])
			extra := len(payload)
			if data.Len() > 0 {
				extra++
			}
			if data.Len()+extra > codexStreamMaxFrameBytes {
				return out.String(), fmt.Errorf("codex stream event frame exceeds %d bytes", codexStreamMaxFrameBytes)
			}
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(payload)
		}
		// other SSE fields (event:, id:, retry:) are ignored
		if err != nil {
			if err == io.EOF {
				break
			}
			return out.String(), err
		}
	}
	if e := handle(); e != nil { // trailing event with no final blank line
		return out.String(), e
	}
	if !completed {
		return out.String(), fmt.Errorf("codex stream ended without a completion marker (truncated?)")
	}
	return out.String(), nil
}

// CodexAgent is an sketch.Agent whose Step is one Codex-backed LLM call. It is
// a peer of OpenAIAgent: same interface, different (unsupported) transport.
type CodexAgent struct {
	addr    sketch.Address
	inbox   sketch.Mailbox
	client  *CodexClient
	system  string
	next    sketch.Address
	compact *runtime.Compactor // optional: bound + corpus-offload the handoff
	fullOut bool               // keep full output in Payload even for routed compacted messages
	seq     int                // per-step counter for unique corpus refs

	promptCorpus   sketch.Corpus // optional: resolve inbound Ctx.Refs into prompt text
	promptRefBytes int           // total resolved corpus bytes allowed in one prompt
}

// NewCodexAgent builds a Codex-backed actor. next is the handoff target
// ("" means the agent produces a terminal, un-routed message).
func NewCodexAgent(addr sketch.Address, client *CodexClient, system string, next sketch.Address, inbox sketch.Mailbox) *CodexAgent {
	return &CodexAgent{addr: addr, inbox: inbox, client: client, system: system, next: next, promptRefBytes: defaultPromptRefBytes}
}

// WithCompactor makes the agent project its output onto a bounded, corpus-backed
// handoff (via c) instead of shipping the full output as Summary. It returns the
// agent for chaining. Pass nil to keep the default full-output behavior.
func (a *CodexAgent) WithCompactor(c *runtime.Compactor) *CodexAgent {
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
func (a *CodexAgent) WithPromptRefBytes(max int) *CodexAgent {
	a.promptRefBytes = max
	return a
}

// WithFullOutputPayload keeps the complete model output in Msg.Payload even
// when the agent uses a Compactor and routes to another mailbox. The handoff
// context is still compacted; use this on a final routed agent whose mailbox
// output is the user-facing result.
func (a *CodexAgent) WithFullOutputPayload() *CodexAgent {
	a.fullOut = true
	return a
}

// Address implements sketch.Agent.
func (a *CodexAgent) Address() sketch.Address { return a.addr }

// Inbox implements sketch.Agent.
func (a *CodexAgent) Inbox() sketch.Mailbox { return a.inbox }

// Step runs one Codex-backed LLM call and hands the result forward.
func (a *CodexAgent) Step(ctx context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	text, err := buildPromptWithCorpus(ctx, in, a.promptCorpus, a.promptRefBytes)
	if err != nil {
		return nil, err
	}

	out, err := a.client.Complete(ctx, a.system, text)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(out) == "" {
		return nil, fmt.Errorf("codex agent %q produced empty output", a.addr)
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

var _ sketch.Agent = (*CodexAgent)(nil)
