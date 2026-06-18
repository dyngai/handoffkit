// Command codex-workers is a parallel coding-agent worker pool: a tasklist of
// independent coding tasks is fed into one shared queue mailbox, N workers each
// pick up a task, run `codex exec` in a separate git worktree, and gate the
// result on `go test` (a compiler/test signal, not an LLM judge). Results fan back
// into a collector.
//
// It reuses the handoffkit runtime unchanged, a worker is just another
// sketch.Agent whose Step shells out to `codex exec` instead of calling an
// HTTP API. The shared queue mailbox gives load-balanced, exactly-once dispatch
// for free (Go delivers each task to exactly one worker).
//
// Security note: this is a local demo, not a portable sandbox. The Codex CLI is
// asked to use workspace-write mode, and generated code is tested with a small
// environment, scratch-local Go caches, no module downloads, bounded output
// capture, and process-group cleanup. Worktrees separate file edits; they do
// not isolate credentials or host execution. `go test` still executes generated
// code on the host, so do not run this example on prompts or repositories you
// would not otherwise trust.
//
// LOCAL, UNSUPPORTED demo behavior: it drives the Codex CLI agent on your
// ChatGPT plan (run `codex login` if the token is stale). Keep N small, a
// `prolite` plan throttles parallel sessions. Requires `codex` and `go` on PATH.
//
//	PATH=/opt/homebrew/bin:$PATH go run ./examples/codex-workers
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

const (
	workerCount     = 2 // keep small: a prolite plan throttles parallel codex sessions
	sandboxMode     = "workspace-write"
	outputLimitByte = 256 * 1024
)

// Task is one unit of coding work. Encoded as JSON in a Msg payload.
type Task struct {
	ID     string `json:"id"`
	Prompt string `json:"prompt"`
}

// Result is what a worker reports back. Encoded as JSON in a Msg payload.
type Result struct {
	ID         string `json:"id"`
	Worker     string `json:"worker"`
	CodexOK    bool   `json:"codex_ok"`
	TestPassed bool   `json:"test_passed"`
	Worktree   string `json:"worktree"`
	Note       string `json:"note"`
}

// codexWorker is an sketch.Agent whose Step runs one coding task via
// `codex exec` in a separate git worktree, then gates on `go test`.
type codexWorker struct {
	addr      sketch.Address
	inbox     sketch.Mailbox // SHARED across all workers, this is the queue
	next      sketch.Address
	baseRepo  string
	wtRoot    string
	codexHome string
	wtMu      *sync.Mutex // serialize `git worktree add` (it locks the base repo)
}

func (w *codexWorker) Address() sketch.Address { return w.addr }
func (w *codexWorker) Inbox() sketch.Mailbox   { return w.inbox }

func (w *codexWorker) Step(ctx context.Context, in sketch.Msg) ([]sketch.Msg, error) {
	var task Task
	if err := json.Unmarshal([]byte(in.Payload), &task); err != nil {
		return nil, err
	}
	res := Result{ID: task.ID, Worker: string(w.addr)}
	wt := filepath.Join(w.wtRoot, "task-"+task.ID)
	res.Worktree = wt

	// Separate worktree per task so parallel edits never clash. This isolates
	// file edits from sibling workers, but it is not a process sandbox.
	// `git worktree add` locks the base repo, so serialize just that step.
	w.wtMu.Lock()
	addOut, addErr := combinedOutputTree(ctx, "", baseChildEnv(), "git", "-C", w.baseRepo, "worktree", "add", "-B", "task-"+task.ID, wt, "HEAD")
	w.wtMu.Unlock()
	if addErr != nil {
		res.Note = "worktree add failed: " + lastLine(string(addOut))
		return w.emit(res), nil
	}

	// Run the real Codex coding agent, unattended, with the worktree as its
	// workspace. The scratch CODEX_HOME lets Codex authenticate without
	// forwarding HOME or XDG paths, but it is not credential isolation: the
	// Codex process receives credentials, and generated tool commands may be
	// able to observe that location depending on Codex's shell env policy.
	if _, err := combinedOutputTree(ctx, "", codexChildEnv(w.codexHome), "codex", "exec", "-C", wt, "-s", sandboxMode, "--ignore-user-config", task.Prompt); err != nil {
		res.Note = "codex exec: " + firstLine(err.Error())
	} else {
		res.CodexOK = true
	}

	// Compiler/test gate, not the model's self-report. This constrained env
	// avoids passing parent secrets/config to generated tests and disables module
	// downloads/toolchain auto-installs, but it is still host execution.
	testEnv, err := goTestEnv(filepath.Dir(w.wtRoot))
	if err != nil {
		res.Note = "go test env setup failed: " + err.Error()
		return w.emit(res), nil
	}
	ttOut, ttErr := combinedOutputTree(ctx, wt, testEnv, "go", "test", "-count=1", "-timeout=30s", "-mod=readonly", "./...")
	res.TestPassed = ttErr == nil
	if !res.TestPassed && res.Note == "" {
		res.Note = "go test failed: " + lastLine(string(ttOut))
	}
	return w.emit(res), nil
}

func (w *codexWorker) emit(res Result) []sketch.Msg {
	b, _ := json.Marshal(res)
	return []sketch.Msg{{From: w.addr, To: w.next, Payload: string(b)}}
}

func main() {
	for _, bin := range []string{"codex", "go", "git"} {
		if _, err := exec.LookPath(bin); err != nil {
			fmt.Fprintf(os.Stderr, "%q not on PATH (try PATH=/opt/homebrew/bin:$PATH): %v\n", bin, err)
			os.Exit(1)
		}
	}

	tasks := []Task{
		{ID: "add", Prompt: "Create add.go (package main) with func Add(a, b int) int returning a+b, plus add_test.go with a passing TestAdd. Minimal; nothing else."},
		{ID: "sub", Prompt: "Create sub.go (package main) with func Sub(a, b int) int returning a-b, plus sub_test.go with a passing TestSub. Minimal; nothing else."},
		{ID: "mul", Prompt: "Create mul.go (package main) with func Mul(a, b int) int returning a*b, plus mul_test.go with a passing TestMul. Minimal; nothing else."},
	}

	baseRepo, wtRoot, codexHome, err := setupScratchRepo()
	if err != nil {
		fmt.Fprintln(os.Stderr, "scratch repo setup:", err)
		os.Exit(1)
	}
	fmt.Printf("scratch base repo: %s\nworktrees under:   %s\ncodex home:        %s\n\n", baseRepo, wtRoot, codexHome)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// One shared queue mailbox (the tasklist) + a results mailbox (fan-in).
	queue := runtime.NewMailbox(len(tasks))
	results := runtime.NewMailbox(len(tasks))
	r := runtime.NewRouter()
	r.Register("results", results)

	// N workers, all sharing the SAME queue mailbox as their inbox. Go's channel
	// fan-out delivers each task to exactly one worker, no locks, no dispatcher
	// bookkeeping.
	var wtMu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, workerCount)
	for i := 1; i <= workerCount; i++ {
		w := &codexWorker{
			addr:  sketch.Address(fmt.Sprintf("worker-%d", i)),
			inbox: queue, next: "results",
			baseRepo: baseRepo, wtRoot: wtRoot, codexHome: codexHome, wtMu: &wtMu,
		}
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			if err := runtime.Run(ctx, a, r, 20*time.Second); err != nil {
				errCh <- fmt.Errorf("%s stopped: %w", a.Address(), err)
			}
		}(w)
	}

	// Load the tasklist into the queue.
	for _, t := range tasks {
		b, _ := json.Marshal(t)
		if err := queue.Send(ctx, sketch.Msg{From: "dispatcher", To: "queue", Payload: string(b)}); err != nil {
			fmt.Fprintln(os.Stderr, "enqueue:", err)
			os.Exit(1)
		}
	}

	// Collect exactly len(tasks) results, each via result OR worker error OR timeout.
	collected := make([]Result, 0, len(tasks))
	collectionFailed := false
	var runErrs []error
	for len(collected) < len(tasks) && !collectionFailed {
		timer := time.NewTimer(5 * time.Minute)
		select {
		case m, ok := <-results.C():
			timer.Stop()
			if !ok {
				fmt.Fprintln(os.Stderr, "collect: results mailbox closed")
				collectionFailed = true
				break
			}
			var got Result
			if err := json.Unmarshal([]byte(m.Payload), &got); err != nil {
				fmt.Fprintln(os.Stderr, "collect: decode result:", err)
				collectionFailed = true
				break
			}
			collected = append(collected, got)
			fmt.Printf("  done: task=%-4s worker=%-9s codex=%v test=%v %s\n",
				got.ID, got.Worker, got.CodexOK, got.TestPassed, got.Note)
		case err := <-errCh:
			timer.Stop()
			fmt.Fprintln(os.Stderr, "agent:", err)
			runErrs = append(runErrs, err)
			collectionFailed = true
		case <-timer.C:
			fmt.Fprintln(os.Stderr, "collect: timed out waiting for results")
			collectionFailed = true
		case <-ctx.Done():
			timer.Stop()
			fmt.Fprintln(os.Stderr, "collect:", ctx.Err())
			collectionFailed = true
		}
	}
	cancel()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			fmt.Fprintln(os.Stderr, "agent:", err)
			runErrs = append(runErrs, err)
		}
	}

	if len(collected) != len(tasks) {
		fmt.Fprintf(os.Stderr, "collect: got %d/%d results\n", len(collected), len(tasks))
		collectionFailed = true
	}

	// Summary + the exactly-once check: every task handled once, by some worker.
	pass := 0
	taskIDs := map[string]bool{}
	seen := map[string]int{}
	for _, t := range tasks {
		taskIDs[t.ID] = true
	}
	for _, c := range collected {
		seen[c.ID]++
		if !taskIDs[c.ID] {
			fmt.Printf("  ERROR: unexpected task %q processed\n", c.ID)
			collectionFailed = true
		}
		if c.TestPassed {
			pass++
		}
	}
	exactlyOnce := true
	for _, t := range tasks {
		if seen[t.ID] != 1 {
			fmt.Printf("  ERROR: task %q processed %d times (expected exactly once)\n", t.ID, seen[t.ID])
			exactlyOnce = false
		}
	}
	fmt.Printf("\n=== %d/%d tasks passed go test (%d workers) ===\n", pass, len(tasks), workerCount)
	fmt.Printf("inspect/clean worktrees: git -C %s worktree list\n", baseRepo)
	fmt.Printf("remove scratch repo: rm -rf %s\n", filepath.Dir(baseRepo))

	if collectionFailed || len(runErrs) > 0 || !exactlyOnce || pass != len(tasks) {
		if pass != len(tasks) {
			fmt.Fprintf(os.Stderr, "failed: only %d/%d tasks passed go test\n", pass, len(tasks))
		}
		if !exactlyOnce {
			fmt.Fprintln(os.Stderr, "failed: task collection was not exactly once")
		}
		if len(runErrs) > 0 {
			fmt.Fprintf(os.Stderr, "failed: %d worker loop error(s)\n", len(runErrs))
		}
		if collectionFailed {
			fmt.Fprintln(os.Stderr, "failed: result collection failed")
		}
		os.Exit(1)
	}
}

// setupScratchRepo creates a throwaway git repo (with go.mod + an initial
// commit so worktrees have a HEAD) and a clean directory to hold worktrees.
func setupScratchRepo() (baseRepo, wtRoot, codexHome string, err error) {
	root, err := os.MkdirTemp("", "codex-workers-*")
	if err != nil {
		return "", "", "", err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(root)
		}
	}()
	base := filepath.Join(root, "base")
	wt := filepath.Join(root, "worktrees")
	if err = os.MkdirAll(base, 0o755); err != nil {
		return "", "", "", err
	}
	if err = os.MkdirAll(wt, 0o755); err != nil {
		return "", "", "", err
	}
	codex, err := prepareCodexHome(root)
	if err != nil {
		return "", "", "", err
	}
	steps := [][]string{
		{"git", "-C", base, "init", "-q"},
		{"git", "-C", base, "config", "user.email", "codex-workers@example.com"},
		{"git", "-C", base, "config", "user.name", "codex-workers"},
		{"go", "mod", "init", "example.com/scratch"},
		{"git", "-C", base, "add", "-A"},
		{"git", "-C", base, "commit", "-qm", "init"},
	}
	for _, s := range steps {
		cmd := exec.Command(s[0], s[1:]...)
		if s[0] == "go" {
			cmd.Dir = base
		}
		cmd.Env = baseChildEnv()
		if out, e := cmd.CombinedOutput(); e != nil {
			return "", "", "", fmt.Errorf("%v: %v: %s", s, e, strings.TrimSpace(string(out)))
		}
	}
	return base, wt, codex, nil
}

func combinedOutputTree(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	out := newBoundedOutput(outputLimitByte)
	pr, pw, err := os.Pipe()
	if err != nil {
		return out.Bytes(), err
	}
	defer pr.Close()
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		_ = pw.Close()
		return out.Bytes(), err
	}
	_ = pw.Close()

	copyDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(&out, pr)
		close(copyDone)
	}()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	var waitErr error
	select {
	case err := <-done:
		waitErr = err
	case <-ctx.Done():
		killProcessTree(cmd)
		waitErr = <-done
		if waitErr == nil {
			waitErr = ctx.Err()
		}
	}
	killProcessGroup(cmd)
	select {
	case <-copyDone:
	case <-time.After(2 * time.Second):
		_ = pr.Close()
		<-copyDone
	}
	return out.Bytes(), waitErr
}

func killProcessTree(cmd *exec.Cmd) {
	killProcessGroup(cmd)
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

type boundedOutput struct {
	mu      sync.Mutex
	buf     []byte
	limit   int
	dropped int64
}

func newBoundedOutput(limit int) boundedOutput {
	return boundedOutput{limit: limit}
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.limit <= 0 {
		b.dropped += int64(len(p))
		return len(p), nil
	}
	if len(p) >= b.limit {
		b.dropped += int64(len(b.buf) + len(p) - b.limit)
		b.buf = append(b.buf[:0], p[len(p)-b.limit:]...)
		return len(p), nil
	}
	if overflow := len(b.buf) + len(p) - b.limit; overflow > 0 {
		b.dropped += int64(overflow)
		copy(b.buf, b.buf[overflow:])
		b.buf = b.buf[:len(b.buf)-overflow]
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *boundedOutput) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]byte, 0, len(b.buf)+96)
	if b.dropped > 0 {
		out = fmt.Appendf(out, "[output truncated: dropped %d earlier bytes, kept last %d bytes]\n", b.dropped, len(b.buf))
	}
	out = append(out, b.buf...)
	return out
}

func baseChildEnv() []string {
	keys := []string{
		"PATH",
		"TMPDIR",
		"TEMP",
		"TMP",
		"USER",
		"LOGNAME",
		"SHELL",
		"TERM",
	}
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok && value != "" {
			env = append(env, key+"="+value)
		}
	}
	return env
}

func codexChildEnv(codexHome string) []string {
	env := baseChildEnv()
	env = append(env, "CODEX_HOME="+codexHome)
	return env
}

func prepareCodexHome(scratchRoot string) (string, error) {
	authFile, err := sourceCodexAuthFile()
	if err != nil {
		return "", err
	}
	auth, err := os.ReadFile(authFile)
	if err != nil {
		return "", err
	}

	// Codex auth is directory-scoped: auth.json lives under CODEX_HOME. Copy
	// only that credential file into scratch CODEX_HOME instead of forwarding
	// HOME or XDG paths from the parent shell.
	codexHome := filepath.Join(scratchRoot, "codex-home")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(codexHome, "auth.json"), auth, 0o600); err != nil {
		return "", err
	}
	return codexHome, nil
}

func sourceCodexAuthFile() (string, error) {
	if codexHome, ok := os.LookupEnv("CODEX_HOME"); ok && codexHome != "" {
		return requireCodexAuthFile(filepath.Join(codexHome, "auth.json"))
	}
	if home, ok := os.LookupEnv("HOME"); ok && home != "" {
		return requireCodexAuthFile(filepath.Join(home, ".codex", "auth.json"))
	}
	return "", fmt.Errorf("codex auth not found: set CODEX_HOME or run `codex login`")
}

func requireCodexAuthFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("codex auth not found at %s: run `codex login`", path)
	}
	if info.IsDir() {
		return "", fmt.Errorf("codex auth path is a directory: %s", path)
	}
	return path, nil
}

func goTestEnv(scratchRoot string) ([]string, error) {
	home := filepath.Join(scratchRoot, "go-home")
	tmp := filepath.Join(scratchRoot, "go-tmp")
	gopath := filepath.Join(scratchRoot, "gopath")
	gocache := filepath.Join(scratchRoot, "go-build-cache")
	gomodcache := filepath.Join(scratchRoot, "go-mod-cache")
	for _, dir := range []string{home, tmp, gopath, gocache, gomodcache} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	env := []string{
		"HOME=" + home,
		"TMPDIR=" + tmp,
		"TEMP=" + tmp,
		"TMP=" + tmp,
		"GOPATH=" + gopath,
		"GOCACHE=" + gocache,
		"GOMODCACHE=" + gomodcache,
		"GOENV=off",
		"GOPROXY=off",
		"GOSUMDB=off",
		"GOTOOLCHAIN=local",
		"CGO_ENABLED=0",
	}
	if path, ok := os.LookupEnv("PATH"); ok && path != "" {
		env = append(env, "PATH="+path)
	}
	return env, nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func lastLine(s string) string {
	s = strings.TrimRight(s, "\n")
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		return s[i+1:]
	}
	return s
}
