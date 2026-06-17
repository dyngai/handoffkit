// Command codex-workers is a parallel coding-agent worker pool: a tasklist of
// independent coding tasks is fed into one shared queue mailbox, N workers each
// pick up a task, run `codex exec` in an ISOLATED git worktree, and gate the
// result on `go test` (an objective oracle, not an LLM judge). Results fan back
// into a collector.
//
// It reuses the handoffkit runtime unchanged, a worker is just another
// sketch.Agent whose Step shells out to `codex exec` instead of calling an
// HTTP API. The shared queue mailbox gives load-balanced, exactly-once dispatch
// for free (Go delivers each task to exactly one worker).
//
// LOCAL, UNSUPPORTED, and untracked: it drives the Codex CLI agent on your
// ChatGPT plan (run `codex login` if the token is stale). Keep N small, a
// `prolite` plan throttles parallel sessions. Requires `codex` and `go` on PATH.
//
//	PATH=/opt/homebrew/bin:$PATH go run ./examples/codex-workers
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dyngai/handoffkit/runtime"
	"github.com/dyngai/handoffkit/sketch"
)

const (
	workerCount = 2 // keep small: a prolite plan throttles parallel codex sessions
	sandboxMode = "workspace-write"
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
// `codex exec` in an isolated git worktree, then gates on `go test`.
type codexWorker struct {
	addr     sketch.Address
	inbox    sketch.Mailbox // SHARED across all workers, this is the queue
	next     sketch.Address
	baseRepo string
	wtRoot   string
	wtMu     *sync.Mutex // serialize `git worktree add` (it locks the base repo)
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

	// Isolated worktree per task so parallel edits never clash. `git worktree
	// add` locks the base repo, so serialize just that step.
	w.wtMu.Lock()
	add := exec.CommandContext(ctx, "git", "-C", w.baseRepo, "worktree", "add", "-B", "task-"+task.ID, wt, "HEAD")
	addOut, addErr := add.CombinedOutput()
	w.wtMu.Unlock()
	if addErr != nil {
		res.Note = "worktree add failed: " + lastLine(string(addOut))
		return w.emit(res), nil
	}

	// Run the real Codex coding agent, unattended, scoped to the worktree.
	cx := exec.CommandContext(ctx, "codex", "exec", "-C", wt, "-s", sandboxMode, task.Prompt)
	if _, err := cx.CombinedOutput(); err != nil {
		res.Note = "codex exec: " + firstLine(err.Error())
	} else {
		res.CodexOK = true
	}

	// Objective gate, the compiler/tests, not the model's self-report.
	tt := exec.CommandContext(ctx, "go", "test", "./...")
	tt.Dir = wt
	ttOut, ttErr := tt.CombinedOutput()
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

	baseRepo, wtRoot, err := setupScratchRepo()
	if err != nil {
		fmt.Fprintln(os.Stderr, "scratch repo setup:", err)
		os.Exit(1)
	}
	fmt.Printf("scratch base repo: %s\nworktrees under:   %s\n\n", baseRepo, wtRoot)

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
	for i := 1; i <= workerCount; i++ {
		w := &codexWorker{
			addr:  sketch.Address(fmt.Sprintf("worker-%d", i)),
			inbox: queue, next: "results",
			baseRepo: baseRepo, wtRoot: wtRoot, wtMu: &wtMu,
		}
		wg.Add(1)
		go func(a sketch.Agent) {
			defer wg.Done()
			if err := runtime.Run(ctx, a, r, 20*time.Second); err != nil {
				fmt.Fprintf(os.Stderr, "%s stopped: %v\n", a.Address(), err)
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

	// Collect exactly len(tasks) results, each via a Select (result OR timeout).
	sel := runtime.NewSelector()
	collected := make([]Result, 0, len(tasks))
	for len(collected) < len(tasks) {
		var got Result
		var ok bool
		_, serr := sel.Run(ctx, sketch.Select{Cases: []sketch.Case{
			{Mailbox: results, OnRecv: func(m sketch.Msg) error {
				ok = json.Unmarshal([]byte(m.Payload), &got) == nil
				return nil
			}},
			{After: 5 * time.Minute, OnAfter: func() error { return fmt.Errorf("timed out waiting for results") }},
		}})
		if serr != nil {
			fmt.Fprintln(os.Stderr, "collect:", serr)
			break
		}
		if ok {
			collected = append(collected, got)
			fmt.Printf("  done: task=%-4s worker=%-9s codex=%v test=%v %s\n",
				got.ID, got.Worker, got.CodexOK, got.TestPassed, got.Note)
		}
	}
	cancel()
	wg.Wait()

	// Summary + the exactly-once check: every task handled once, by some worker.
	pass := 0
	seen := map[string]int{}
	for _, c := range collected {
		seen[c.ID]++
		if c.TestPassed {
			pass++
		}
	}
	fmt.Printf("\n=== %d/%d tasks passed go test (%d workers) ===\n", pass, len(tasks), workerCount)
	for _, t := range tasks {
		if seen[t.ID] != 1 {
			fmt.Printf("  WARNING: task %q processed %d times (expected exactly once)\n", t.ID, seen[t.ID])
		}
	}
	fmt.Printf("inspect/clean worktrees: git -C %s worktree list\n", baseRepo)
}

// setupScratchRepo creates a throwaway git repo (with go.mod + an initial
// commit so worktrees have a HEAD) and a clean directory to hold worktrees.
func setupScratchRepo() (baseRepo, wtRoot string, err error) {
	base := filepath.Join(os.TempDir(), "codex-workers-base")
	wt := filepath.Join(os.TempDir(), "codex-workers-wt")
	for _, d := range []string{base, wt} {
		if err = os.RemoveAll(d); err != nil {
			return "", "", err
		}
	}
	if err = os.MkdirAll(base, 0o755); err != nil {
		return "", "", err
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
		if out, e := cmd.CombinedOutput(); e != nil {
			return "", "", fmt.Errorf("%v: %v: %s", s, e, strings.TrimSpace(string(out)))
		}
	}
	return base, wt, nil
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
