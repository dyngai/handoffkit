---
name: handoffkit-scaffold
description: "Use when the user wants to scaffold the HandoffKit runtime primitives (channel-backed mailbox, select, router, broker, join/quorum barriers, trace, budget, corpus/compactor, supervisor/nursery, dead letters, plus the sketch interfaces) into a Go project. Copies the vendored reference implementation and rewrites the import path to the target module. Trigger words include scaffold handoffkit, add the HandoffKit runtime, set up message-passing agents in this repo, drop in the CSP agent primitives."
---

# Scaffold the HandoffKit runtime into a Go project

Use this to add the message-passing primitives to a **Go** project. The reference
sources are vendored under this skill's `references/_src/` directory, a snapshot
of the canonical runtime, so this works offline. (The `_src` name keeps the go
tool from compiling these reference files as part of the HandoffKit module.)

This is the **Go reference implementation**, one instantiation of the pattern.
The pattern itself is language-agnostic: for a Python, TypeScript, Rust, or
Erlang project there is nothing to copy, use the `handoffkit` skill and build the
equivalent from its primitive→language mapping table.

## Steps

1. **Determine the target module path.** Read the project's `go.mod`. If there
   is none, ask the user for a module path and run `go mod init <path>`.
   The scaffolded runtime requires Go 1.21+. The full HandoffKit repo requires
   Go 1.22+ because of its OpenAI SDK dependency.

2. **Copy the reference sources** from this skill into the project, preserving
   structure and license attribution:
   - `references/_src/sketch/handoffkit.go` → `sketch/handoffkit.go`
   - `references/_src/runtime/*.go` → `runtime/`
   - The copied files are MIT-licensed HandoffKit source; preserve the original
     license notice by keeping the repo's `LICENSE` text with the copied
     scaffold or adding an equivalent attribution/license notice where the
     target project tracks third-party code.

3. **Rewrite the import path.** In every copied file, replace the placeholder
   module path `github.com/dyngai/handoffkit` with the target module
   path. (The interfaces live at `<module>/sketch`; the runtime imports them.)

4. **Verify.** Run `go build ./... && go vet ./...` and fix anything that
   fails.

5. **Point the user at the API** so they can wire their first topology:
   - `runtime.NewMailbox(buffer)`, unbuffered = rendezvous; buffered = queue.
   - `runtime.NewSelector()`, wait on inbox / timeout / cancellation.
   - `runtime.NewRouter()` + `runtime.Run` / `runtime.RunTraced`, single-owner agent loop with point-to-point routing (and optional message tracing).
   - `runtime.NewBroker()`, broadcast one message to every subscriber.
   - `runtime.NewJoinAgent(addr, inbox, next, need, combine)`, fan-in barrier: emits after `need` messages arrive.
   - `runtime.NewQuorumAgent(addr, inbox, next, need, total, combine)`, fan-in quorum: emits after the first `need` of `total` messages arrive.
   - `runtime.NewBudget(total)`, a selectable resource ceiling whose `Done()` closes on exhaustion.
   - `runtime.NewCorpus(merge)` + `runtime.NewCompactor(...)`, shared referenced knowledge plus bounded handoff summaries.
   - `runtime.NewNursery(ctx, maxDepth)`, a structured-concurrency supervisor with depth/lineage guards and subtree cancellation.
   - `runtime.WithDeadLetters(dispatcher, sink)`, captures undeliverable messages instead of making routing failure fatal.
   - Implement `sketch.Agent` (Address / Inbox / Step) to add an LLM-backed actor; `Step` returns the messages to route next.

For the design pattern, the coordination shapes (pipeline / pool / broadcast /
join), and the honest tradeoffs, use the `handoffkit` skill.
