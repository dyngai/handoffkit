# Primitives

The mapping from CSP/Go to agents, and the reference API in [`../sketch/handoffkit.go`](../sketch/handoffkit.go).

| Go / CSP | This model | Notes |
|---|---|---|
| goroutine | `Agent` (actor) | private state = its context window; single-owner run loop |
| channel | `Mailbox` | unbuffered = rendezvous; buffered = decoupling queue |
| unbuffered send | synchronous handoff | producer blocks until consumer ready → backpressure |
| `select` | `Select` | wait on peer / user / budget / timeout / cancel, atomically |
| ownership transfer on send | `Handoff` | task + needed context travel together; sender lets go |
| `context.Done()` | cancellation propagation | unwinds a spawned subtree |
| happens-before | message ordering | survives; behavioral guarantees do not (non-determinism) |

## Runtime: what is built

The sketch is interface-only, but [`../runtime/`](../runtime) is a working, unit-tested reference implementation (race-clean; the substantive guard on each primitive is revert-proven against its own test). Every concept above, plus the agent-specific extensions below, has a concrete type:

| Concept | `runtime/` type | One line |
|---|---|---|
| Mailbox (channel) | `ChanMailbox` | channel-backed inbox; buffer 0 = rendezvous |
| Select (composer) | `NewSelector` | `reflect.Select` over mailbox / done / after / cancel |
| Point-to-point | `Router` | address to mailbox delivery |
| Broadcast | `Broker` | one event fanned out to every subscriber |
| Run loop | `Run` / `RunTraced` | single-owner loop; delivers outputs via a `Dispatcher` |
| Trace | `Tracer` | every message an agent saw (`TraceRecv`) and sent (`TraceSend`) |
| Fan-in barrier | `JoinAgent` | emit once all N of a batch arrive, then reset |
| Fan-in quorum | `QuorumAgent` | emit on the first `need` of `total`; drop the stragglers |
| Structured concurrency | `Nursery` | implements `Supervisor`: depth/lineage guard, topology-enforced `Route`, subtree `Cancel` |
| Shared knowledge | `MemCorpus` | implements `Corpus`: namespaced KV reconciled by a conflict-free `Merge` |
| Bounded handoff | `Compactor` | offloads the full output to the `Corpus`, hands off a budget-bounded `Summary` + refs |
| Resource ceiling | `Budget` | a selectable `Done()` that closes on token / dollar / call / wall-clock exhaustion |
| Failure capture | `WithDeadLetters` | wraps any `Dispatcher` so an undeliverable message is captured (with its reason), not fatal |

`Router`, `Nursery`, and `WithDeadLetters` all satisfy the `Dispatcher` interface, so `Run` composes them: `Run(ctx, agent, WithDeadLetters(nursery, sink), idle)` runs an agent under a topology guard whose undeliverable outputs land in a dead-letter sink.

## Agent: the actor

An `Agent` has an address, an inbox, and a `Step` that consumes one message and may emit others. Its real state, the context window, is private and never shipped wholesale. That privacy is the point: it's what makes message passing safe without locks, and it's what makes handoff lossy (see tradeoffs).

## Mailbox: the channel

A typed conduit with an explicit buffering policy:

- **Unbuffered**: `Send` blocks until a `Recv` is ready. A *rendezvous*: synchronization, not just transfer. This is the natural default for agents because it gives backpressure for free: a producer cannot flood a consumer's context window.
- **Buffered**: `Send` blocks only when full. Decouples producer/consumer timing for fan-out worker pools.

Closing rules mirror Go and are worth stating because they're the classic footguns: only the **sender** closes; sending on a closed mailbox is a programming error; receiving from a closed mailbox yields a zero message with `ok == false`.

## Select: the composer

The whole reason to prefer this over a shared scratchpad. A `Select` blocks on several cases and proceeds on the first ready one:

```
select {
  case m := <-peer.Inbox(): handle(m)
  case <-userInterrupt:      pause()
  case <-budget.Done():      wind_down()
  case <-time.After(d):      escalate()
}
```

Everything hard about orchestration (HITL interrupts, cancellation, budget ceilings, deadlines) is one `Select`, not four polled flags on a shared board.

## Handoff: ownership transfer

A handoff moves a task from agent A to agent B. What travels is the task **plus the context B needs to act**, expressed as `References` into the shared corpus rather than inlined memory. After the send, A no longer owns the task and must not act on it. This is the single-writer guarantee restated at the agent level, and the reason there is no race to guard against.

The unavoidable tension lives here: B needs *enough* context to act, but the context window is private and serializing it is lossy and costly. The design pushes as much as possible into corpus *references* (cheap, shared, conflict-free) and accepts that the prose summary that accompanies a handoff is a lossy projection. Minimizing that loss is the open research question (see [tradeoffs.md](./tradeoffs.md)).

This is now concrete, not just a sketch: `Compactor` writes an agent's full output to a `MemCorpus` and ships a budget-bounded `Summary` plus a `MemoryRef`, and `OpenAIAgent`/`CodexAgent` opt in via `WithCompactor`. `examples/compaction` measures the difference deterministically: down a 4-hop chain the naive full-output handoff grows the prose every hop (0, 598, 1197, 1796 bytes) while the compacted one stays flat (160 bytes) and every hop's full detail is recovered by walking the refs. The *mechanism* exists; the open part is the *quality* of the projection (see tradeoffs §"open problem").

## Topology: who may message whom

CSP doesn't dictate a graph shape, but for agents the *topology* is a safety lever. An unconstrained mesh where any agent can spawn or delegate to any other produces spawn cascades and unbounded lineage. A sane default is a constrained topology: e.g. a coordinator fans out to leaf workers that cannot themselves delegate laterally (depth-capped). Open up richer topologies only behind an explicit depth/lineage guard. The `Supervisor` interface is where that policy lives, and `runtime.Nursery` implements it: `Spawn` enforces the depth cap, `Route` allows only parent-child edges (a sibling-to-sibling lateral message is rejected), and `Cancel` unwinds a whole subtree. It is also the prerequisite guard for ever opening up delegation: leaf agents stay leaves until a nursery bounds the lineage.
