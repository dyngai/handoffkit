# Tradeoffs: where CSP snaps for agents

CSP is elegant for goroutines because three things are true: channels are cheap and lossless, processes are stateless conduits, and each process is deterministic given its inputs. Agents violate all three. This document is the load-bearing one: the pitch in the README is only honest if you read this.

## 1. Channels are cheap and lossless. Agent messages are expensive and lossy.

A Go channel send is a pointer handoff. An agent "message" is natural-language tokens: costly to produce, **lossy to serialize** (a rich context compressed to prose), and **lossy to receive** (the reader re-interprets the prose). Every channel hop is a context-compression + re-inflation round trip, billed in tokens. In Go you'd build a five-stage pipeline without a second thought; with agents each hop degrades fidelity and costs money. **Implication:** prefer shallow topologies; push shared knowledge into corpus references rather than re-sending it as prose at every hop. This is built (`MemCorpus` + `Compactor`, opt-in on the LLM agents via `WithCompactor`) and measured (`examples/compaction`: a 4-hop chain's handoff prose grows 0 to 1796 bytes naively versus a flat 160 bytes compacted, with every hop's full detail recoverable from the refs).

## 2. Goroutines are stateless conduits. Agents carry enormous private state.

"Ownership transfers with the message" works in Go because the data *is* the thing being passed. But an agent's real state is its accumulated context window, which can't be handed over a channel without serializing, and serializing it is exactly the lossy step above. Pure message passing therefore forces a dilemma: either re-derive context on every handoff (expensive, drifts) or smuggle it through shared memory anyway (back to the blackboard). **Implication:** the design splits the difference: control flow goes by message, the corpus stays shared, and handoffs carry *references* plus a deliberately-lossy summary, not the whole window.

## 3. Determinism.

CSP's happens-before gives reasoning guarantees *because* each process is deterministic given its inputs. Agents aren't. The ordering guarantee survives (the receiver provably sees what the sender knew at send time), but the *behavioral* guarantee does not: "the receiver sees what the sender knew" does not imply "the receiver concludes what the sender would." **Implication:** don't lean on message ordering to enforce correctness the way you would in occam/Go; you still need verification at the receiving end.

## 4. Deadlock is worse, not better.

Go's runtime detects all-goroutines-blocked deadlock and panics. Two agents each politely waiting for the other to send produce no panic: just two idle context windows burning a budget until something times out. CSP's failure modes assumed a scheduler that can prove global stall; there is no such oracle here. **Implication:** every wait must have a cancellation path. A `Select` without a timeout or `Done()` case is a latent budget leak, not just a hang. `Budget` makes that ceiling a first-class, selectable value: its `Done()` closes on exhaustion, so an agent waits on "message OR cancel OR budget spent" in one `Select`, and a `Nursery` watcher can unwind a stalled subtree when the budget runs dry.

## The synthesis

The resolution is the same one Pike gives for Go itself: it is **not either/or**. Go ships mutexes *and* channels and says use channels for orchestration/handoff and mutexes for guarding a small piece of shared state. The agent translation:

- **Message passing for control flow**: delegation, results, cancellation, signalling. Ownership and `Select`-composition earn their keep here.
- **Shared corpus for knowledge**: guarded by conflict-free merge (CRDT), not channelled as prose. This is the mutex's job done right: protect the small shared thing; don't channel-ify the world.

And the model that actually fits is the **actor model**, not pure CSP: message passing for coordination, but with private durable state and addressable mailboxes per agent. CSP supplies the discipline; actors supply the shape.

## The open problem

The unsolved-research bit is **#2: minimizing handoff loss.** The *mechanism* is now built: handoffs carry corpus references plus a bounded summary, and `examples/compaction` shows the prose staying flat across a chain while the dropped detail stays recoverable. But a mechanism is not a solution. What stays genuinely open is the *quality* of the projection:

- **The summarizer is a placeholder.** The reference `Compactor` head-truncates with a marker; a real one would summarize. How good can a bounded projection be before the receiver's behavior diverges from what the full context would have produced?
- **Drift is unmeasured.** Flat prose across hops is necessary, not sufficient. The honest metric is behavioral: does B (and C, and D) conclude what the full window would have, or does a faithful-looking summary quietly steer it wrong? "The receiver sees what the sender knew" never implied "concludes what the sender would" (see §3), and a lossy summary widens that gap at every hop.
- **The split point is a knob, not a law.** How much belongs in cheap shared-corpus references versus the expensive prose summary almost certainly depends on the task, and nothing here tells you where to put the line.

Everything else in this repo is engineering, and most of it is now done. Bounding handoff drift, *that* is the question worth a repo.
