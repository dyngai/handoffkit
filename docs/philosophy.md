# Philosophy: the fork

CSP (Hoare, 1978) and its descendants (occam, Erlang, Go) are built on one inversion:

> Instead of multiple processes touching shared state guarded by locks, have independent
> processes that *own* their data and pass it through channels. Ownership transfers with
> the message, so at any instant exactly one process is responsible for a given piece of data.

Applied to LLM agents, this is a genuine fork, because the dominant pattern today is the *other* branch.

## The two branches

**Blackboard (share memory by communicating's opposite).** Agents write to a shared substrate (a vector store, a scratchpad, a "memory") and other agents read it. Coordination is implicit: you find out what a peer did by reading the board. This is what most agent-memory systems are, and it is exactly the shared-memory model CSP reacted against. Its failure mode is also exactly the shared-memory one: two agents racing on the same record/plan with no defined single writer.

**Message passing (this repo's premise).** Agents are addressable actors. They coordinate by sending each other explicit messages. A task is *owned* by one agent at a time and moves by handoff. There is no board to race on; there is a conversation.

## What actually transfers cleanly

The deepest CSP idea that survives the jump to agents is **ownership transfer as the synchronization mechanism**. The reason blackboard agent systems get flaky is the reason shared-memory threads do: undefined single-writer. If a task is owned by exactly one agent at a time and moves only by explicit message, you recover the same reasoning simplification Go gives you: at any instant, one agent is responsible for this thing.

The second idea that transfers is **`select` as the unit of composition**. In Go, the value of channels over mutexes is not speed; it's that you can compose *waits*: block on this OR that OR cancellation, atomically. Agents need precisely this for interrupts, human-in-the-loop, budget ceilings, and timeouts. You cannot "select over multiple mutexes," and you cannot cleanly select over multiple pieces of polled shared state either.

## The boundary this repo draws

Pure message passing fails for agents the moment you try to push the org's *entire knowledge* through channels as prose: it's lossy and it's expensive (see [tradeoffs.md](./tradeoffs.md)). So the design keeps a deliberate split, mirroring Go's own "use channels *and* mutexes" pragmatism:

- **Message passing for control flow**: delegation, results, "I'm done," "I need X," cancellation. This is where ownership and `select`-composition pay off.
- **Shared corpus for knowledge**: the actual accumulated facts stay in a shared store, because channelling them as prose is the lossy/expensive trap. The default corpus merge is last-write-wins and is intended for single-writer keys such as minted handoff artifacts. If multiple agents write the same key, configure a merge policy that is associative, commutative, and idempotent. That is the mutex's job, done right: guard the small shared thing; don't channel-ify the world.

Messages carry **references** into the corpus, not inlined copies of it. The conversation moves; the library stays put.

## The punchline

What you end up with is not CSP; it's the **actor model** (Erlang/Akka): message-passing-for-coordination, but each actor keeps private, durable state and an addressable mailbox. CSP's anonymous, stateless, rendezvous-only channels are too austere for entities as heavy and long-lived as agents. CSP supplies the discipline (`select`, ownership, backpressure); actors supply the shape (identity, private state, mailboxes).
