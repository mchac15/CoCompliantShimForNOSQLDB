---
name: co-shim-context
description: Load at the start of every conversation in this repo, and whenever an error/bug is found in pseudo.txt (or other project pseudocode) before implementing a fix. Provides the full project context, invariants, settled design decisions, and deferred items for the Speculative CO-Compliant Subtransaction Shim so reviews and fixes are consistent with prior analysis.
---

# Project: Speculative CO-Compliant Subtransaction Shim for NoSQL Stores

## Purpose

This project is a shim layer that sits between clients and a NoSQL store that has no native
multi-key transaction support. It executes subtransactions of global (distributed) transactions
and guarantees that the histories it produces are **Commitment Ordering (CO) compliant**: if two
transactions conflict, their commit order matches their conflict order. Because CO is preserved
locally at every shim, global serializability holds across shims coordinated by 2PC without any
global concurrency-control component.

The shim combines three mechanisms:

- **Strict Two-Phase Locking (S2PL)** for isolation between subtransactions on the same shim.
- **Two-Phase Commit (2PC)** as a participant, answering PREPARE/COMMIT/ABORT from an external
  coordinator.
- **Speculation**: a transaction does not wait for its conflicting predecessors to *commit* before
  it starts using their data. It waits only until they have finished *executing*. From that point
  on, their values no longer change, so successors can speculatively read and write on top of
  them. The ordering cost is paid later, at prepare time, where a transaction must wait until all
  its predecessors have actually committed (or aborted) before voting.

The price of speculation is **cascading aborts**: if a writer aborts, every transaction that
speculated on its data must abort too.

## Core data structures

- `lock_mode ∈ {SHARED, EXCLUSIVE}`.
- `LockNode { mode, holders: set of txn, predecessor, next, upgraded }`. A node is one "generation"
  of lock holders on a key. Consecutive SHARED requests join the same node; any EXCLUSIVE request,
  or a SHARED request behind an EXCLUSIVE node, creates a new node. `upgraded` (default false) means
  the holder read the key before writing it (set by `upgrade()`); a node from a direct `put` is a
  blind write.
- `chain { head, tail }` per key, doubly linked through `predecessor`/`next`; `locks_map:
  ConcurrentHashMap<key, chain>`. The chain order *is* the conflict order on that key, and therefore
  the required commit order. Empty chains are removed from `locks_map` under the latch.
- `txn { txn_id, locks_acquired: key -> (mode, node), status, code, write_buffer: key -> value }`.
- `transactions_map: ConcurrentHashMap<txn_id, txn>`. `txn_id` is assigned by the 2PC coordinator.
  A txn is removed from it when it is committed or aborted.
- `lock_timeout`: bounds the waits of the execution phase.

## Transaction lifecycle

Statuses: `started` (running) → `executed` → `prepared` → `committing` → `committed`, with
`must_abort` (doomed by a cascade) and `aborted` as the failure path.

Writes are **buffered** (`write_buffer`), not in place: nothing touches the store before `commit`,
so there is no undo log. The buffer is final once the txn is `executed`. Nothing is logged and there
is no crash recovery (out of scope).

1. **execute(txn_id, code)**: registers the txn and runs client code, which calls `get(k)` /
   `put(k, v)`. Any `FAILED` result stops execution. On completion, CAS `started → executed`; on any
   failure it calls the idempotent `abort_transaction`.
2. **get / put**: acquire SHARED / EXCLUSIVE via `lock()`. `put` fills `write_buffer`. `get` returns
   the txn's own buffered write, else the buffer of the nearest EXCLUSIVE node at or before its own
   node (safe: `lock()` returns only once every predecessor holder is at least `executed`), else the
   store value (the writer, if any, already committed and applied its buffer).
3. **lock(k, mode)**: appends to the key's chain (after re-checking the txn is not doomed, under the
   latch), then waits until every holder of the predecessor node has at least `executed` (the
   speculation point). If a predecessor holder is aborted or doomed, the txn aborts. The wait has a
   deadline (`lock_timeout`): on expiry the txn aborts (deadlock breaking).
4. **upgrade(k)**: SHARED → EXCLUSIVE, moving the txn only across readers, never across a writer.
   Skip the SHARED nodes behind the txn's node to `p`, look at `after = p.next`. If `after.upgraded`
   (RMW vs RMW): abort. Else if the txn is the sole holder and `p` is its node: flip in place. Else
   leave the node and put a new EXCLUSIVE node right behind `p`: at the tail, or in front of the
   blind writer `after` (whose predecessor pointer is fixed so it also waits for the upgrader). The
   wait for the readers ahead is bounded by `lock_timeout` too.
5. **prepare(txn_id)**: for every acquired lock, wait until all predecessor holders are
   `committed`, `aborted`, or `must_abort`. If any predecessor aborted, or the txn itself was
   marked, abort and vote NO. Otherwise CAS to `prepared` and vote YES. No timeout here: cross-shim
   cycles are the coordinator's problem. A duplicate prepare on `prepared/committing/committed`
   re-sends YES.
6. **commit(txn_id)**: CAS `prepared → committing` (so an abort can no longer take it and a
   successor's prepare cannot pass early), apply `write_buffer` to the store, remove the txn from
   every node and splice out nodes that become empty, set `committed`, evict from `transactions_map`.
7. **abort_transaction(txn)**: CAS to `aborted`. For each EXCLUSIVE lock held, mark every holder
   of every later node on that chain `must_abort` (under the latch, **before** splicing).
   Then remove the txn, splice out empty nodes, evict from `transactions_map`.
8. **mark_must_abort(txn)**: CAS `{started, executed} → must_abort`. Never touches `prepared`.

## Invariants that must hold

- **CO**: a transaction may only commit after every transaction ahead of it on any shared key's
  chain has resolved. Ordering can be enforced transitively through intermediate nodes (a writer
  behind a reader behind a writer commits after both), but only if the chain stays connected.
- **Chain integrity**: `predecessor`/`next` pointers and the chain's head/tail must always agree.
  Every new node must be appended (or inserted), not only linked by pointers. A node is spliced out
  only when its holder set is empty, and splicing must relink both neighbours.
- **Prepared is final for cascades**: once a txn has voted YES it must be able to commit. No
  cascade may move a `prepared` txn to `must_abort`; the prepare-time wait guarantees all its
  predecessors are already resolved.
- **Cascade before splice**: an aborting writer must mark its successors doomed before it
  disappears from the chain, so no successor can observe an empty predecessor and vote YES on
  rolled-back data.
- **No leaked node from a foreign abort**: a foreign thread may abort a running txn
  (`abort_transaction`), so `lock()`/`upgrade()` re-check the status inside the latch before
  appending, and the status check and the `locks_acquired` update must be atomic w.r.t. that abort's
  read of `locks_acquired`. `execute()` always calls the idempotent `abort_transaction` on FAILED.
- **An upgrade never moves a txn across a writer**: otherwise the value it read is stale when its
  own write takes effect (lost update).
- **Wait predicates must re-read pointers**: `node.predecessor` changes while waiting because
  predecessors get spliced out or a node is inserted in front. Waits must loop and re-read under the
  latch, never capture a node reference once.

## Settled design decisions (do not re-litigate)

- **Speculation point is `executed`**, not `prepared`: that is when a transaction's values stop
  changing. The extra cascades this causes are an accepted trade-off.
- **Concurrency control**: `ConcurrentHashMap` with per-entry (per-key) synchronization, not a
  global latch. Hold at most one key latch at a time; release multi-key commits/aborts key by key.
  Chains are created with `computeIfAbsent`. The pseudocode's `atomic(locks_map)` is one critical
  section; an implementation may refine it if the status re-check stays atomic (see invariants).
- **Wait/notify mechanics** (condition variables, futures) are an implementation detail, not a
  protocol concern.
- **Transactions that hang or throw inside client code** are the client's problem; no execution
  timeout is required in the protocol.
- **Status changing during lock acquisition (running on data later found doomed)** is accepted:
  there is no clean way to prevent it, and prepare catches it.
- **Aborting behind aborted *readers*** (prepare aborts if any predecessor holder aborted, even a
  SHARED one) is known to over-abort; deferred for now.
- **No crash recovery / durability**: out of scope; the shim is assumed not to crash between voting
  YES and the decision.
- **The 2PC coordinator owns the txn lifecycle**: it calls `execute` at most once per txn_id, sends
  no prepare/commit/abort before `execute` was delivered, and garbage-collects. The shim evicts a
  txn on commit/abort; later messages hit the unknown-id branches. No tombstones.
- **Deadlocks**: only the execution phase is handled, by timeouts (`lock_timeout`). Waits in
  `prepare()` have no timeout; a cross-shim cycle there is a 2PC-level matter left to the
  coordinator.
- **Retry** is the coordinator's job (fresh txn_id), not a shim state.

## Deferred (not bugs unless asked)

- Cascade could stop at the next blind writer instead of dooming every later node.
- A doomed-but-finished (`executed → must_abort`) txn holds its locks until the coordinator contacts
  it (successors and prepare treat it as aborted meanwhile).
- Blocked `prepare` handlers can exhaust a thread pool (implementation note).
- Values are assumed non-null (`get` uses null as "no predecessor version").

## Guidance for working on this project

- When reviewing or changing code, check every change against the invariants above, especially
  chain integrity and cascade-before-splice. Try to construct a concrete interleaving that breaks
  CO or 2PC atomicity before accepting a change.
- Respect the settled decisions and the scoping of TODOs; don't flag deferred items as bugs unless
  asked.
- Prefer concrete failing schedules (e.g. "W(X) ← R(S) ← T(X), R aborts while T prepares") over
  abstract concerns.
