---
name: co-shim-context
description: Load at the start of every conversation in this repo, and whenever an error/bug is found in pseudo.txt (or other project pseudocode) before implementing a fix. Provides the full project context, invariants, settled design decisions, and known open issues for the Speculative CO-Compliant Subtransaction Shim so reviews and fixes are consistent with prior analysis.
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
- `LockNode { mode, holders: set of txn, predecessor, next }`. A node is one "generation" of lock
  holders on a key. Consecutive SHARED requests join the same node; any EXCLUSIVE request, or a
  SHARED request behind an EXCLUSIVE node, creates a new node.
- `locks_map: ConcurrentHashMap<key, chain>`. Each key has a chain (doubly linked list) of
  LockNodes ordered by arrival. The chain order *is* the conflict order on that key, and therefore
  the required commit order.
- `txn { txn_id, locks_acquired: key -> (mode, node), status, code }`.
- `transactions_map: ConcurrentHashMap<txn_id, txn>`. `txn_id` is assigned by the 2PC coordinator,
  not generated locally.

## Transaction lifecycle

Statuses: `NULL` (running) → `executed` → `prepared` → `committed`, with `must_abort` (doomed by a
cascade) and `aborted` as the failure path. A design for executor-owned cleanup adds
`abort_requested` (running and doomed) — see open issues.

1. **execute(code)**: registers the txn and runs client code, which calls `get(k)` / `put(k, v)`.
   Any `FAILED` result stops execution. On completion, CAS `NULL → executed`.
2. **get / put**: acquire SHARED / EXCLUSIVE via `lock()`, then read or write the store directly.
   Writes are in place (dirty); rollback will rely on an undo log (not yet implemented).
3. **lock(k, mode)**: appends to the key's chain, then waits until every holder of the
   predecessor node has at least `executed` (the speculation point). If a predecessor holder is
   aborted or doomed, the txn aborts.
4. **upgrade(k)**: SHARED → EXCLUSIVE. Case 1: sole holder of the tail node, upgrade in place.
   Case 2: tail node shared with others, leave it and append a new EXCLUSIVE node behind it.
   Case 3: something newer already queued behind, abort.
5. **prepare(txn_id)**: for every acquired lock, wait until all predecessor holders are
   `committed`, `aborted`, or `must_abort`. If any predecessor aborted, or the txn itself was
   marked, abort and vote NO. Otherwise CAS to `prepared` and vote YES.
6. **commit(txn_id)**: CAS `prepared → committed`, remove txn from every node, splice out nodes
   that become empty (relinking predecessor/next).
7. **abort_transaction(txn)**: CAS to `aborted`. For each EXCLUSIVE lock held, mark every holder
   of every later node on that chain `must_abort` (under the key's latch, **before** splicing).
   Then remove the txn and splice out empty nodes.
8. **mark_must_abort(txn)**: CAS `{NULL, executed} → must_abort`. Never touches `prepared`.

## Invariants that must hold

- **CO**: a transaction may only commit after every transaction ahead of it on any shared key's
  chain has resolved. Ordering can be enforced transitively through intermediate nodes (a writer
  behind a reader behind a writer commits after both), but only if the chain stays connected.
- **Chain integrity**: `predecessor`/`next` pointers and the chain's list order must always agree.
  Every new node must be appended to the list, not only linked by pointers. A node is spliced out
  only when its holder set is empty, and splicing must relink both neighbours.
- **Prepared is final for cascades**: once a txn has voted YES it must be able to commit. No
  cascade may move a `prepared` txn to `must_abort`; the prepare-time wait guarantees all its
  predecessors are already resolved.
- **Cascade before splice**: an aborting writer must mark its successors doomed before it
  disappears from the chain, so no successor can observe an empty predecessor and vote YES on
  rolled-back data.
- **Single cleanup owner for running txns**: while a txn is executing, only its execution thread
  may release its locks; other threads request the abort and the executor acts on it.
- **Wait predicates must re-read pointers**: `node.predecessor` changes while waiting because
  predecessors get spliced out. Waits must loop and re-read under the key's latch, never capture a
  node reference once.

## Settled design decisions (do not re-litigate)

- **Speculation point is `executed`**, not `prepared`: that is when a transaction's values stop
  changing. The extra cascades this causes are an accepted trade-off.
- **Concurrency control**: `ConcurrentHashMap` with per-entry (per-key) synchronization, not a
  global latch. Hold at most one key latch at a time; release multi-key commits/aborts key by key.
  Chains are created with `computeIfAbsent`.
- **Wait/notify mechanics** (condition variables, futures) are an implementation detail, not a
  protocol concern.
- **Transactions that hang or throw inside client code** are the client's problem; no execution
  timeout is required in the protocol.
- **Status changing during lock acquisition (running on data later found doomed)** is accepted:
  there is no clean way to prevent it, and prepare catches it.
- **Aborting behind aborted *readers*** (prepare aborts if any predecessor holder aborted, even a
  SHARED one) is known to over-abort; deferred for now.

## Not yet implemented (known TODOs)

- **Deadlock detection/prevention**: waits in `lock()`, `upgrade()` and `prepare()` are unbounded.
  This includes cross-shim deadlocks at prepare time, where conflict orders differ between shims.
- **Undo / write log** for rolling back in-place writes. Undo records must be written before the
  store write; undo across a cascade of writers must apply newest-first.
- **Durability** of prepared state for 2PC crash recovery.
- **Retry mechanism** and its intermediate states.

## Known open issues in the current pseudocode

1. **Upgrade case 2 never appends `new_node` to the chain list**, so `last_node()` still returns
   the shared node; later readers join ahead of the upgraded write and later writers overwrite
   `node.next`. Also `upgrade()` references an undefined `target_node` after its wait (should be
   `node`).
2. **Stale predecessor pointers in wait predicates**: after a predecessor commits or aborts and is
   spliced out, `node.predecessor` may be null (null dereference) or the captured node's holder
   set may be empty (vacuous wait, allowing a commit ahead of an earlier writer). Fix with a
   re-reading loop under the key latch; for prepare, "all earlier nodes resolved" reduces to
   `node.predecessor == null`.
3. **Cross-thread abort of a running txn**: coordinator abort, prepare on a `NULL` txn, and
   prepare after a `NULL → must_abort` cascade all call `abort_transaction` from a foreign thread
   while the executor may still be appending to `locks_acquired` or about to write. Planned fix:
   `abort_requested` status, executor-owned cleanup, status re-check inside the latch in `lock()`,
   and tombstones for aborts that arrive before `execute`.
4. **Lock leak in prepare**: when the final CAS to `prepared` fails (late cascade mark), the txn
   votes NO but is never aborted.
5. **Smaller**: `execute()` returns SUCCEEDED even on failure or failed final CAS;
   `putIfAbsent` returns null on success in Java; missing null checks for unknown txn_ids in
   `prepare`/`commit`/`abort`; duplicate prepare after commit votes NO; upgrade case 3 aborts
   common read-modify-write patterns; finished `must_abort` txns hold locks until the coordinator
   contacts them; empty chains are never removed from `locks_map` (removal must be conditional
   under the entry latch); `tartget_node` typo; stale comments ("integrate this data structure",
   "recursive").

## Guidance for working on this project

- When reviewing or changing code, check every change against the invariants above, especially
  chain integrity and cascade-before-splice. Try to construct a concrete interleaving that breaks
  CO or 2PC atomicity before accepting a change.
- Respect the settled decisions and the scoping of TODOs; don't flag deferred items as bugs unless
  asked.
- Prefer concrete failing schedules (e.g. "W(X) ← R(S) ← T(X), R aborts while T prepares") over
  abstract concerns.
