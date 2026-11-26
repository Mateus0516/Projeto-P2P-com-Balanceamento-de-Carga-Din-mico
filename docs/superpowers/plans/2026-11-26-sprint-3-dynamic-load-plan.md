# Sprint 3 Dynamic Load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for testing.

**Goal:** Complete automatic two-Master worker lending, task execution, and automatic return without changing the existing protocol contract.

**Architecture:** Reuse `PeerLoanManager` for durable offer/commit/close state and `WorkerSessionManager` for redirect/re-register/return transitions. Add a peer-negotiation thread to the saturated requester and a load-release hook to send `command_release` only for committed borrowed loans.

**Tech Stack:** Python 3.14, native TCP, NDJSON, threading, pytest, JSON ledger files.

**Spec:** `docs/superpowers/specs/2026-11-26-sprint-3-dynamic-load-design.md`

## Global Constraints

- Preserve `sd_2026_2_v1` and existing message payloads.
- No existing test may be changed to conceal failures.
- Do not commit or push.
- Validate loan_id as UUIDv4, home_master as master identity, assignment_epoch as non-negative integer, and worker identity as UUIDv4 worker.

---

### Task 1: Add failing integration regressions

**Files:**
- Modify: `tests/test_protocol.py` by appending new test cases only.

**Interfaces:**
- `PeerLoanManager.commit_mirror(...)` will mirror an accepted loan on requester Master.
- `PeerLoanManager.request_peer_help(...)` will negotiate with configured peers.
- `LoadMonitor.start(...)` will accept optional callbacks for saturation and release.

- [ ] Add tests for mirrored committed loans and strict epoch/worker mismatches.
- [ ] Add tests for the returned-worker notification handler closing the ledger.
- [ ] Add tests for performance metrics lent/received/loan contents.
- [ ] Run focused tests and verify they fail for missing behavior.

### Task 2: Implement peer negotiation and mirrored loan state

**Files:**
- Modify: `master/master.py`
- Modify: `protocol.py` only if a protocol validation gap is proven.

- [ ] Implement peer request/response TCP negotiation.
- [ ] Implement loan mirror creation on `loan_committed` and temporary registration handling.
- [ ] Implement stable-saturation callback to request one or more Workers.
- [ ] Run focused tests until green.

### Task 3: Implement automatic release and return handlers

**Files:**
- Modify: `master/master.py`
- Modify: `worker/worker.py` if release validation or return signaling is incomplete.

- [ ] Add stable-release callback issuing `command_release` to borrowed idle Workers.
- [ ] Add Master handlers for `notify_worker_returned` and `return_ack`.
- [ ] Keep the Worker on the home Master after successful return and update epoch/loan state.
- [ ] Run focused tests until green.

### Task 4: Update metrics and ledger observability

**Files:**
- Modify: `master/master.py`
- Modify: `README.md`

- [ ] Include lent/received/reserved Worker counts and loan records in performance metrics.
- [ ] Add persisted ledger evidence and integration notes.
- [ ] Run focused metrics tests until green.

### Task 5: Full verification and real integration

**Files:**
- Create: `evidencias/issue-3/` evidence files and logs.

- [ ] Run the complete pytest suite.
- [ ] Start two Masters and two Workers.
- [ ] Submit a burst through the Client.
- [ ] Capture saturation, loan offer/commit, redirect, task execution, stable release, return and ledger/metric snapshots.
- [ ] Stop processes and leave no servers running.