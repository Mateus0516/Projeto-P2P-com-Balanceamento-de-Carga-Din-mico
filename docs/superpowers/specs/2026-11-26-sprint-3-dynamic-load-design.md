# Sprint 3 Dynamic Load Design

## Goal
Complete dynamic P2P worker lending between Masters while preserving the existing `sd_2026_2_v1` protocol and Sprint 1/2 behavior.

## Current State
The repository contains Sprint 3 protocol builders and partial `PeerLoanManager` and worker redirect logic. The baseline suite has 144 passing tests. Missing runtime integration is in automatic peer negotiation, committed-loan mirroring, master return handlers, load-triggered release, and metrics/ledger exposure.

## Protocol
Keep the existing envelope fields and message types: `request_help`, `response_accepted`, `response_rejected`, `confirm_loan`, `loan_committed`, `command_redirect`, `register_temporary_worker`, `register_worker`, `command_release`, `notify_worker_returned`, `return_ack`, `task_request`, `task_assignment`, `task_result`, `task_ack`, and `performance_report`.

## Loan Lifecycle
1. Saturated requester Master contacts configured peer Masters with `request_help`.
2. Peer Master validates capacity and reserves one idle worker, creating an `offered` ledger record.
3. Requester confirms the offer with `confirm_loan`; peer commits and redirects the worker over its existing Worker session.
4. Worker validates loan identity and epoch, switches to requester Master, and re-registers as temporary with `home_master`, `loan_id`, and new `assignment_epoch`.
5. Requester assigns tasks and receives results normally, with strict worker, loan, epoch, task and attempt validation.
6. After stable low load, requester releases the loan and sends `command_release` to the temporary Worker.
7. Worker returns to `home_master`, re-registers with the home identity and sends `notify_worker_returned`.
8. Home Master closes the ledger and updates the Worker back to available; metrics reflect lent, received, reserved, active and loan state counters.

## Validation
- Reject unknown or malformed loan IDs.
- Reject mismatched home, target or requester Master identity.
- Reject stale or mismatched assignment epochs.
- Reject wrong worker IDs and duplicate/conflicting requests.
- Never treat a pending task count from the peer offer as the lender's local load.

## Observability
Ledger files remain persistent per Master. Performance metrics include loan records and accurate lent/received/reserved counters. Evidence logs and snapshots are saved under `evidencias/issue-3/`.

## Compatibility
No existing Sprint 1 or Sprint 2 message types or payload fields are changed. Existing tests are not modified merely to make failures pass.