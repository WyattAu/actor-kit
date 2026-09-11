# Requirements — actor-kit

Numbered, testable requirements. Every requirement maps to at least one named
test or doc-comment contract; security-relevant items cite THREAT-MODEL.md rows.

Scope: OTP-style actor runtime — work-stealing scheduler, supervision trees, bounded mailboxes, deterministic sim module

## Functional

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-AK-001 | Every `send` on a live actor is delivered exactly once in the default (at-most-once transport failure) mode; ordering per sender-receiver pair is FIFO | MUST |
| REQ-AK-002 | Supervisor restart policies (`OneForOne`/`OneForAll`) honor max-restarts-within-window with escalation | MUST |
| REQ-AK-003 | Mailboxes are hard-bounded: senders observe backpressure (block or reject) and cannot grow the queue past capacity | MUST |
| REQ-AK-004 | Sim module replays identical fault schedules for identical seeds (deterministic given seed) | MUST |

## Security

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-AK-100 | A crashing actor panics inside its own task; the worker and scheduler survive and the actor moves to `Failed` | MUST |
| REQ-AK-101 | Flood of messages cannot grow memory without bound (backpressure threshold blocks/rejects) | MUST |

## Observability & API hygiene

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-AK-900 | All fallible public APIs return typed errors; production `unwrap`/`expect` is denied or explicitly justified with an invariant comment | MUST |
| REQ-AK-901 | Public items carry doc comments with runnable examples where practical | SHOULD |

Reviewed: 2026-09-11
