// Quint model of the OneForOne supervision strategy in actor-kit's
// src/supervisor.rs.
//
// ── Mapping model actions → real code ─────────────────────────────────────
//
//   crash(c)   ~ `Supervisor::handle_child_exit(name, ExitReason::Error(_))`
//                for a `RestartPolicy::Permanent` child (should_restart ==
//                true), which synchronously reaps the exit:
//     - restart grant: `check_restart_allowed` true (recent count <
//       max_restarts) → `restart_one_for_one` → `record_restart` → the
//       child enters `ChildState::Restarting { attempt }` with an
//       incremented per-child restart count.
//     - escalation:    `check_restart_allowed` false (recent count ≥
//       max_restarts) → `handle_max_restarts_exceeded` → the child enters
//       `ChildState::Stopped` and the supervisor returns
//       `Err(MaxRestartsExceeded { .. })`.
//     `handle_child_exit` is synchronous, so the model has no intermediate
//     "crashed" state: `ChildState` never holds one on this code path, and
//     the crash action resolves the exit in a single step.
//   resume(c)  ~ `Supervisor::mark_child_running`: a `Restarting` child
//     comes back up and enters `ChildState::Running`.
//
// Model scope: one supervisor, 3 children (indices 0, 1, 2), every child
// `RestartPolicy::Permanent`, exits always abnormal. The supervisor tree
// (`SupervisorTree`, OneForAll/RestForOne, significant-child escalation to
// a parent) and normal/shutdown exits (the no-restart path) are out of
// scope.
//
// ── Abstraction: the `within` time window (v1) ────────────────────────────
//
// `SupervisionStrategy::OneForOne { max_restarts, within }` counts restarts
// only within a rolling time window (`check_restart_allowed` filters
// timestamps by `now - t < within`, and `record_restart` retains them the
// same way). v1 models the window as *unbounded*: no time passes in the
// model, so recorded restarts never age out and "recent count" == "total
// count". This is the conservative direction — an exhausted budget in the
// model is exactly the worst case of the real windowed check. Adding a
// discrete clock with a `tick` action that expires old timestamps is the
// planned refinement.
//
// ── State space ───────────────────────────────────────────────────────────
//
// Per child: running × restart-counts {0,1,2} (3) ∪ restarting × {1,2} (2)
// ∪ stopped (1) = 6 configurations → 6³ = 216 global states. max_restarts
// is pinned to 2 (matches the `one_for_one(2, _)` shape of the unit test
// `test_supervisor_max_restarts_exceeded`) to keep the space small; the
// invariants are parametric in MAX_RESTARTS.
module supervisor {
    /** Children are identified by 0, 1, 2. */
    pure def CHILDREN = Set(0, 1, 2)

    /**
     * max_restarts of the modeled OneForOne strategy
     * (`SupervisionStrategy::one_for_one(2, _)`).
     */
    pure def MAX_RESTARTS = 2

    /** The `ChildState` values reachable on the crash/restart path. */
    pure def LIFECYCLE = Set("running", "restarting", "stopped")

    // Mirrors `ChildState`: "running" | "restarting" | "stopped".
    // ("Stopping" is only reachable via explicit graceful shutdown, not the
    // crash path, and is not modeled.)
    var state: int -> str

    // Per-child restart count in the (unbounded) window —
    // `restart_counts` in `Supervisor` under the v1 abstraction.
    var restarts: int -> int

    // escalated(c): c was permanently stopped by
    // `handle_max_restarts_exceeded`.
    var escalated: int -> bool

    action init = all {
        state' = Map(0 -> "running", 1 -> "running", 2 -> "running"),
        restarts' = Map(0 -> 0, 1 -> 0, 2 -> 0),
        escalated' = Map(0 -> false, 1 -> false, 2 -> false),
    }

    /**
     * A Running child crashes; the supervisor handles the exit in the same
     * step (`handle_child_exit` → `check_restart_allowed`):
     * - budget left (count < max_restarts): `record_restart` grants a
     *   restart, the child becomes Restarting with count+1;
     * - budget exhausted (count == max_restarts):
     *   `handle_max_restarts_exceeded` permanently Stops the child and the
     *   supervisor returns Err (modeled by `escalated' = true`).
     * The branch is deterministic in the count — exactly as in the code,
     * where the same `check_restart_allowed` predicate picks the path.
     */
    action crash = {
        nondet c = CHILDREN.oneOf()
        if (restarts.get(c) < MAX_RESTARTS) {
            all {
                state.get(c) == "running",
                restarts' = restarts.put(c, restarts.get(c) + 1),
                state' = state.put(c, "restarting"),
                escalated' = escalated,
            }
        } else {
            all {
                state.get(c) == "running",
                state' = state.put(c, "stopped"),
                escalated' = escalated.put(c, true),
                restarts' = restarts,
            }
        }
    }

    /** `mark_child_running`: a restarted child process came back up. */
    action resume = {
        nondet c = CHILDREN.oneOf()
        all {
            state.get(c) == "restarting",
            state' = state.put(c, "running"),
            restarts' = restarts,
            escalated' = escalated,
        }
    }

    action step = any {
        crash,
        resume,
    }

    // ── Invariants ────────────────────────────────────────────────────────

    /**
     * INV1: every child is in exactly one of {Running, Restarting, Stopped}.
     *
     * `state` is a function into `LIFECYCLE`, so a child holds at most one
     * lifecycle value by construction; this asserts the value is always
     * one of the three reachable `ChildState`s (no fourth state is ever
     * minted).
     */
    def inv1_child_in_exactly_one_lifecycle_state =
        CHILDREN.forall(c => LIFECYCLE.contains(state.get(c)))

    /**
     * INV2: if a child is Running then its restart count ≤ max_restarts.
     *
     * `record_restart` only fires under `check_restart_allowed`
     * (recent < max_restarts), so the count can never pass max_restarts —
     * for *any* state, in particular Running.
     */
    def inv2_running_implies_restart_budget_ok =
        CHILDREN.forall(c =>
            state.get(c) == "running" implies restarts.get(c) <= MAX_RESTARTS
        )

    /**
     * INV3: an escalated child never returns to Running.
     *
     * `handle_max_restarts_exceeded` is the only producer of `escalated`,
     * and it Stops the child; no action transitions out of "stopped"
     * (`crash` requires "running", `resume` requires "restarting"), so an
     * escalated child is Stopped forever.
     */
    def inv3_escalated_child_never_runs_again =
        CHILDREN.forall(c => escalated.get(c) implies state.get(c) == "stopped")

    /** Conjunction of all model invariants, asserted after every step. */
    def all_invariants = and {
        inv1_child_in_exactly_one_lifecycle_state,
        inv2_running_implies_restart_budget_ok,
        inv3_escalated_child_never_runs_again,
    }

    /** One step, with the invariants checked on the resulting state. */
    action checked_step = all {
        step,
        assert(all_invariants),
    }

    /**
     * Experiment: init, then 30 checked steps. 30 covers the worst-case
     * drain of all three children (crash/resume/crash/resume/crash = 5
     * steps per child, 15 total) with slack; the full reachable space is
     * the 216 states described in the header.
     */
    run supervisor_one_for_one = init.then(30.reps(_ => checked_step))
}
