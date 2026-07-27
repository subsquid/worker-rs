//! Property-based tests for the chunk [`State`] machine and the assignment
//! confirmation logic. Random operation sequences are folded over the state
//! while checking, at every step, the guarantees the module is built around:
//!
//! 1. Deletion before download: a download is never handed out while any chunk
//!    on disk or in flight is not part of the current assignment — except
//!    condemned chunks (undesired but still query-held), whose overcommit is
//!    bounded by the locks held at the assignment switch.
//! 2. No wedge states: whenever downloads are refused, the blockage is
//!    attributable to a pending removal or a condemned copy that a future
//!    event clears, and draining all pending events always reaches a terminal
//!    state with nothing condemned left.
//! 3. Correct confirmation: an assignment is only ever marked applied when all
//!    of its chunks are actually available; a stalled assignment never is.

use std::sync::Arc;

use proptest::prelude::*;

use sqd_worker::storage::state::{State, MAX_DOWNLOAD_ATTEMPTS};
use sqd_worker::types::state::{ChunkRef, ChunkSet};

/// Small chunk universe so random subsets collide and re-assignments overlap.
const UNIVERSE: usize = 8;

/// Upper bound on drain iterations; generously above any reachable amount of
/// pending work, so hitting it means the state machine is wedged.
const DRAIN_STEP_LIMIT: usize = 10_000;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn random_op_sequences_uphold_ordering_liveness_and_bookkeeping(
        ops in prop::collection::vec(arb_op(), 1..200),
    ) {
        let mut state = State::new(ChunkSet::new());
        let mut shadow = Shadow::default();

        for op in &ops {
            apply_op(&mut state, &mut shadow, op);
            state.assert_invariants();
        }
        drain(&mut state, &mut shadow);
    }

    // The attempt cap in isolation: however the failures are interleaved with
    // other work, a chunk is offered at most MAX_DOWNLOAD_ATTEMPTS times per
    // assignment when every attempt fails.
    #[test]
    fn a_chunk_is_never_attempted_more_than_the_cap_per_assignment(
        target in 0..UNIVERSE,
        ops in prop::collection::vec(arb_op(), 1..100),
    ) {
        let mut state = State::new(ChunkSet::new());
        let mut shadow = Shadow::default();
        let mut attempts_since_assignment = 0u32;
        let target = chunk(target);

        for op in &ops {
            if matches!(op, Op::NewAssignment(_)) {
                attempts_since_assignment = 0;
            }
            // Force every download of the target chunk to fail immediately
            if matches!(op, Op::TakeDownload) {
                if let Some(chunk) = state.take_next_download() {
                    assert_no_overcommit(&state);
                    if chunk == target {
                        attempts_since_assignment += 1;
                        prop_assert!(
                            attempts_since_assignment <= MAX_DOWNLOAD_ATTEMPTS,
                            "chunk retried past the attempt cap within one assignment"
                        );
                        state.complete_download(&chunk, false);
                    } else {
                        shadow.in_flight.push(chunk);
                    }
                }
                state.assert_invariants();
                continue;
            }
            apply_op(&mut state, &mut shadow, op);
            state.assert_invariants();
        }
    }
}

#[derive(Debug, Clone)]
enum Op {
    /// A new assignment arrives, desiring the given subset of the universe.
    NewAssignment(Vec<usize>),
    /// The manager loop asks for the next download to start.
    TakeDownload,
    /// An in-flight download finishes; `pick` selects which one.
    CompleteDownload { pick: usize, success: bool },
    /// A query locks the chunk with the given universe index (no-op if it
    /// isn't available).
    Lock { target: usize },
    /// A query finishes, releasing one outstanding lock; `pick` selects which.
    Unlock { pick: usize },
    /// The manager loop collects removable chunks.
    TakeRemovals,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        1 => prop::collection::vec(0..UNIVERSE, 0..=UNIVERSE).prop_map(Op::NewAssignment),
        4 => Just(Op::TakeDownload),
        4 => (0..UNIVERSE, any::<bool>())
            .prop_map(|(pick, success)| Op::CompleteDownload { pick, success }),
        2 => (0..UNIVERSE).prop_map(|target| Op::Lock { target }),
        2 => (0..UNIVERSE).prop_map(|pick| Op::Unlock { pick }),
        2 => Just(Op::TakeRemovals),
    ]
}

/// Shadow bookkeeping mirroring what the manager and the queries hold: which
/// downloads are in flight and which locks are outstanding. Needed both to
/// issue only legal `complete_download`/`unlock_chunk` calls and to drain the
/// state at the end.
#[derive(Default)]
struct Shadow {
    in_flight: Vec<ChunkRef>,
    locks_held: Vec<ChunkRef>,
}

fn apply_op(state: &mut State, shadow: &mut Shadow, op: &Op) {
    match op {
        Op::NewAssignment(indexes) => {
            state.set_desired_chunks(chunk_set(indexes));
        }
        Op::TakeDownload => take_download(state, shadow),
        Op::CompleteDownload { pick, success } => {
            if shadow.in_flight.is_empty() {
                return;
            }
            let chunk = shadow.in_flight.swap_remove(pick % shadow.in_flight.len());
            state.complete_download(&chunk, *success);
        }
        Op::Lock { target } => {
            let target = chunk(*target);
            let condemned = state.is_condemned(&target);
            if let Some(locked) =
                state.get_and_lock_chunk(target.dataset.clone(), target.chunk.clone())
            {
                assert!(!condemned, "a condemned chunk accepted a new query lock");
                shadow.locks_held.push(locked);
            }
        }
        Op::Unlock { pick } => {
            if shadow.locks_held.is_empty() {
                return;
            }
            let chunk = shadow
                .locks_held
                .swap_remove(pick % shadow.locks_held.len());
            state.unlock_chunk(&chunk);
        }
        Op::TakeRemovals => {
            for removed in state.take_removals() {
                assert!(
                    !shadow.locks_held.contains(&removed),
                    "removed a chunk still locked by a query"
                );
                // A desired chunk may be deleted only as a stale condemned
                // copy making way for its own re-download
                assert!(
                    !state.desired().contains(&removed)
                        || state.queued_downloads().contains(&removed),
                    "removed a desired chunk that is not queued for re-download"
                );
            }
        }
    }
}

fn take_download(state: &mut State, shadow: &mut Shadow) {
    match state.take_next_download() {
        Some(chunk) => {
            assert_no_overcommit(state);
            assert!(
                !state.is_condemned(&chunk),
                "handed out a download whose stale condemned copy is still on disk"
            );
            shadow.in_flight.push(chunk);
        }
        None => {
            // Guarantee 2 (accountability): a refusal with work still queued
            // must be attributable to something a future event clears — a
            // pending removal, or queued chunks whose stale condemned copies
            // are still held by queries.
            if state.has_queued_downloads() {
                assert!(
                    state.has_pending_removals()
                        || state
                            .queued_downloads()
                            .iter()
                            .all(|chunk| state.is_condemned(chunk)),
                    "downloads refused with work queued but nothing to remove"
                );
            }
        }
    }
}

/// Guarantee 2 — liveness. Deliver every pending event (downloads succeed,
/// queries finish, removals proceed) and require a terminal verdict: either
/// fully applied or stalled, never a wedge.
fn drain(state: &mut State, shadow: &mut Shadow) {
    for _ in 0..DRAIN_STEP_LIMIT {
        state.take_removals();
        if let Some(chunk) = shadow.in_flight.pop() {
            state.complete_download(&chunk, true);
            state.assert_invariants();
            continue;
        }
        if let Some(chunk) = shadow.locks_held.pop() {
            state.unlock_chunk(&chunk);
            state.assert_invariants();
            continue;
        }
        if let Some(chunk) = state.take_next_download() {
            assert_no_overcommit(state);
            state.complete_download(&chunk, true);
            state.assert_invariants();
            continue;
        }
        // Quiescent: exactly one terminal verdict must hold, and no undesired
        // or condemned data may remain on disk.
        assert!(
            state.is_fully_applied() ^ state.is_stalled(),
            "quiescent state is neither applied nor stalled (or claims both)"
        );
        assert!(
            !state.has_pending_removals(),
            "quiescent state still holds undesired chunks"
        );
        assert!(
            !state.has_condemned(),
            "quiescent state still holds condemned chunks"
        );
        return;
    }
    panic!("state machine did not reach quiescence within {DRAIN_STEP_LIMIT} steps");
}

/// Guarantee 1 — deletion before download. Checked at the only moment it can
/// be violated: when a download is handed out, everything available and in
/// flight must belong to the current assignment. Condemned chunks are the one
/// deliberate exception — still on disk, but bounded by the query locks held
/// at the assignment switch and invisible to new queries.
fn assert_no_overcommit(state: &State) {
    assert!(
        state.available().is_subset(state.desired()),
        "download handed out while an undesired chunk is still on disk"
    );
    assert!(
        state.downloading().is_subset(state.desired()),
        "download handed out while an undesired chunk is still in flight"
    );
}

fn chunk(i: usize) -> ChunkRef {
    // Two datasets so the per-dataset download scheduling is exercised too
    let dataset = if i < UNIVERSE / 2 { "ds0" } else { "ds1" };
    ChunkRef {
        dataset: Arc::new(dataset.to_owned()),
        chunk: Arc::from(format!(
            "0000000000/000000000{}-000000000{}-00000000",
            i,
            i + 1
        )),
    }
}

fn chunk_set(indexes: &[usize]) -> ChunkSet {
    indexes.iter().map(|&i| chunk(i)).collect()
}

/// Guarantee 3 — confirmation correctness: the real check-and-mark critical
/// section under randomized interleavings of the assignment pipeline with the
/// state loop's own mark and download-progress steps.
mod confirmation {
    use parking_lot::Mutex;

    use super::*;
    use sqd_worker::storage::manager::{
        mark_assignment_settled_if_ready, AssignmentApplicationStatus, AssignmentOutcome,
        AssignmentSettled,
    };

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn only_genuinely_applied_assignments_are_confirmed(scripts in arb_scripts()) {
            let (settled_tx, _settled_rx) = tokio::sync::watch::channel(None);
            let mut pipeline = Pipeline {
                state: Mutex::new(State::new(ChunkSet::new())),
                application: Mutex::new(AssignmentApplicationStatus::default()),
                settled_tx,
                chunk_sets: Vec::with_capacity(scripts.len()),
            };

            for (i, (subset, after)) in scripts.iter().enumerate() {
                let chunks = chunk_set(subset);
                pipeline.chunk_sets.push(chunks.clone());
                // Mirrors `set_assignment`: the desired chunks and the current
                // id are updated under one critical section w.r.t. the
                // settled-check, followed by its own mark call.
                {
                    let mut application = pipeline.application.lock();
                    pipeline.state.lock().set_desired_chunks(chunks);
                    application.current_assignment_id = Some(format!("A{i}"));
                }
                pipeline.mark()?;
                for op in after {
                    pipeline.run(op)?;
                }
            }
        }
    }

    /// One assignment: its chunk subset and the ops that run after it is
    /// registered. Desired chunks and `current_assignment_id` change
    /// atomically w.r.t. the settled-check, as in `set_assignment`, so no ops
    /// interleave between them; the mixed-observation hazard is pinned in
    /// `src/storage/regression.rs`.
    type Script = (Vec<usize>, Vec<MidOp>);

    fn arb_scripts() -> impl Strategy<Value = Vec<Script>> {
        prop::collection::vec(
            (
                prop::collection::vec(0..UNIVERSE, 0..=UNIVERSE),
                prop::collection::vec(arb_mid(), 0..8),
            ),
            1..5,
        )
    }

    #[derive(Debug, Clone)]
    enum MidOp {
        /// The state loop's periodic check runs.
        Mark,
        /// One download is taken and completed with the given outcome.
        Progress(bool),
    }

    fn arb_mid() -> impl Strategy<Value = MidOp> {
        prop_oneof![
            1 => Just(MidOp::Mark),
            2 => any::<bool>().prop_map(MidOp::Progress),
        ]
    }

    struct Pipeline {
        state: Mutex<State>,
        application: Mutex<AssignmentApplicationStatus>,
        settled_tx: tokio::sync::watch::Sender<Option<AssignmentSettled>>,
        /// The chunk set each registered assignment id desires.
        chunk_sets: Vec<ChunkSet>,
    }

    impl Pipeline {
        fn run(&self, op: &MidOp) -> Result<(), TestCaseError> {
            match op {
                MidOp::Mark => self.mark()?,
                MidOp::Progress(success) => self.progress(*success),
            }
            Ok(())
        }

        fn mark(&self) -> Result<(), TestCaseError> {
            let before = self.application.lock().last_applied_assignment_id.clone();
            mark_assignment_settled_if_ready(&self.state, &self.application, &self.settled_tx);
            let after = self.application.lock().last_applied_assignment_id.clone();

            if after != before {
                let id = after.expect("last_applied only ever advances to a concrete id");
                let idx: usize = id[1..].parse().expect("ids are generated as A<idx>");
                let state = self.state.lock();
                prop_assert!(
                    self.chunk_sets[idx].is_subset(state.available()),
                    "assignment {id} confirmed while some of its chunks are missing"
                );
            }
            if let Some(settled) = self.settled_tx.borrow().as_ref() {
                if settled.outcome == AssignmentOutcome::Stalled {
                    let last_applied = self.application.lock().last_applied_assignment_id.clone();
                    prop_assert_ne!(
                        last_applied.as_ref(),
                        Some(&settled.id),
                        "a stalled assignment must never be confirmed as applied"
                    );
                }
            }
            Ok(())
        }

        fn progress(&self, success: bool) {
            let mut state = self.state.lock();
            if let Some(chunk) = state.take_next_download() {
                state.complete_download(&chunk, success);
            }
        }
    }
}
