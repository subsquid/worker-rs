//! Deterministic regression tests pinning counterexamples found by the
//! property-based tests in `state_pbt.rs`.
//!
//! Counterexample: assignment A0 desires one chunk that is never downloaded;
//! assignment A1 desires nothing. A settled-check that observes A1's
//! (trivially satisfied) chunk state while `current_assignment_id` still
//! points at A0 confirms A0 as applied even though its chunk is missing.

use std::sync::Arc;

use parking_lot::Mutex;

use sqd_worker::cli::DEFAULT_MAX_DOWNLOAD_ATTEMPTS;
use sqd_worker::storage::manager::{
    mark_assignment_settled_if_ready, AssignmentApplicationStatus, AssignmentOutcome,
    AssignmentSettled,
};
use sqd_worker::storage::state::State;
use sqd_worker::types::state::{ChunkRef, ChunkSet};

// The counterexample replayed verbatim against the settled-check. The wrong
// confirmation DOES happen: the mark function cannot detect the inconsistency
// by itself — the guarantee lives in `set_assignment`, which updates the
// desired chunks and the current id under one critical section. If this test
// ever fails, mark became self-sufficient and that critical section can be
// revisited.
#[test]
fn mark_misattributes_application_when_observing_mixed_state() {
    let pipeline = Pipeline::new();

    // A0 registered: desires one chunk, which never gets downloaded
    pipeline
        .state
        .lock()
        .set_desired_chunks([chunk()].into_iter().collect());
    pipeline.application.lock().current_assignment_id = Some("A0".to_owned());
    pipeline.mark();
    assert_eq!(pipeline.last_applied(), None);

    // The mixed observation `set_assignment`'s critical section rules out:
    // A1's empty chunk set is visible while the current id still says A0
    pipeline.state.lock().set_desired_chunks(ChunkSet::new());
    pipeline.mark();

    assert_eq!(
        pipeline.last_applied(),
        Some("A0".to_owned()),
        "the misattribution hazard disappeared — mark became self-sufficient?"
    );
}

// The same trace with desired chunks and current id changing atomically, as
// `set_assignment` does: A0 — never applied — is never confirmed; A1 is
// confirmed as itself.
#[test]
fn atomic_registration_never_confirms_the_undownloaded_assignment() {
    let pipeline = Pipeline::new();

    // A0 registered atomically: desires one chunk, which never gets downloaded
    {
        let mut application = pipeline.application.lock();
        pipeline
            .state
            .lock()
            .set_desired_chunks([chunk()].into_iter().collect());
        application.current_assignment_id = Some("A0".to_owned());
    }
    pipeline.mark();
    assert_eq!(pipeline.last_applied(), None);

    // A1 registered atomically: desires nothing
    {
        let mut application = pipeline.application.lock();
        pipeline.state.lock().set_desired_chunks(ChunkSet::new());
        application.current_assignment_id = Some("A1".to_owned());
    }
    pipeline.mark();

    // A1 is genuinely applied and confirmed as itself; A0 never was
    assert_eq!(pipeline.last_applied(), Some("A1".to_owned()));
    assert_eq!(
        *pipeline.settled_tx.borrow(),
        Some(AssignmentSettled {
            id: "A1".to_owned(),
            outcome: AssignmentOutcome::Applied,
        })
    );
}

struct Pipeline {
    state: Mutex<State>,
    application: Mutex<AssignmentApplicationStatus>,
    settled_tx: tokio::sync::watch::Sender<Option<AssignmentSettled>>,
}

impl Pipeline {
    fn new() -> Self {
        let (settled_tx, _) = tokio::sync::watch::channel(None);
        Self {
            state: Mutex::new(State::new(ChunkSet::new(), DEFAULT_MAX_DOWNLOAD_ATTEMPTS)),
            application: Mutex::new(AssignmentApplicationStatus::default()),
            settled_tx,
        }
    }

    fn mark(&self) {
        mark_assignment_settled_if_ready(&self.state, &self.application, &self.settled_tx);
    }

    fn last_applied(&self) -> Option<String> {
        self.application.lock().last_applied_assignment_id.clone()
    }
}

fn chunk() -> ChunkRef {
    ChunkRef::new(
        Arc::new("ds".to_owned()),
        Arc::from("0000000000/0000000000-0000000001-00000000"),
    )
}
