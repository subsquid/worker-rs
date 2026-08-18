//! Regression tests for counterexamples found by `state_pbt`.

use std::sync::Arc;

use parking_lot::Mutex;

use sqd_worker::cli::DEFAULT_MAX_DOWNLOAD_ATTEMPTS;
use sqd_worker::storage::manager::{
    mark_assignment_settled_if_ready, AssignmentApplicationStatus, AssignmentOutcome,
    AssignmentSettled,
};
use sqd_worker::storage::state::State;
use sqd_worker::types::state::{ChunkRef, ChunkSet};

/// Shows why assignment ID and desired chunks must change atomically.
#[test]
fn mark_misattributes_application_when_observing_mixed_state() {
    let pipeline = Pipeline::new();

    pipeline
        .state
        .lock()
        .set_desired_chunks([chunk()].into_iter().collect());
    pipeline.application.lock().current_assignment_id = Some("A0".to_owned());
    pipeline.mark();
    assert_eq!(pipeline.last_applied(), None);

    // Expose A1's desired set while the current ID still names A0.
    pipeline.state.lock().set_desired_chunks(ChunkSet::new());
    pipeline.mark();

    assert_eq!(
        pipeline.last_applied(),
        Some("A0".to_owned()),
        "the misattribution hazard disappeared — mark became self-sufficient?"
    );
}

#[test]
fn atomic_registration_never_confirms_the_undownloaded_assignment() {
    let pipeline = Pipeline::new();

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

    {
        let mut application = pipeline.application.lock();
        pipeline.state.lock().set_desired_chunks(ChunkSet::new());
        application.current_assignment_id = Some("A1".to_owned());
    }
    pipeline.mark();

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
