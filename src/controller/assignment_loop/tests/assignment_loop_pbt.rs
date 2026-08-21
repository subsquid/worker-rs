use super::*;
use proptest::prelude::*;

#[derive(Clone, Copy, Debug)]
enum PublishedFault {
    Good,
    Refused,
    Transient,
}

#[derive(Clone, Copy, Debug)]
struct PublishedPair {
    assignment: u8,
    bundle: u8,
    assignment_fault: PublishedFault,
    bundle_fault: PublishedFault,
}

fn published_pair() -> impl Strategy<Value = PublishedPair> {
    let fault = || {
        prop_oneof![
            4 => Just(PublishedFault::Good),
            2 => Just(PublishedFault::Refused),
            2 => Just(PublishedFault::Transient),
        ]
    };
    (0u8..4, 0u8..4, fault(), fault()).prop_map(
        |(assignment, bundle, assignment_fault, bundle_fault)| PublishedPair {
            assignment,
            bundle,
            assignment_fault,
            bundle_fault,
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn random_pair_failures_never_publish_partial_state_or_stall(
        history in prop::collection::vec(published_pair(), 1..16),
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(check_pair_history(history));
    }
}

async fn check_pair_history(history: Vec<PublishedPair>) {
    let f = fixture().await;
    let stranger = Keypair::generate_ed25519().public().to_peer_id();
    let mut expected_assignment = None;
    let mut expected_bundle = None;

    for (step, pair) in history.into_iter().enumerate() {
        let assignment_id = format!("a{}-{:?}", pair.assignment, pair.assignment_fault);
        let assigned_to = match pair.assignment_fault {
            PublishedFault::Refused => stranger,
            PublishedFault::Good | PublishedFault::Transient => f.peer_id,
        };
        let document_path = format!("/pbt-{step}.fb.gz");
        let document = f.stub.serve(
            &document_path,
            gzip(&worker_assignment(assigned_to)),
            usize::from(matches!(pair.assignment_fault, PublishedFault::Transient)),
        );

        let extra_schema = format!("{}.yaml", 100 + u16::from(pair.bundle));
        let archive = match pair.bundle_fault {
            PublishedFault::Refused => targz(&[(extra_schema.as_str(), SCHEMA.as_bytes())]),
            PublishedFault::Good | PublishedFault::Transient => targz(&[
                ("7.yaml", SCHEMA.as_bytes()),
                (extra_schema.as_str(), SCHEMA.as_bytes()),
            ]),
        };
        let bundle_hash = BundleHash::of(&archive);
        let bundle_path = format!("/pbt-{step}.tar.gz");
        let bundle = (
            bundle_hash,
            f.stub.serve(
                &bundle_path,
                archive,
                usize::from(matches!(pair.bundle_fault, PublishedFault::Transient)),
            ),
        );
        let update = update(&assignment_id, document, bundle);

        // Bundle and assignment fetches can each fail once, so three attempts are enough
        // for every transient pair. Each attempt is bounded: a generated history must never
        // wedge the applier on one half of the pair.
        let mut outcome = ApplyOutcome::Failed;
        for _ in 0..3 {
            outcome = tokio::time::timeout(Duration::from_secs(2), f.applier.apply(&update))
                .await
                .expect("pair application stalled");
            if outcome != ApplyOutcome::Failed {
                break;
            }
        }

        let applicable = !matches!(pair.assignment_fault, PublishedFault::Refused)
            && !matches!(pair.bundle_fault, PublishedFault::Refused);
        if applicable {
            assert_eq!(outcome, ApplyOutcome::Applied);
            expected_assignment = Some(assignment_id);
            expected_bundle = Some(bundle_hash);
        } else {
            assert_eq!(outcome, ApplyOutcome::Refused);
        }

        assert_eq!(
            f.worker.registered_assignment_id(),
            expected_assignment,
            "a refused or incomplete pair changed the active assignment at step {step}"
        );
        assert_eq!(
            f.schemas.registry().installed_hash(),
            expected_bundle,
            "a refused or incomplete pair changed the active bundle at step {step}"
        );
    }
}
