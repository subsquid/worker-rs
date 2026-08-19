use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn every_changed_published_pair_is_emitted_whole(
        pairs in prop::collection::vec((any::<u8>(), any::<u8>()), 1..100),
    ) {
        let mut announced = None;
        let mut shadow = NetworkPair::default();

        for (assignment_tag, bundle_tag) in pairs {
            let assignment = assignment(&format!("assignment-{assignment_tag}"));
            let bundle = SchemaBundle {
                hash: hash(bundle_tag),
                url: format!("https://example.test/bundle-{bundle_tag}.tar.gz"),
            };
            let current = NetworkPair {
                assignment_id: Some(assignment.id.clone()),
                bundle_hash: Some(bundle.hash),
            };

            let update = published_update(&assignment, Some(bundle), &mut announced);
            if current == shadow {
                prop_assert!(update.is_none(), "an unchanged pair was emitted again");
            } else {
                let update = update.expect("a changed pair must be emitted");
                prop_assert_eq!(update.id, assignment.id);
                prop_assert_eq!(
                    update.schema_bundle.map(|bundle| bundle.hash),
                    current.bundle_hash,
                    "a bundle change must carry its assignment, not a detached bundle event"
                );
                shadow = current;
            }
            let remembered = announced
                .as_ref()
                .map(AssignmentUpdate::pair)
                .unwrap_or_default();
            prop_assert_eq!(&remembered, &shadow);
        }
    }
}
