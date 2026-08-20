use std::{io::ErrorKind, time::Duration};

use async_stream::stream;
use futures::Stream;
use sqd_assignments::{AssignmentType, NetworkState, ResolvedAssignments};
use sqd_contract_client::PeerId;
use tokio::time::MissedTickBehavior;
use tower::retry::backoff::{Backoff, MakeBackoff};

use super::schema_bundle::{BundleHash, SchemaBundle};
use crate::metrics;
use crate::storage::datasets_index::AssignmentBlob;
use crate::util::backoff;

/// Identifies an assignment and schema bundle announcement (ADR-21). Identity, not location: an
/// id names one document for all time (IB-40b).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NetworkPair {
    pub assignment_id: Option<String>,
    pub bundle_hash: Option<BundleHash>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentUpdate {
    pub id: String,
    /// Which of the state's assignments this came from, and so how its document reads. Resolved
    /// per poll, so it belongs to the announcement rather than to the process.
    pub assignment_type: AssignmentType,
    pub fb_url: String,
    pub schema_bundle: Option<SchemaBundle>,
}

impl AssignmentUpdate {
    pub fn pair(&self) -> NetworkPair {
        NetworkPair {
            assignment_id: Some(self.id.clone()),
            bundle_hash: self.schema_bundle.as_ref().map(|b| b.hash),
        }
    }
}

pub fn new_reqwest_client(timeout: Duration, peer_id: PeerId) -> reqwest::Client {
    let version = env!("CARGO_PKG_VERSION");
    reqwest::Client::builder()
        .user_agent(format!("SQD Worker/{version} {peer_id}"))
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// Yields each assignment-bundle pair once; the consumer owns retry policy.
///
/// `assignment_type` overrides the type the state names itself; unset, the state decides.
pub fn new_assignments_stream(
    url: String,
    frequency: Duration,
    timeout: Duration,
    max_delay: Duration,
    peer_id: PeerId,
    assignment_type: Option<AssignmentType>,
) -> impl Stream<Item = AssignmentUpdate> {
    let mut timer = tokio::time::interval(frequency);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let reqwest_client = new_reqwest_client(timeout, peer_id);
    let mut announced: Option<AssignmentUpdate> = None;
    let mut retry = backoff::exponential(Duration::from_secs(1), max_delay);

    stream! {
        loop {
            timer.tick().await;

            let mut backoff = retry.make_backoff();
            loop {
                match poll_network_state(&url, &reqwest_client, assignment_type, &mut announced).await {
                    Ok(Some(data)) => {
                        yield data;
                        break;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(error = %format!("{e:#}"), "Failed to update assignment; retrying");
                        backoff.next_backoff().await;
                    }
                }
            }
        }
    }
}

async fn poll_network_state(
    url: &str,
    reqwest_client: &reqwest::Client,
    assignment_type: Option<AssignmentType>,
    announced: &mut Option<AssignmentUpdate>,
) -> anyhow::Result<Option<AssignmentUpdate>> {
    tracing::debug!("Checking network state: {url}");
    let published = fetch_network_state(url, reqwest_client).await?;

    // From here on the state was read fine; what follows judges the scheduler's content, and a
    // state that is not applicable waits at the poll cadence rather than on the fetch-retry
    // ladder, whose backoff reaches hours between reads (WP-1, FM-53d). None of these exits
    // touches `announced`, so a corrected state is offered whole.
    let resolved = match published.resolve(assignment_type) {
        Ok(resolved) => resolved,
        Err(unresolved) => {
            unresolved.record();
            tracing::warn!(
                reason = %unresolved,
                "Network state names no assignment this worker can read; waiting"
            );
            return Ok(None);
        }
    };

    Ok(published_update(resolved, announced))
}

/// The pair as the network published it, or nothing: a pointer that names no document to fetch,
/// or a bundle reference the worker cannot use, is unusable in the same way as one that will
/// not decode.
///
/// `announced` is what the stream last handed over: the pair, and where it said to fetch it
/// from. Locations are not identity, but a corrected one is the only thing that can rescue a
/// fetch that keeps failing, so a change of *where* is announced like a change of *what* — and
/// the applier, which is the only side that knows whether a fetch is still outstanding, decides
/// what it means.
fn published_update(
    resolved: ResolvedAssignments,
    announced: &mut Option<AssignmentUpdate>,
) -> Option<AssignmentUpdate> {
    let update = match resolved {
        ResolvedAssignments::Legacy(assignment) => {
            let Some(fb_url) = assignment.fb_url_v1 else {
                metrics::ASSIGNMENTS_REFUSED.inc();
                tracing::warn!(
                    assignment_id = %assignment.id,
                    "Network state's assignment names no fb_url_v1; waiting"
                );
                return None;
            };
            AssignmentUpdate {
                id: assignment.id,
                assignment_type: AssignmentType::Legacy,
                fb_url,
                schema_bundle: None,
            }
        }
        // `worker.version` names the document's format, and is free-form until that format
        // settles; a document the worker cannot read fails verification as a malformed one
        // (FM-12) rather than being gated on a string whose spelling may yet change.
        ResolvedAssignments::Split {
            worker,
            schema_bundle,
            ..
        } => {
            let schema_bundle = match SchemaBundle::try_from(schema_bundle) {
                Ok(bundle) => bundle,
                Err(e) => {
                    metrics::SCHEMA_BUNDLE_MISMATCHES.inc();
                    tracing::warn!(
                        assignment_id = %worker.id,
                        error = %e,
                        "Network state's schema bundle reference is unusable; waiting"
                    );
                    return None;
                }
            };
            AssignmentUpdate {
                id: worker.id,
                assignment_type: AssignmentType::Split,
                fb_url: worker.fb_url,
                schema_bundle: Some(schema_bundle),
            }
        }
    };
    if announced.as_ref() == Some(&update) {
        return None;
    }
    tracing::debug!("Discovered assignment \"{}\"", update.id);
    *announced = Some(update.clone());
    Some(update)
}

/// The published state with every blob still JSON, decoded one at a time so that the shape of
/// one the resolved type does not name cannot make the state unreadable (IB-40) —
/// [`NetworkState`] refuses the whole state over a `portal_assignment` this worker never reads.
#[derive(Debug, serde::Deserialize)]
struct PublishedState {
    /// Unread; carried so `resolve` sees what was published.
    #[serde(default)]
    network: String,
    assignment_type: Option<serde_json::Value>,
    assignment: Option<serde_json::Value>,
    worker_assignment: Option<serde_json::Value>,
    portal_assignment: Option<serde_json::Value>,
    schema_bundle: Option<serde_json::Value>,
}

impl PublishedState {
    /// The assignments the state names, `assignment_type` overriding the type it names itself.
    fn resolve(
        mut self,
        assignment_type: Option<AssignmentType>,
    ) -> Result<ResolvedAssignments, Unresolved> {
        let own = match self.assignment_type.take() {
            // Absent means the state predates the split, exactly as `NetworkState` reads it.
            None => AssignmentType::default(),
            Some(raw) => match serde_json::from_value(raw) {
                Ok(own) => own,
                // The type is what picks, so one this worker cannot read leaves it nothing to
                // pick with — unless a pinned type has already made the state's own moot.
                Err(_) if assignment_type.is_none() => return Err(Unresolved::AssignmentType),
                Err(_) => AssignmentType::default(),
            },
        };
        let published = [
            ("assignment", self.assignment.is_some()),
            ("worker_assignment", self.worker_assignment.is_some()),
            ("portal_assignment", self.portal_assignment.is_some()),
            ("schema_bundle", self.schema_bundle.is_some()),
        ];
        NetworkState {
            network: self.network,
            assignment_type: own,
            assignment: decode(self.assignment),
            worker_assignment: decode(self.worker_assignment),
            portal_assignment: decode(self.portal_assignment),
            schema_bundle: decode(self.schema_bundle),
        }
        .resolve(assignment_type)
        // `resolve` names the blob it wanted; the state having published that key means it
        // would not decode, which is the scheduler's to correct rather than a network migrating.
        .map_err(|error| {
            if published.contains(&(error.missing, true)) {
                Unresolved::Malformed(error)
            } else {
                Unresolved::Absent(error)
            }
        })
    }
}

/// One published blob, dropped if it will not decode; `resolve` decides what the loss costs.
fn decode<T: serde::de::DeserializeOwned>(blob: Option<serde_json::Value>) -> Option<T> {
    serde_json::from_value(blob?).ok()
}

/// Why a state that read fine names nothing this worker can apply.
#[derive(Debug, thiserror::Error)]
enum Unresolved {
    /// The state's own type, with nothing pinned to stand in for it.
    #[error("assignment_type will not decode")]
    AssignmentType,
    #[error("{0}")]
    Absent(sqd_assignments::InvalidNetworkState),
    #[error("assignment_type is \"{}\" but {} will not decode", .0.assignment_type, .0.missing)]
    Malformed(sqd_assignments::InvalidNetworkState),
}

impl Unresolved {
    /// What the state failed to name, as a bounded label (OB-14).
    fn reason(&self) -> &'static str {
        match self {
            Self::AssignmentType => "assignment_type",
            Self::Absent(error) | Self::Malformed(error) => error.missing,
        }
    }

    /// Every unresolved poll is counted by reason (OB-19): absence is legal mid-migration, so
    /// what marks a stalled fleet is persistence, not a single edge — and a scheduler that
    /// declares `split` and never publishes the portal half moves nothing else at all.
    ///
    /// On top of that, the bundle the resolved type names is FM-53d either way, and this
    /// worker's own document, published but undecodable, is a refused assignment (FM-12,
    /// OB-18).
    fn record(&self) {
        metrics::network_state_unresolved(self.reason());
        match self {
            Self::Absent(e) | Self::Malformed(e) if e.missing == "schema_bundle" => {
                metrics::SCHEMA_BUNDLE_MISMATCHES.inc();
            }
            Self::Malformed(e) if matches!(e.missing, "assignment" | "worker_assignment") => {
                metrics::ASSIGNMENTS_REFUSED.inc();
            }
            _ => {}
        }
    }
}

/// Fails only when the state cannot be read at all — transport, or a body that is not a JSON
/// object; that is the poll failure WP-1's ladder is for.
async fn fetch_network_state(
    url: &str,
    reqwest_client: &reqwest::Client,
) -> anyhow::Result<PublishedState> {
    let response = reqwest_client.get(url).send().await?.error_for_status()?;
    let published = response.json().await?;
    Ok(published)
}

/// The assignment document's bytes, gunzipped. Fails on transport and gzip errors only: whether
/// the bytes are a document is a verdict on the document, decided in [`decode_document`].
pub async fn fetch_document(
    url: &str,
    reqwest_client: &reqwest::Client,
) -> anyhow::Result<Vec<u8>> {
    download_gzipped(url, reqwest_client).await
}

/// Reads a fetched document in the announcement's format. A split document is verified and a
/// failure is a property of the bytes (FM-12); a legacy document is trusted (ADR-3), so reading
/// it can panic later and callers contain that where they read.
pub fn decode_document(
    assignment_type: AssignmentType,
    document: Vec<u8>,
) -> anyhow::Result<AssignmentBlob> {
    match assignment_type {
        AssignmentType::Legacy => Ok(AssignmentBlob::Legacy(
            sqd_assignments::Assignment::from_owned_unchecked(document),
        )),
        AssignmentType::Split => sqd_assignments::WorkerAssignment::from_owned(document)
            .map(AssignmentBlob::Worker)
            .map_err(|e| anyhow::anyhow!("malformed worker assignment: {e}")),
    }
}

async fn download_gzipped(url: &str, reqwest_client: &reqwest::Client) -> anyhow::Result<Vec<u8>> {
    use async_compression::tokio::bufread::GzipDecoder;
    use futures::TryStreamExt;
    use tokio::io::AsyncReadExt;
    use tokio_util::io::StreamReader;

    let response = reqwest_client.get(url).send().await?.error_for_status()?;
    let stream = response.bytes_stream();
    let reader = StreamReader::new(stream.map_err(|e| std::io::Error::new(ErrorKind::Other, e)));
    let mut buf = Vec::new();
    let mut decoder = GzipDecoder::new(reader);
    decoder
        .read_to_end(&mut buf)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to download assignment: {}", e))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::test_support::TestServer;

    fn published_state(json: &str) -> PublishedState {
        serde_json::from_str(json).unwrap()
    }

    const LEGACY: &str = r#""assignment":{"id":"legacy","fb_url_v1":"https://example.test/legacy.fb.gz","effective_from":123}"#;
    const WORKER: &str = r#""worker_assignment":{"id":"worker","fb_url":"https://example.test/worker.fb.gz","version":"1"}"#;
    const PORTAL: &str = r#""portal_assignment":{"id":"portal","fb_url":"https://example.test/portal.fb.gz","version":"1"}"#;
    const BUNDLE: &str = r#""schema_bundle":{"hash":"sha256:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899","url":"https://example.test/bundle.tar.gz"}"#;

    fn state(assignment_type: &str, blobs: &[&str]) -> String {
        format!(
            r#"{{"network":"test","assignment_type":"{assignment_type}",{}}}"#,
            blobs.join(",")
        )
    }

    /// The id `resolve` picked, or the reason it picked nothing.
    fn resolved(state: &str, assignment_type: Option<AssignmentType>) -> Result<String, String> {
        match published_state(state).resolve(assignment_type) {
            Ok(ResolvedAssignments::Legacy(assignment)) => Ok(assignment.id),
            Ok(ResolvedAssignments::Split { worker, .. }) => Ok(worker.id),
            Err(unresolved) => Err(unresolved.to_string()),
        }
    }

    /// The type picks, a pin beats it, and blobs the picked type does not name may take any
    /// shape at all (IB-40) — including one `NetworkState` would refuse.
    #[test]
    fn the_resolved_type_picks_the_assignment_and_ignores_the_rest() {
        #[track_caller]
        fn check(case: &str, state: &str, pin: Option<AssignmentType>, want: Result<&str, &str>) {
            let picked = resolved(state, pin);
            assert_eq!(
                picked.as_deref().map_err(String::as_str),
                want,
                "{case}: {state}"
            );
        }

        let both = state("legacy", &[LEGACY, WORKER, PORTAL, BUNDLE]);
        let both_split = state("split", &[LEGACY, WORKER, PORTAL, BUNDLE]);
        let odd_shapes_beside_legacy = state(
            "legacy",
            &[
                LEGACY,
                r#""worker_assignment":"not an object""#,
                r#""portal_assignment":42"#,
                r#""schema_bundle":{"url":"no hash here"}"#,
            ],
        );
        let odd_shape_beside_split = state(
            "split",
            &[r#""assignment":"not an object""#, WORKER, PORTAL, BUNDLE],
        );
        let untyped = format!(r#"{{"network":"test",{LEGACY},{WORKER},{PORTAL},{BUNDLE}}}"#);
        let bad_type = format!(r#"{{"network":"test","assignment_type":"combined",{LEGACY}}}"#);

        check("the state's own type picks", &both, None, Ok("legacy"));
        check(
            "a pin beats it",
            &both,
            Some(AssignmentType::Split),
            Ok("worker"),
        );
        check("either way round", &both_split, None, Ok("worker"));
        check(
            "either way round",
            &both_split,
            Some(AssignmentType::Legacy),
            Ok("legacy"),
        );
        check(
            "a pin still needs its blobs",
            &state("legacy", &[LEGACY]),
            Some(AssignmentType::Split),
            Err(r#"assignment_type is "split" but worker_assignment is not published"#),
        );
        check(
            "in both directions",
            &state("split", &[WORKER, PORTAL, BUNDLE]),
            Some(AssignmentType::Legacy),
            Err(r#"assignment_type is "legacy" but assignment is not published"#),
        );
        check(
            "the blobs legacy does not name are not read",
            &odd_shapes_beside_legacy,
            None,
            Ok("legacy"),
        );
        check(
            "nor the one split does not name",
            &odd_shape_beside_split,
            None,
            Ok("worker"),
        );
        check(
            "an absent type predates the split",
            &untyped,
            None,
            Ok("legacy"),
        );
        check(
            "a type this worker cannot read leaves nothing to pick with",
            &bad_type,
            None,
            Err("assignment_type will not decode"),
        );
        check(
            "unless a pin has made it moot",
            &bad_type,
            Some(AssignmentType::Legacy),
            Ok("legacy"),
        );
    }

    fn split_state_json(id: &str, bundle_hash: BundleHash) -> Vec<u8> {
        split_state_json_at(id, bundle_hash, &format!("http://example.com/{id}.fb.gz"))
    }

    fn split_state_json_at(id: &str, bundle_hash: BundleHash, document_url: &str) -> Vec<u8> {
        state(
            "split",
            &[
                &format!(r#""worker_assignment":{{"id":"{id}","fb_url":"{document_url}","version":"1"}}"#),
                PORTAL,
                &format!(
                    r#""schema_bundle":{{"hash":"{bundle_hash}","url":"http://example.com/bundle.tar.gz"}}"#
                ),
            ],
        )
        .into_bytes()
    }

    fn hash(tag: u8) -> BundleHash {
        format!("sha256:{}", format!("{tag:02x}").repeat(32))
            .parse()
            .unwrap()
    }

    /// The update the stream would have remembered from a poll of [`split_state_json`].
    fn announced_update(id: &str, bundle_hash: BundleHash) -> AssignmentUpdate {
        AssignmentUpdate {
            id: id.to_owned(),
            assignment_type: AssignmentType::Split,
            fb_url: format!("http://example.com/{id}.fb.gz"),
            schema_bundle: Some(SchemaBundle {
                hash: bundle_hash,
                url: "http://example.com/bundle.tar.gz".to_owned(),
            }),
        }
    }

    fn test_client() -> reqwest::Client {
        new_reqwest_client(Duration::from_secs(5), PeerId::random())
    }

    /// Either half moving re-announces the pair, and so does a change of location under an
    /// unchanged pair — only the applier knows whether a fetch is still outstanding for it. An
    /// unchanged poll announces nothing.
    #[tokio::test]
    async fn poll_announces_each_change_of_pair_or_location_once() {
        async fn poll(
            url: &str,
            client: &reqwest::Client,
            announced: &mut Option<AssignmentUpdate>,
        ) -> Option<AssignmentUpdate> {
            poll_network_state(url, client, None, announced)
                .await
                .unwrap()
        }

        let a1b1 = split_state_json("assignment-1", hash(0xaa));
        let a2b1 = split_state_json("assignment-2", hash(0xaa));
        let a2b2 = split_state_json("assignment-2", hash(0xbb));
        let a2b2_moved =
            split_state_json_at("assignment-2", hash(0xbb), "http://example.com/moved.fb.gz");
        let url = TestServer::serve_sequence(vec![
            a1b1,
            a2b1.clone(),
            a2b1,
            a2b2,
            a2b2_moved.clone(),
            a2b2_moved,
        ])
        .await;
        let client = test_client();
        let mut announced = None;

        let update = poll(&url, &client, &mut announced).await.unwrap();
        assert_eq!(update.id, "assignment-1");
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xaa));

        let update = poll(&url, &client, &mut announced).await.unwrap();
        assert_eq!(update.id, "assignment-2", "the assignment moved");
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xaa));

        assert!(
            poll(&url, &client, &mut announced).await.is_none(),
            "neither half moved"
        );

        let update = poll(&url, &client, &mut announced).await.unwrap();
        assert_eq!(
            update.id, "assignment-2",
            "a bundle change re-offers its assignment"
        );
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xbb));

        let update = poll(&url, &client, &mut announced).await.unwrap();
        assert_eq!(
            update.fb_url, "http://example.com/moved.fb.gz",
            "the same pair, moved"
        );

        assert!(
            poll(&url, &client, &mut announced).await.is_none(),
            "nothing moved this time"
        );
    }

    /// A state that reads but is not applicable waits at the poll cadence rather than erroring
    /// onto the fetch-retry ladder, counts the fault if it is the scheduler's to resolve, and
    /// leaves `announced` untouched so a corrected pair is offered whole. Only a body that is not a
    /// JSON object is a failure to read the state.
    #[tokio::test]
    async fn a_state_that_is_not_applicable_waits_without_touching_announced() {
        #[derive(Clone, Copy)]
        enum Counted {
            Nothing,
            Mismatch,
            Refused,
        }
        let remembered = || Some(announced_update("assignment-1", hash(0xaa)));
        let worker = r#""worker_assignment":{"id":"assignment-1","fb_url":"http://example.com/a.fb.gz","version":"1"}"#;
        let bare_hex = format!(
            r#""schema_bundle":{{"hash":"{}","url":"http://example.com/bundle.tar.gz"}}"#,
            "aa".repeat(32)
        );
        let no_hash = r#""schema_bundle":{"url":"http://example.com/bundle.tar.gz"}"#;
        let cases: Vec<(&str, String, Option<AssignmentUpdate>, Counted)> = vec![
            (
                "split assignment without its bundle",
                state("split", &[worker, PORTAL]),
                remembered(),
                Counted::Mismatch,
            ),
            (
                "bundle reference that is not sha256:<hex>",
                state("split", &[worker, PORTAL, &bare_hex]),
                remembered(),
                Counted::Mismatch,
            ),
            (
                "bundle object that will not decode",
                state("split", &[worker, PORTAL, no_hash]),
                remembered(),
                Counted::Mismatch,
            ),
            (
                "split state whose portal half has yet to be published",
                state("split", &[worker, BUNDLE]),
                remembered(),
                Counted::Nothing,
            ),
            (
                "no assignment for the resolved type, whatever the rest says",
                state("legacy", &[worker, PORTAL, BUNDLE]),
                None,
                Counted::Nothing,
            ),
            (
                "assignment_type that will not decode, with nothing pinned",
                format!(r#"{{"network":"test","assignment_type":"combined",{LEGACY}}}"#),
                None,
                Counted::Nothing,
            ),
            (
                "legacy pointer without a fetch url",
                state(
                    "legacy",
                    &[r#""assignment":{"id":"a1","effective_from":0}"#],
                ),
                None,
                Counted::Refused,
            ),
            (
                "worker pointer that will not decode",
                state(
                    "split",
                    &[r#""worker_assignment":{"id":7}"#, PORTAL, BUNDLE],
                ),
                None,
                Counted::Refused,
            ),
        ];

        for (case, published, before, counted) in cases {
            let url = TestServer::serve_once(published.into_bytes()).await;
            let mut announced = before.clone();
            let mismatches = metrics::SCHEMA_BUNDLE_MISMATCHES.get();
            let refused = metrics::ASSIGNMENTS_REFUSED.get();

            let update = poll_network_state(&url, &test_client(), None, &mut announced)
                .await
                .unwrap_or_else(|e| panic!("{case}: not a read failure: {e:#}"));

            assert!(update.is_none(), "{case}");
            // `>`, not `+ 1`: the counters are process-global.
            match counted {
                Counted::Mismatch => {
                    assert!(
                        metrics::SCHEMA_BUNDLE_MISMATCHES.get() > mismatches,
                        "{case}"
                    )
                }
                Counted::Refused => assert!(metrics::ASSIGNMENTS_REFUSED.get() > refused, "{case}"),
                Counted::Nothing => {}
            }
            assert_eq!(announced, before, "{case}: nothing was announced");
        }

        let url = TestServer::serve_once(b"[]".to_vec()).await;
        let mut announced = None;
        let outcome = poll_network_state(&url, &test_client(), None, &mut announced).await;
        assert!(outcome.is_err(), "not a network state at all");
        assert_eq!(announced, None);
    }

    /// The stalls nothing else counts are still visible by reason (OB-19), so a fleet stuck on
    /// a state that names assignments it does not publish can be alerted on by persistence.
    #[tokio::test]
    async fn every_unresolved_state_is_counted_by_its_reason() {
        let cases = [
            (
                "the portal half a split state must publish but the worker never reads",
                state("split", &[WORKER, BUNDLE]),
                "portal_assignment",
            ),
            (
                "no assignment for the resolved type",
                state("legacy", &[WORKER, PORTAL, BUNDLE]),
                "assignment",
            ),
            (
                "a picker this worker cannot read",
                format!(r#"{{"network":"test","assignment_type":"combined",{LEGACY}}}"#),
                "assignment_type",
            ),
        ];

        for (case, published, reason) in cases {
            let url = TestServer::serve_once(published.into_bytes()).await;
            // `>`, not `+ 1`: the counters are process-global.
            let before = metrics::unresolved_count(reason);
            let mut announced = None;

            let update = poll_network_state(&url, &test_client(), None, &mut announced)
                .await
                .expect("a state that reads is not a poll failure");

            assert!(update.is_none(), "{case}");
            assert!(metrics::unresolved_count(reason) > before, "{case}");
        }
    }

    #[tokio::test]
    async fn legacy_ignores_the_schema_bundle() {
        // Not even a bundle object the worker could decode: a legacy state never reads it.
        let published = state(
            "legacy",
            &[
                r#""assignment":{"id":"a1","fb_url_v1":"http://example.com/a1.fb.gz","effective_from":0}"#,
                r#""schema_bundle":{"url":"http://example.com/b.tar.gz"}"#,
            ],
        )
        .into_bytes();
        let url = TestServer::serve_sequence(vec![published.clone(), published]).await;
        let client = test_client();
        let mut announced: Option<AssignmentUpdate> = None;

        let update = poll_network_state(&url, &client, None, &mut announced)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(update.id, "a1");
        assert_eq!(update.assignment_type, AssignmentType::Legacy);
        assert!(
            update.schema_bundle.is_none(),
            "a legacy state drops the bundle, malformed object and all"
        );

        let update = poll_network_state(&url, &client, None, &mut announced)
            .await
            .unwrap();
        assert!(update.is_none(), "the assignment alone is the whole pair");
    }
}
