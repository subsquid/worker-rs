use std::{io::ErrorKind, time::Duration};

use async_stream::stream;
use futures::Stream;
use sqd_contract_client::PeerId;
use tokio::time::MissedTickBehavior;
use tower::retry::backoff::{Backoff, MakeBackoff};

use super::schema_bundle::{BundleHash, SchemaBundle};
use crate::cli::AssignmentSource;
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

/// What the stream last handed over: the pair, and where it said to fetch it from. Locations are
/// not identity, but a corrected one is the only thing that can rescue a fetch that keeps
/// failing, so a change of *where* is announced like a change of *what* — and the applier, which
/// is the only side that knows whether a fetch is still outstanding, decides what it means.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Announced {
    pair: NetworkPair,
    fb_url_v1: String,
    bundle_url: Option<String>,
}

pub struct AssignmentUpdate {
    pub id: String,
    pub fb_url_v1: String,
    pub _effective_from: u64,
    pub schema_bundle: Option<SchemaBundle>,
}

impl AssignmentUpdate {
    fn announced(&self) -> Announced {
        Announced {
            pair: self.pair(),
            fb_url_v1: self.fb_url_v1.clone(),
            bundle_url: self.schema_bundle.as_ref().map(|bundle| bundle.url.clone()),
        }
    }

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
pub fn new_assignments_stream(
    url: String,
    frequency: Duration,
    timeout: Duration,
    max_delay: Duration,
    peer_id: PeerId,
    assignment_source: AssignmentSource,
) -> impl Stream<Item = AssignmentUpdate> {
    let mut timer = tokio::time::interval(frequency);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let reqwest_client = new_reqwest_client(timeout, peer_id);
    let mut announced = Announced::default();
    let mut retry = backoff::exponential(Duration::from_secs(1), max_delay);

    stream! {
        loop {
            timer.tick().await;

            let mut backoff = retry.make_backoff();
            loop {
                match poll_network_state(&url, &reqwest_client, assignment_source, &mut announced).await {
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
    assignment_source: AssignmentSource,
    announced: &mut Announced,
) -> anyhow::Result<Option<AssignmentUpdate>> {
    tracing::debug!("Checking network state: {url}");
    let mut published = fetch_network_state(url, reqwest_client).await?;

    // From here on the state was read fine; what follows judges the scheduler's content, and a
    // state that is not applicable waits at the poll cadence rather than on the fetch-retry
    // ladder, whose backoff reaches hours between reads (WP-1, FM-53d). None of these exits
    // touches `announced`, so a corrected state is offered whole.
    let (pointer, expected) = published.take_pointer(assignment_source);
    let Some(pointer) = pointer else {
        tracing::warn!(
            expected,
            "Network state carries no assignment for this worker's mode; waiting"
        );
        return Ok(None);
    };
    let assignment: sqd_assignments::NetworkAssignment = match serde_json::from_value(pointer) {
        Ok(assignment) => assignment,
        Err(e) => {
            metrics::ASSIGNMENTS_REFUSED.inc();
            tracing::warn!(
                pointer = expected,
                error = %e,
                "Network state's assignment pointer will not decode; waiting"
            );
            return Ok(None);
        }
    };

    let published_bundle = match assignment_source {
        AssignmentSource::Legacy => None,
        // A half-published pair — the bundle missing, or its reference unusable — is FM-53d.
        AssignmentSource::Worker => match published.schema_bundle.take() {
            None => {
                metrics::SCHEMA_BUNDLE_MISMATCHES.inc();
                tracing::warn!(
                    assignment_id = %assignment.id,
                    "Network state publishes a worker assignment but no schema bundle; waiting"
                );
                return Ok(None);
            }
            Some(bundle) => match decode_bundle(bundle) {
                Ok(bundle) => Some(bundle),
                Err(e) => {
                    metrics::SCHEMA_BUNDLE_MISMATCHES.inc();
                    tracing::warn!(
                        assignment_id = %assignment.id,
                        error = %e,
                        "Network state publishes a worker assignment with an unusable schema bundle reference; waiting"
                    );
                    return Ok(None);
                }
            },
        },
    };

    Ok(published_update(&assignment, published_bundle, announced))
}

/// The pair as the network published it, or nothing: a pointer that names no document to
/// fetch is unusable in the same way as one that will not decode.
fn published_update(
    assignment: &sqd_assignments::NetworkAssignment,
    published_bundle: Option<SchemaBundle>,
    announced: &mut Announced,
) -> Option<AssignmentUpdate> {
    let Some(fb_url_v1) = assignment.fb_url_v1.clone() else {
        metrics::ASSIGNMENTS_REFUSED.inc();
        tracing::warn!(
            assignment_id = %assignment.id,
            "Network state's assignment pointer names no fb_url_v1; waiting"
        );
        return None;
    };
    let update = AssignmentUpdate {
        fb_url_v1,
        id: assignment.id.clone(),
        _effective_from: assignment.effective_from,
        schema_bundle: published_bundle,
    };
    let current = update.announced();
    if *announced == current {
        return None;
    }
    tracing::debug!("Discovered assignment \"{}\"", update.id);
    *announced = current;
    Some(update)
}

fn decode_bundle(bundle: serde_json::Value) -> anyhow::Result<SchemaBundle> {
    let bundle: sqd_assignments::SchemaBundle = serde_json::from_value(bundle)?;
    SchemaBundle::try_from(bundle)
}

/// The network state with each pointer left as published. A mode reads one pointer, so the
/// shape of the others cannot make the state unreadable, and the one it reads is judged on its
/// own (IB-40, IB-40b).
#[derive(Debug, serde::Deserialize)]
struct PublishedState {
    assignment: Option<serde_json::Value>,
    worker_assignment: Option<serde_json::Value>,
    schema_bundle: Option<serde_json::Value>,
}

impl PublishedState {
    /// The mode's own pointer and its name; the other pointer is never read.
    fn take_pointer(
        &mut self,
        assignment_source: AssignmentSource,
    ) -> (Option<serde_json::Value>, &'static str) {
        match assignment_source {
            AssignmentSource::Worker => (self.worker_assignment.take(), "worker_assignment"),
            AssignmentSource::Legacy => (self.assignment.take(), "assignment"),
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

/// Reads a fetched document in the mode's format. A worker document is verified and a failure
/// is a property of the bytes (FM-12); a legacy document is trusted (ADR-3), so reading it can
/// panic later and callers contain that where they read.
pub fn decode_document(
    assignment_source: AssignmentSource,
    document: Vec<u8>,
) -> anyhow::Result<AssignmentBlob> {
    match assignment_source {
        AssignmentSource::Legacy => Ok(AssignmentBlob::Legacy(
            sqd_assignments::Assignment::from_owned_unchecked(document),
        )),
        AssignmentSource::Worker => sqd_assignments::WorkerAssignment::from_owned(document)
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

    #[path = "assignments_pbt.rs"]
    mod pbt;

    #[allow(deprecated)]
    fn assignment(id: &str) -> sqd_assignments::NetworkAssignment {
        sqd_assignments::NetworkAssignment {
            url: None,
            fb_url: None,
            fb_url_v1: Some(format!("https://example.test/{id}.fb.gz")),
            id: id.to_string(),
            effective_from: 123,
        }
    }

    fn published_state(json: &str) -> PublishedState {
        serde_json::from_str(json).unwrap()
    }

    fn pointer_id(pointer: Option<serde_json::Value>) -> Option<String> {
        pointer.map(|pointer| pointer["id"].as_str().unwrap().to_owned())
    }

    const BOTH_POINTERS: &str = r#"{"network":"testnet","assignment":{"id":"legacy","fb_url_v1":"https://example.test/legacy.fb.gz","effective_from":123},"worker_assignment":{"id":"worker","fb_url_v1":"https://example.test/worker.fb.gz","effective_from":123}}"#;

    #[test]
    fn visible_assignment_uses_legacy_assignment_by_default() {
        let mut state = published_state(BOTH_POINTERS);

        assert_eq!(
            pointer_id(state.take_pointer(AssignmentSource::Legacy).0).as_deref(),
            Some("legacy")
        );
    }

    #[test]
    fn visible_assignment_uses_worker_assignment_when_enabled() {
        let mut state = published_state(BOTH_POINTERS);

        assert_eq!(
            pointer_id(state.take_pointer(AssignmentSource::Worker).0).as_deref(),
            Some("worker")
        );
    }

    #[test]
    fn visible_assignment_never_falls_back_to_the_other_pointer() {
        let mut legacy_only = published_state(
            r#"{"network":"testnet","assignment":{"id":"legacy","fb_url_v1":"https://example.test/legacy.fb.gz","effective_from":123}}"#,
        );
        assert!(legacy_only
            .take_pointer(AssignmentSource::Worker)
            .0
            .is_none());

        let mut worker_only = published_state(
            r#"{"network":"testnet","worker_assignment":{"id":"worker","fb_url_v1":"https://example.test/worker.fb.gz","effective_from":123}}"#,
        );
        assert!(worker_only
            .take_pointer(AssignmentSource::Legacy)
            .0
            .is_none());
    }

    /// The pointers a mode does not read may take any shape — the state is still readable, and
    /// the mode's own pointer is judged on its own (IB-40, IB-40b).
    #[test]
    fn a_pointer_the_mode_does_not_read_can_have_any_shape() {
        let mut state = published_state(
            r#"{"network":"testnet","assignment":{"id":"legacy","fb_url_v1":"https://example.test/legacy.fb.gz","effective_from":123},"worker_assignment":"not an object","schema_bundle":{"url":"no hash here"},"portal_assignment":42}"#,
        );

        assert_eq!(
            pointer_id(state.take_pointer(AssignmentSource::Legacy).0).as_deref(),
            Some("legacy")
        );
    }

    fn worker_state_json(id: &str, bundle_hash: BundleHash) -> Vec<u8> {
        worker_state_json_at(id, bundle_hash, &format!("http://example.com/{id}.fb.gz"))
    }

    fn worker_state_json_at(id: &str, bundle_hash: BundleHash, document_url: &str) -> Vec<u8> {
        format!(
            r#"{{"network":"test","worker_assignment":{{"id":"{id}","fb_url_v1":"{document_url}","effective_from":0}},"schema_bundle":{{"hash":"{bundle_hash}","url":"http://example.com/bundle.tar.gz"}}}}"#
        )
        .into_bytes()
    }

    fn hash(tag: u8) -> BundleHash {
        format!("sha256:{}", format!("{tag:02x}").repeat(32))
            .parse()
            .unwrap()
    }

    fn test_client() -> reqwest::Client {
        new_reqwest_client(Duration::from_secs(5), PeerId::random())
    }

    #[tokio::test]
    async fn either_half_moving_yields_an_update() {
        let a1b1 = worker_state_json("assignment-1", hash(0xaa));
        let a2b1 = worker_state_json("assignment-2", hash(0xaa));
        let a2b2 = worker_state_json("assignment-2", hash(0xbb));
        let url = TestServer::serve_sequence(vec![a1b1, a2b1.clone(), a2b1, a2b2]).await;
        let client = test_client();
        let source = AssignmentSource::Worker;
        let mut announced = Announced::default();

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(update.id, "assignment-1");
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xaa));

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(update.id, "assignment-2");
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xaa));

        assert!(
            poll_network_state(&url, &client, source, &mut announced)
                .await
                .unwrap()
                .is_none(),
            "neither half moved"
        );

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(update.id, "assignment-2");
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xbb));
    }

    /// A location is not identity, so a corrected url leaves the pair unchanged — and the applier
    /// holds whatever url it was announced with. If the poll stayed quiet about it, an expired or
    /// mistyped url would be retried forever with no way for a correction to reach the worker.
    /// Announcing it is safe because only the applier knows whether a fetch is still outstanding.
    #[tokio::test]
    async fn a_moved_location_is_announced_under_an_unchanged_pair() {
        let first = worker_state_json_at("a1", hash(0xaa), "http://example.com/first.fb.gz");
        let moved = worker_state_json_at("a1", hash(0xaa), "http://example.com/moved.fb.gz");
        let url = TestServer::serve_sequence(vec![first, moved.clone(), moved]).await;
        let client = test_client();
        let source = AssignmentSource::Worker;
        let mut announced = Announced::default();

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(update.fb_url_v1, "http://example.com/first.fb.gz");

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap()
            .expect("the pair is unchanged, but where to fetch it is not");
        assert_eq!(update.id, "a1", "the same pair, at a new location");
        assert_eq!(update.fb_url_v1, "http://example.com/moved.fb.gz");

        assert!(
            poll_network_state(&url, &client, source, &mut announced)
                .await
                .unwrap()
                .is_none(),
            "nothing moved this time"
        );
    }

    #[tokio::test]
    async fn a_pair_already_announced_is_not_offered_again() {
        let state = worker_state_json("assignment-1", hash(0xaa));
        let url = TestServer::serve_sequence(vec![state.clone(), state]).await;
        let client = test_client();
        let source = AssignmentSource::Worker;
        let mut announced = Announced::default();

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap();
        assert_eq!(update.unwrap().id, "assignment-1");

        assert!(
            poll_network_state(&url, &client, source, &mut announced)
                .await
                .unwrap()
                .is_none(),
            "whether the pair applied is the consumer's business, not the stream's: \
             another attempt is for it to ask for"
        );
    }

    #[tokio::test]
    async fn a_bundle_change_reoffers_the_assignment_as_a_pair() {
        let state = worker_state_json("assignment-1", hash(0xaa));
        let url = TestServer::serve_once(state).await;
        let mut announced = Announced {
            pair: NetworkPair {
                assignment_id: None,
                bundle_hash: Some(hash(0xaa)),
            },
            ..Default::default()
        };

        let update = poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Worker,
            &mut announced,
        )
        .await
        .unwrap();
        let update = update.unwrap();
        assert_eq!(update.id, "assignment-1");
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xaa));
    }

    /// Erroring here would take the poll off its tick and onto the fetch-retry ladder, which
    /// doubles to `assignment_fetch_max_delay` — hours of not noticing a scheduler that has
    /// already fixed the state. A half-published state is a legal condition of a rolling
    /// migration, so it is a non-event like any other unchanged poll.
    #[tokio::test]
    async fn a_state_missing_the_bundle_is_not_applicable_rather_than_an_error() {
        let state = br#"{"network":"test","worker_assignment":{"id":"assignment-1","fb_url_v1":"http://example.com/a.fb.gz","effective_from":0}}"#;
        let url = TestServer::serve_once(state.to_vec()).await;
        let mut announced = Announced {
            pair: NetworkPair {
                assignment_id: Some("assignment-1".to_owned()),
                bundle_hash: Some(hash(0xaa)),
            },
            ..Default::default()
        };
        let mismatches_before = metrics::SCHEMA_BUNDLE_MISMATCHES.get();

        let update = poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Worker,
            &mut announced,
        )
        .await
        .expect("a half-published state is not a failure to read one");

        assert!(update.is_none(), "half a pair is not applicable");
        // `>`, not `+ 1`: the counter is process-global and other tests in this binary move it.
        assert!(
            metrics::SCHEMA_BUNDLE_MISMATCHES.get() > mismatches_before,
            "the scheduler is who resolves it, so it counts with the other pair faults"
        );
        assert_eq!(
            announced.pair.bundle_hash,
            Some(hash(0xaa)),
            "nothing was announced, so the pair is offered whole when the bundle returns"
        );
    }

    /// The other way a pair can be half-published: the bundle is there but its reference is one
    /// the worker cannot use. Same fault class as a missing bundle — the state was read fine and
    /// it is the scheduler's content that is not applicable — so it takes the same exit: waiting
    /// at the poll cadence, not the fetch-retry ladder.
    #[tokio::test]
    async fn a_state_with_an_unusable_bundle_reference_is_not_applicable_rather_than_an_error() {
        // A bare hex digest: exactly the kind of near-miss a scheduler could publish by mistake.
        let state = format!(
            r#"{{"network":"test","worker_assignment":{{"id":"assignment-1","fb_url_v1":"http://example.com/a.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"{}","url":"http://example.com/bundle.tar.gz"}}}}"#,
            "aa".repeat(32)
        )
        .into_bytes();
        let url = TestServer::serve_once(state).await;
        let mut announced = Announced {
            pair: NetworkPair {
                assignment_id: Some("assignment-1".to_owned()),
                bundle_hash: Some(hash(0xaa)),
            },
            ..Default::default()
        };
        let mismatches_before = metrics::SCHEMA_BUNDLE_MISMATCHES.get();

        let update = poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Worker,
            &mut announced,
        )
        .await
        .expect("an unusable bundle reference is not a failure to read the state");

        assert!(update.is_none(), "the pair cannot be applied as published");
        assert!(
            metrics::SCHEMA_BUNDLE_MISMATCHES.get() > mismatches_before,
            "counted with the other pair faults, not as a bundle that failed to install"
        );
        assert_eq!(
            announced.pair.bundle_hash,
            Some(hash(0xaa)),
            "nothing was announced, so the corrected pair is offered whole"
        );
    }

    /// Pins the order of the checks: with no assignment for this mode there is nothing to pair
    /// the bundle with, so the state is the same wait as a network that has not migrated — and
    /// whether the bundle reference is usable must not be able to turn that wait into an error.
    #[tokio::test]
    async fn a_state_without_an_assignment_waits_whatever_its_bundle_says() {
        let state = format!(
            r#"{{"network":"test","assignment":{{"id":"legacy","fb_url_v1":"http://example.com/l.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"{}","url":"http://example.com/bundle.tar.gz"}}}}"#,
            "aa".repeat(32)
        )
        .into_bytes();
        let url = TestServer::serve_once(state).await;
        let mut announced = Announced::default();

        let update = poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Worker,
            &mut announced,
        )
        .await
        .expect("no assignment for the mode is a wait, not an error, however the bundle reads");

        assert!(update.is_none());
        assert_eq!(announced, Announced::default(), "nothing was announced");
    }

    /// The assignment half of the same rule, in the mode that has no bundle at all: a pointer
    /// that names no document to fetch is unusable content, not a failure to read the state.
    #[tokio::test]
    async fn a_pointer_without_a_fetch_url_waits_rather_than_erroring() {
        let state = br#"{"network":"test","assignment":{"id":"a1","effective_from":0}}"#;
        let url = TestServer::serve_once(state.to_vec()).await;
        let mut announced = Announced::default();
        let refused_before = metrics::ASSIGNMENTS_REFUSED.get();

        let update = poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Legacy,
            &mut announced,
        )
        .await
        .expect("a pointer with no fb_url_v1 is not a failure to read the state");

        assert!(update.is_none(), "there is nothing to fetch");
        assert!(
            metrics::ASSIGNMENTS_REFUSED.get() > refused_before,
            "an assignment the worker cannot use is what OB-18 counts"
        );
        assert_eq!(announced, Announced::default(), "nothing was announced");
    }

    #[tokio::test]
    async fn a_pointer_that_will_not_decode_waits_rather_than_erroring() {
        // An id that is not a string and no effective_from: the object is there, but it is not
        // an assignment pointer.
        let state = br#"{"network":"test","worker_assignment":{"id":7,"fb_url_v1":"http://example.com/a.fb.gz"},"schema_bundle":{"hash":"sha256:aa","url":"http://example.com/b.tar.gz"}}"#;
        let url = TestServer::serve_once(state.to_vec()).await;
        let mut announced = Announced::default();
        let refused_before = metrics::ASSIGNMENTS_REFUSED.get();

        let update = poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Worker,
            &mut announced,
        )
        .await
        .expect("a pointer that will not decode is not a failure to read the state");

        assert!(update.is_none());
        assert!(metrics::ASSIGNMENTS_REFUSED.get() > refused_before);
        assert_eq!(announced, Announced::default(), "nothing was announced");
    }

    /// A bundle object missing a field is as unusable a reference as a hash that will not parse,
    /// and must not be told apart from it by which parser happens to reject it.
    #[tokio::test]
    async fn a_bundle_object_that_will_not_decode_is_not_applicable_rather_than_an_error() {
        let state = br#"{"network":"test","worker_assignment":{"id":"assignment-1","fb_url_v1":"http://example.com/a.fb.gz","effective_from":0},"schema_bundle":{"url":"http://example.com/bundle.tar.gz"}}"#;
        let url = TestServer::serve_once(state.to_vec()).await;
        let mut announced = Announced {
            pair: NetworkPair {
                assignment_id: Some("assignment-1".to_owned()),
                bundle_hash: Some(hash(0xaa)),
            },
            ..Default::default()
        };
        let mismatches_before = metrics::SCHEMA_BUNDLE_MISMATCHES.get();

        let update = poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Worker,
            &mut announced,
        )
        .await
        .expect("a bundle object that will not decode is not a failure to read the state");

        assert!(update.is_none(), "the pair cannot be applied as published");
        assert!(metrics::SCHEMA_BUNDLE_MISMATCHES.get() > mismatches_before);
        assert_eq!(
            announced.pair.bundle_hash,
            Some(hash(0xaa)),
            "nothing was announced, so the corrected pair is offered whole"
        );
    }

    /// The boundary of the rule: a body that is not a JSON object is a state the worker cannot
    /// read, and that is what the fetch-retry ladder is for.
    #[tokio::test]
    async fn a_body_that_is_not_a_json_object_is_a_read_failure() {
        let url = TestServer::serve_once(b"[]".to_vec()).await;
        let mut announced = Announced::default();

        let outcome = poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Worker,
            &mut announced,
        )
        .await;

        assert!(outcome.is_err(), "not a network state at all");
        assert_eq!(announced, Announced::default());
    }

    #[tokio::test]
    async fn legacy_mode_ignores_the_schema_bundle() {
        // Not even a bundle object the worker could decode: legacy mode never reads it.
        let state = format!(
            r#"{{"network":"test","assignment":{{"id":"a1","fb_url_v1":"http://example.com/a1.fb.gz","effective_from":0}},"schema_bundle":{{"url":"http://example.com/b.tar.gz"}}}}"#
        )
        .into_bytes();
        let url = TestServer::serve_sequence(vec![state.clone(), state]).await;
        let client = test_client();
        let source = AssignmentSource::Legacy;
        let mut announced = Announced::default();

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap();
        let update = update.unwrap();
        assert_eq!(update.id, "a1");
        assert!(
            update.schema_bundle.is_none(),
            "legacy mode drops the bundle, malformed object and all"
        );

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap();
        assert!(update.is_none(), "the assignment alone is the whole pair");
    }
}
