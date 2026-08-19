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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentUpdate {
    pub id: String,
    pub fb_url_v1: String,
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
    let mut announced: Option<AssignmentUpdate> = None;
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
    announced: &mut Option<AssignmentUpdate>,
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
///
/// `announced` is what the stream last handed over: the pair, and where it said to fetch it
/// from. Locations are not identity, but a corrected one is the only thing that can rescue a
/// fetch that keeps failing, so a change of *where* is announced like a change of *what* — and
/// the applier, which is the only side that knows whether a fetch is still outstanding, decides
/// what it means.
fn published_update(
    assignment: &sqd_assignments::NetworkAssignment,
    published_bundle: Option<SchemaBundle>,
    announced: &mut Option<AssignmentUpdate>,
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
        schema_bundle: published_bundle,
    };
    if announced.as_ref() == Some(&update) {
        return None;
    }
    tracing::debug!("Discovered assignment \"{}\"", update.id);
    *announced = Some(update.clone());
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

    fn published_state(json: &str) -> PublishedState {
        serde_json::from_str(json).unwrap()
    }

    fn pointer_id(pointer: Option<serde_json::Value>) -> Option<String> {
        pointer.map(|pointer| pointer["id"].as_str().unwrap().to_owned())
    }

    const BOTH_POINTERS: &str = r#"{"network":"testnet","assignment":{"id":"legacy","fb_url_v1":"https://example.test/legacy.fb.gz","effective_from":123},"worker_assignment":{"id":"worker","fb_url_v1":"https://example.test/worker.fb.gz","effective_from":123}}"#;
    const LEGACY_ONLY: &str = r#"{"network":"testnet","assignment":{"id":"legacy","fb_url_v1":"https://example.test/legacy.fb.gz","effective_from":123}}"#;
    const WORKER_ONLY: &str = r#"{"network":"testnet","worker_assignment":{"id":"worker","fb_url_v1":"https://example.test/worker.fb.gz","effective_from":123}}"#;
    /// The pointers a mode does not read may take any shape — the state is still readable, and
    /// the mode's own pointer is judged on its own (IB-40, IB-40b).
    const ODD_SHAPES_BESIDE_LEGACY: &str = r#"{"network":"testnet","assignment":{"id":"legacy","fb_url_v1":"https://example.test/legacy.fb.gz","effective_from":123},"worker_assignment":"not an object","schema_bundle":{"url":"no hash here"},"portal_assignment":42}"#;

    #[test]
    fn take_pointer_reads_only_the_modes_own_pointer() {
        let cases = [
            (BOTH_POINTERS, AssignmentSource::Legacy, Some("legacy")),
            (BOTH_POINTERS, AssignmentSource::Worker, Some("worker")),
            (LEGACY_ONLY, AssignmentSource::Worker, None),
            (WORKER_ONLY, AssignmentSource::Legacy, None),
            (
                ODD_SHAPES_BESIDE_LEGACY,
                AssignmentSource::Legacy,
                Some("legacy"),
            ),
        ];
        for (state, mode, expected) in cases {
            let mut state = published_state(state);
            assert_eq!(
                pointer_id(state.take_pointer(mode).0).as_deref(),
                expected,
                "{mode:?} reading {state:?}"
            );
        }
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

    /// The update the stream would have remembered from a poll of [`worker_state_json`].
    fn announced_update(id: &str, bundle_hash: BundleHash) -> AssignmentUpdate {
        AssignmentUpdate {
            id: id.to_owned(),
            fb_url_v1: format!("http://example.com/{id}.fb.gz"),
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
    /// unchanged pair: a location is not identity, but a corrected url is the only thing that can
    /// rescue a fetch that keeps failing, and only the applier knows whether one is outstanding.
    /// An unchanged poll announces nothing — whether the pair applied is the consumer's business.
    #[tokio::test]
    async fn poll_announces_each_change_of_pair_or_location_once() {
        let a1b1 = worker_state_json("assignment-1", hash(0xaa));
        let a2b1 = worker_state_json("assignment-2", hash(0xaa));
        let a2b2 = worker_state_json("assignment-2", hash(0xbb));
        let a2b2_moved =
            worker_state_json_at("assignment-2", hash(0xbb), "http://example.com/moved.fb.gz");
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
        let source = AssignmentSource::Worker;
        let mut announced: Option<AssignmentUpdate> = None;

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap()
            .expect("the first pair");
        assert_eq!(update.id, "assignment-1");
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xaa));

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap()
            .expect("the assignment moved");
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
            .expect("the bundle moved");
        assert_eq!(
            update.id, "assignment-2",
            "a bundle change re-offers its assignment"
        );
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xbb));

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap()
            .expect("the pair is unchanged, but where to fetch it is not");
        assert_eq!(update.id, "assignment-2");
        assert_eq!(update.fb_url_v1, "http://example.com/moved.fb.gz");

        assert!(
            poll_network_state(&url, &client, source, &mut announced)
                .await
                .unwrap()
                .is_none(),
            "nothing moved this time"
        );
    }

    #[derive(Clone, Copy)]
    enum Counted {
        Nothing,
        Mismatch,
        Refused,
    }

    /// A state that reads but is not applicable — half a pair, a pointer or bundle reference the
    /// worker cannot use, no assignment for the mode — is a non-event: it waits at the poll
    /// cadence rather than erroring onto the fetch-retry ladder (which doubles to
    /// `assignment_fetch_max_delay` — hours of not noticing a scheduler that has already fixed
    /// the state), counts the fault if it is the scheduler's to resolve, and leaves `announced`
    /// untouched so a corrected pair is offered whole. Only a body that is not a JSON object is a
    /// failure to read the state. Check order is pinned by the third row: no assignment for the
    /// mode is a wait whatever the bundle says.
    #[tokio::test]
    async fn a_state_that_is_not_applicable_waits_without_touching_announced() {
        let remembered = || Some(announced_update("assignment-1", hash(0xaa)));
        let bare_hex = "aa".repeat(32);
        let cases: Vec<(&str, Vec<u8>, AssignmentSource, Option<AssignmentUpdate>, Counted)> = vec![
            (
                "worker assignment without its bundle",
                br#"{"network":"test","worker_assignment":{"id":"assignment-1","fb_url_v1":"http://example.com/a.fb.gz","effective_from":0}}"#.to_vec(),
                AssignmentSource::Worker,
                remembered(),
                Counted::Mismatch,
            ),
            (
                "bundle reference that is not sha256:<hex>",
                format!(r#"{{"network":"test","worker_assignment":{{"id":"assignment-1","fb_url_v1":"http://example.com/a.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"{bare_hex}","url":"http://example.com/bundle.tar.gz"}}}}"#).into_bytes(),
                AssignmentSource::Worker,
                remembered(),
                Counted::Mismatch,
            ),
            (
                "no assignment for the mode, whatever the bundle says",
                format!(r#"{{"network":"test","assignment":{{"id":"legacy","fb_url_v1":"http://example.com/l.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"{bare_hex}","url":"http://example.com/bundle.tar.gz"}}}}"#).into_bytes(),
                AssignmentSource::Worker,
                None,
                Counted::Nothing,
            ),
            (
                "pointer without a fetch url",
                br#"{"network":"test","assignment":{"id":"a1","effective_from":0}}"#.to_vec(),
                AssignmentSource::Legacy,
                None,
                Counted::Refused,
            ),
            (
                "pointer that will not decode",
                br#"{"network":"test","worker_assignment":{"id":7,"fb_url_v1":"http://example.com/a.fb.gz"},"schema_bundle":{"hash":"sha256:aa","url":"http://example.com/b.tar.gz"}}"#.to_vec(),
                AssignmentSource::Worker,
                None,
                Counted::Refused,
            ),
            (
                "bundle object that will not decode",
                br#"{"network":"test","worker_assignment":{"id":"assignment-1","fb_url_v1":"http://example.com/a.fb.gz","effective_from":0},"schema_bundle":{"url":"http://example.com/bundle.tar.gz"}}"#.to_vec(),
                AssignmentSource::Worker,
                remembered(),
                Counted::Mismatch,
            ),
        ];

        for (case, state, mode, before, counted) in cases {
            let url = TestServer::serve_once(state).await;
            let mut announced = before.clone();
            let mismatches = metrics::SCHEMA_BUNDLE_MISMATCHES.get();
            let refused = metrics::ASSIGNMENTS_REFUSED.get();

            let update = poll_network_state(&url, &test_client(), mode, &mut announced)
                .await
                .unwrap_or_else(|e| panic!("{case}: not applicable is not a read failure: {e:#}"));

            assert!(update.is_none(), "{case}: nothing to apply");
            // `>`, not `+ 1`: the counters are process-global and other tests in this binary move them.
            match counted {
                Counted::Mismatch => assert!(
                    metrics::SCHEMA_BUNDLE_MISMATCHES.get() > mismatches,
                    "{case}: the scheduler is who resolves it, so it counts with the other pair faults"
                ),
                Counted::Refused => assert!(
                    metrics::ASSIGNMENTS_REFUSED.get() > refused,
                    "{case}: an assignment the worker cannot use is what OB-18 counts"
                ),
                Counted::Nothing => {}
            }
            assert_eq!(announced, before, "{case}: nothing was announced");
        }

        let url = TestServer::serve_once(b"[]".to_vec()).await;
        let mut announced: Option<AssignmentUpdate> = None;
        let outcome = poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Worker,
            &mut announced,
        )
        .await;
        assert!(
            outcome.is_err(),
            "a body that is not a JSON object is not a network state at all"
        );
        assert_eq!(announced, None);
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
        let mut announced: Option<AssignmentUpdate> = None;

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
