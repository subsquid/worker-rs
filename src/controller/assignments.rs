use std::{io::ErrorKind, time::Duration};

use async_stream::stream;
use futures::Stream;
use rand::Rng;
use sqd_contract_client::PeerId;
use tokio::time::MissedTickBehavior;

use super::schema_bundle::{BundleHash, SchemaBundle};
use crate::cli::AssignmentSource;
use crate::metrics;

/// What the worker has in force: the pair, since an assignment and its bundle are one state
/// (ADR-21).
///
/// Both halves are what the worker *did*, never what it saw. A "last seen" marker cannot tell an
/// assignment that applied from one that was refused, so it consumes the refused one for good.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AppliedPair {
    /// Registered, which is earlier than fully applied: chunks may still be downloading. That
    /// is the point — an assignment mid-download must not be offered as though it were new.
    pub assignment_id: Option<String>,
    pub bundle_hash: Option<BundleHash>,
}

pub struct AssignmentUpdate {
    pub id: String,
    pub fb_url_v1: String,
    pub _effective_from: u64,
    /// `None` outside worker-assignment mode.
    pub schema_bundle: Option<SchemaBundle>,
}

impl AssignmentUpdate {
    /// The pair this update would put in force, for comparing against what already is.
    pub fn pair(&self) -> AppliedPair {
        AppliedPair {
            assignment_id: Some(self.id.clone()),
            bundle_hash: self.schema_bundle.as_ref().map(|b| b.hash),
        }
    }
}

/// The half of the published state that differs from what the worker holds.
///
/// Halves are reported separately because reaching them costs different things: a bundle is
/// merged where it stands, while an assignment has to be fetched and applied. Re-applying an
/// assignment that has not changed would be pure work — the index already holds it, and its
/// admission was decided when it was applied.
pub enum NetworkUpdate {
    /// The assignment differs from the one registered. Carries whatever bundle is published
    /// with it, which is merged first: the pair is admitted together (ADR-21).
    Assignment(AssignmentUpdate),
    /// Only the bundle differs. Merged where it stands; the assignment in force is untouched.
    SchemaBundle(SchemaBundle),
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

/// Yields the network state whenever it differs from `applied` — the pair the worker has in
/// force, read from what actually applied. A "last seen" marker would consume an assignment on
/// sighting, leaving a refused one unretryable, and would miss a bundle that moves under an
/// unchanged assignment id.
pub fn new_assignments_stream(
    url: String,
    frequency: Duration,
    timeout: Duration,
    max_delay: Duration,
    peer_id: PeerId,
    assignment_source: AssignmentSource,
    applied: impl Fn() -> AppliedPair,
) -> impl Stream<Item = NetworkUpdate> {
    let mut timer = tokio::time::interval(frequency);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let reqwest_client = new_reqwest_client(timeout, peer_id);

    stream! {
        loop {
            timer.tick().await;

            let mut current_delay = Duration::from_secs(1);
            loop {
                match poll_network_state(&url, &reqwest_client, assignment_source, &applied).await {
                    Ok(Some(data)) => {
                        yield data;
                        break;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to update assignment, retrying in {:?}", current_delay);
                        let duration = rand::rng().random_range((current_delay / 2)..current_delay);
                        tokio::time::sleep(duration).await;
                        current_delay = std::cmp::min(current_delay * 2, max_delay);
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
    applied: &impl Fn() -> AppliedPair,
) -> anyhow::Result<Option<NetworkUpdate>> {
    tracing::debug!("Checking network state: {url}");
    let mut network_state = fetch_network_state(url, reqwest_client).await?;
    // Parsed here, at the edge: a hash that isn't `sha256:<hex>` is a malformed network state,
    // and every comparison downstream is then between two verified hashes.
    let published_bundle = (assignment_source == AssignmentSource::Worker)
        .then(|| network_state.schema_bundle.take())
        .flatten()
        .map(SchemaBundle::try_from)
        .transpose()
        .inspect_err(|_| {
            metrics::SCHEMA_BUNDLE_FAILURES.inc();
        })?;

    let Some(assignment) = visible_assignment(&network_state, assignment_source) else {
        // Warn, not debug: a worker whose mode the publisher doesn't serve stands still.
        tracing::warn!(
            expected = match assignment_source {
                AssignmentSource::Worker => "worker_assignment",
                AssignmentSource::Legacy => "assignment",
            },
            "Network state carries no assignment for this worker's mode; waiting"
        );
        return Ok(None);
    };

    let in_force = applied();
    if in_force.assignment_id.as_deref() == Some(assignment.id.as_str()) {
        // The assignment is the one in force, so only a bundle can still differ.
        return Ok(published_bundle
            .filter(|bundle| in_force.bundle_hash != Some(bundle.hash))
            .map(NetworkUpdate::SchemaBundle));
    }

    let update = AssignmentUpdate {
        fb_url_v1: assignment
            .fb_url_v1
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Missing fb_url_v1"))?,
        id: assignment.id.clone(),
        _effective_from: assignment.effective_from,
        schema_bundle: published_bundle,
    };
    tracing::debug!("Discovered assignment \"{}\"", update.id);
    Ok(Some(NetworkUpdate::Assignment(update)))
}

/// Selects the assignment pointer to discover updates from.
///
/// Never falls back to the other mode's pointer: its bytes are a different format.
fn visible_assignment(
    network_state: &sqd_assignments::NetworkState,
    assignment_source: AssignmentSource,
) -> Option<&sqd_assignments::NetworkAssignment> {
    match assignment_source {
        AssignmentSource::Worker => network_state.worker_assignment.as_ref(),
        AssignmentSource::Legacy => network_state.assignment.as_ref(),
    }
}

async fn fetch_network_state(
    url: &str,
    reqwest_client: &reqwest::Client,
) -> anyhow::Result<sqd_assignments::NetworkState> {
    let response = reqwest_client.get(url).send().await?.error_for_status()?;
    let network_state = response.json().await?;
    Ok(network_state)
}

pub async fn fetch_assignment(
    url: &str,
    reqwest_client: &reqwest::Client,
) -> anyhow::Result<sqd_assignments::Assignment> {
    let buf = download_gzipped(url, reqwest_client).await?;
    Ok(sqd_assignments::Assignment::from_owned_unchecked(buf))
}

/// Decodes the dedicated worker-oriented assignment (NET-1186). Validated unlike
/// [`fetch_assignment`], so a malformed blob fails here instead of panicking much later, while
/// resolving a chunk's tables.
pub async fn fetch_worker_assignment(
    url: &str,
    reqwest_client: &reqwest::Client,
) -> anyhow::Result<sqd_assignments::WorkerAssignment> {
    let buf = download_gzipped(url, reqwest_client).await?;
    sqd_assignments::WorkerAssignment::from_owned(buf)
        .map_err(|e| anyhow::anyhow!("malformed worker assignment: {e}"))
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    fn network_state() -> sqd_assignments::NetworkState {
        sqd_assignments::NetworkState {
            network: "testnet".to_string(),
            assignment: Some(assignment("legacy")),
            worker_assignment: None,
            portal_assignment: None,
            schema_bundle: None,
        }
    }

    #[test]
    fn visible_assignment_uses_legacy_assignment_by_default() {
        let mut state = network_state();
        state.worker_assignment = Some(assignment("worker"));

        assert_eq!(
            visible_assignment(&state, AssignmentSource::Legacy)
                .unwrap()
                .id,
            "legacy"
        );
    }

    #[test]
    fn visible_assignment_uses_worker_assignment_when_enabled() {
        let mut state = network_state();
        state.worker_assignment = Some(assignment("worker"));

        assert_eq!(
            visible_assignment(&state, AssignmentSource::Worker)
                .unwrap()
                .id,
            "worker"
        );
    }

    #[test]
    fn visible_assignment_never_falls_back_to_the_other_pointer() {
        let legacy_only = network_state();
        assert!(visible_assignment(&legacy_only, AssignmentSource::Worker).is_none());

        let mut worker_only = network_state();
        worker_only.assignment = None;
        worker_only.worker_assignment = Some(assignment("worker"));
        assert!(visible_assignment(&worker_only, AssignmentSource::Legacy).is_none());
    }

    /// Serves each queued response to one connection, then stops accepting.
    /// Returns the server's base URL.
    async fn serve_responses(responses: Vec<Vec<u8>>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            for response in responses {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(&response).await;
            }
        });
        url
    }

    fn http_ok(body: &[u8]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn worker_state_json(id: &str, bundle_hash: BundleHash) -> Vec<u8> {
        format!(
            r#"{{"network":"test","worker_assignment":{{"id":"{id}","fb_url_v1":"http://example.com/{id}.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"{bundle_hash}","url":"http://example.com/bundle.tar.gz"}}}}"#
        )
        .into_bytes()
    }

    /// A distinct well-formed hash per byte value; the bundles are never fetched here.
    fn hash(tag: u8) -> BundleHash {
        format!("sha256:{}", format!("{tag:02x}").repeat(32))
            .parse()
            .unwrap()
    }

    #[track_caller]
    fn assignment_of(update: Option<NetworkUpdate>) -> AssignmentUpdate {
        match update {
            Some(NetworkUpdate::Assignment(update)) => update,
            Some(NetworkUpdate::SchemaBundle(b)) => {
                panic!("expected an assignment, got schema bundle {}", b.hash)
            }
            None => panic!("expected an assignment, got no update"),
        }
    }

    #[track_caller]
    fn bundle_of(update: Option<NetworkUpdate>) -> SchemaBundle {
        match update {
            Some(NetworkUpdate::SchemaBundle(bundle)) => bundle,
            Some(NetworkUpdate::Assignment(a)) => {
                panic!("expected a bundle on its own, got assignment {}", a.id)
            }
            None => panic!("expected a bundle, got no update"),
        }
    }

    fn test_client() -> reqwest::Client {
        new_reqwest_client(Duration::from_secs(5), PeerId::random())
    }

    /// The worker reconciles against the pair it has in force, so any half moving is a state to
    /// reach — and a pair it already holds is not re-offered.
    #[tokio::test]
    async fn either_half_moving_yields_an_update() {
        let a1b1 = worker_state_json("assignment-1", hash(0xaa));
        let a2b1 = worker_state_json("assignment-2", hash(0xaa));
        let a2b2 = worker_state_json("assignment-2", hash(0xbb));
        let url = serve_responses(vec![
            http_ok(&a1b1),
            http_ok(&a2b1),
            http_ok(&a2b1),
            http_ok(&a2b2),
        ])
        .await;
        let in_force = std::sync::Mutex::new(AppliedPair::default());
        // One guard: a second `lock()` in the same expression would still hold the first, and
        // the mutex is not reentrant.
        let applied = || {
            let in_force = in_force.lock().unwrap();
            AppliedPair {
                assignment_id: in_force.assignment_id.clone(),
                bundle_hash: in_force.bundle_hash,
            }
        };
        let client = test_client();
        let poll = || poll_network_state(&url, &client, AssignmentSource::Worker, &applied);

        // Nothing in force yet: the assignment, carrying the bundle it must be admitted with.
        let update = assignment_of(poll().await.unwrap());
        assert_eq!(update.id, "assignment-1");
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xaa));
        *in_force.lock().unwrap() = AppliedPair {
            assignment_id: Some("assignment-1".to_owned()),
            bundle_hash: Some(hash(0xaa)),
        };

        // The assignment moved; the bundle rides along and turns out to be already installed.
        let update = assignment_of(poll().await.unwrap());
        assert_eq!(update.id, "assignment-2");
        assert_eq!(update.schema_bundle.unwrap().hash, hash(0xaa));
        in_force.lock().unwrap().assignment_id = Some("assignment-2".to_owned());

        // Neither moved.
        assert!(poll().await.unwrap().is_none());

        // Only the bundle moved: merged on its own. Re-applying the assignment would be work for
        // nothing — the index already holds it, and it was admitted when it applied.
        assert_eq!(bundle_of(poll().await.unwrap()).hash, hash(0xbb));
    }

    /// The reason to reconcile against what applied rather than what was seen: a pair that fails
    /// anywhere — a bundle that won't install, an assignment that won't register — leaves what is
    /// in force unchanged, so the next poll offers it again instead of it being consumed.
    #[tokio::test]
    async fn a_pair_that_did_not_apply_is_offered_again() {
        let state = worker_state_json("assignment-1", hash(0xaa));
        let url = serve_responses(vec![http_ok(&state), http_ok(&state)]).await;
        // Nothing ever applies.
        let applied = AppliedPair::default;

        for attempt in 1..=2 {
            let update =
                poll_network_state(&url, &test_client(), AssignmentSource::Worker, &applied)
                    .await
                    .unwrap();
            assert_eq!(
                assignment_of(update).id,
                "assignment-1",
                "attempt {attempt}"
            );
        }
    }

    /// A bundle installed under an assignment that then failed to register is not enough: the
    /// pair is the unit, so the assignment comes round again on its own.
    #[tokio::test]
    async fn installing_the_bundle_alone_does_not_settle_the_pair() {
        let state = worker_state_json("assignment-1", hash(0xaa));
        let url = serve_responses(vec![http_ok(&state)]).await;
        let applied = || AppliedPair {
            assignment_id: None,
            bundle_hash: Some(hash(0xaa)),
        };

        let update = poll_network_state(&url, &test_client(), AssignmentSource::Worker, &applied)
            .await
            .unwrap();
        assert_eq!(assignment_of(update).id, "assignment-1");
    }

    #[tokio::test]
    async fn legacy_mode_ignores_the_schema_bundle() {
        let state = format!(
            r#"{{"network":"test","assignment":{{"id":"a1","fb_url_v1":"http://example.com/a1.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"sha256:aaa","url":"http://example.com/b.tar.gz"}}}}"#
        )
        .into_bytes();
        let url = serve_responses(vec![http_ok(&state), http_ok(&state)]).await;
        let in_force = std::sync::Mutex::new(AppliedPair::default());
        let applied = || AppliedPair {
            assignment_id: in_force.lock().unwrap().assignment_id.clone(),
            bundle_hash: None,
        };

        let update = poll_network_state(&url, &test_client(), AssignmentSource::Legacy, &applied)
            .await
            .unwrap();
        let update = assignment_of(update);
        assert_eq!(update.id, "a1");
        assert!(
            update.schema_bundle.is_none(),
            "legacy mode drops the bundle, malformed hash and all"
        );
        in_force.lock().unwrap().assignment_id = Some("a1".to_owned());

        let update = poll_network_state(&url, &test_client(), AssignmentSource::Legacy, &applied)
            .await
            .unwrap();
        assert!(update.is_none(), "the assignment alone is the whole pair");
    }
}
