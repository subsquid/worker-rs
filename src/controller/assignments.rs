use std::{io::ErrorKind, time::Duration};

use async_stream::stream;
use futures::Stream;
use rand::Rng;
use sqd_contract_client::PeerId;
use tokio::time::MissedTickBehavior;

use super::schema_bundle::{BundleHash, SchemaBundle};
use crate::cli::AssignmentSource;
use crate::metrics;

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

    stream! {
        loop {
            timer.tick().await;

            let mut current_delay = Duration::from_secs(1);
            loop {
                match poll_network_state(&url, &reqwest_client, assignment_source, &mut announced).await {
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
    announced: &mut Announced,
) -> anyhow::Result<Option<AssignmentUpdate>> {
    tracing::debug!("Checking network state: {url}");
    let mut network_state = fetch_network_state(url, reqwest_client).await?;
    let published_bundle = (assignment_source == AssignmentSource::Worker)
        .then(|| network_state.schema_bundle.take())
        .flatten()
        .map(SchemaBundle::try_from)
        .transpose()
        .inspect_err(|_| {
            metrics::SCHEMA_BUNDLE_FAILURES.inc();
        })?;

    let Some(assignment) = visible_assignment(&network_state, assignment_source) else {
        tracing::warn!(
            expected = match assignment_source {
                AssignmentSource::Worker => "worker_assignment",
                AssignmentSource::Legacy => "assignment",
            },
            "Network state carries no assignment for this worker's mode; waiting"
        );
        return Ok(None);
    };
    // A half-published pair is re-read at the poll cadence, not the error backoff (FM-53d).
    if assignment_source == AssignmentSource::Worker && published_bundle.is_none() {
        metrics::SCHEMA_BUNDLE_MISMATCHES.inc();
        tracing::warn!(
            assignment_id = %assignment.id,
            "Network state publishes a worker assignment but no schema bundle; waiting"
        );
        return Ok(None);
    }

    published_update(assignment, published_bundle, announced)
}

fn published_update(
    assignment: &sqd_assignments::NetworkAssignment,
    published_bundle: Option<SchemaBundle>,
    announced: &mut Announced,
) -> anyhow::Result<Option<AssignmentUpdate>> {
    let update = AssignmentUpdate {
        fb_url_v1: assignment
            .fb_url_v1
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Missing fb_url_v1"))?,
        id: assignment.id.clone(),
        _effective_from: assignment.effective_from,
        schema_bundle: published_bundle,
    };
    let current = update.announced();
    if *announced == current {
        return Ok(None);
    }
    tracing::debug!("Discovered assignment \"{}\"", update.id);
    *announced = current;
    Ok(Some(update))
}

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
        assert_eq!(
            metrics::SCHEMA_BUNDLE_MISMATCHES.get(),
            mismatches_before + 1,
            "the scheduler is who resolves it, so it counts with the other pair faults"
        );
        assert_eq!(
            announced.pair.bundle_hash,
            Some(hash(0xaa)),
            "nothing was announced, so the pair is offered whole when the bundle returns"
        );
    }

    #[tokio::test]
    async fn legacy_mode_ignores_the_schema_bundle() {
        let state = format!(
            r#"{{"network":"test","assignment":{{"id":"a1","fb_url_v1":"http://example.com/a1.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"sha256:aaa","url":"http://example.com/b.tar.gz"}}}}"#
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
            "legacy mode drops the bundle, malformed hash and all"
        );

        let update = poll_network_state(&url, &client, source, &mut announced)
            .await
            .unwrap();
        assert!(update.is_none(), "the assignment alone is the whole pair");
    }
}
