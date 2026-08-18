use std::{io::ErrorKind, time::Duration};

use async_stream::stream;
use futures::Stream;
use rand::Rng;
use sqd_contract_client::PeerId;
use tokio::time::MissedTickBehavior;

use super::schema_bundle::{BundleHash, SchemaBundle};
use crate::cli::AssignmentSource;
use crate::metrics;

/// Identifies an (assignment, bundle) pair — ADR-21's unit of intake. The stream remembers
/// the last pair it announced, and the pending queue dedups on the same identity.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NetworkPair {
    pub assignment_id: Option<String>,
    pub bundle_hash: Option<BundleHash>,
}

pub struct AssignmentUpdate {
    pub id: String,
    pub fb_url_v1: String,
    pub _effective_from: u64,
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

/// Yields published state that differs from what this stream last announced (WP-1: an
/// unchanged pair is a no-op). What became of an announced pair — applied, refused, or
/// still being fetched — is the consumer's business: only it knows whether another attempt
/// could end differently, so only it may ask for one.
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
    let mut announced = NetworkPair::default();

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
    announced: &mut NetworkPair,
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
    if assignment_source == AssignmentSource::Worker && published_bundle.is_none() {
        metrics::SCHEMA_BUNDLE_FAILURES.inc();
        anyhow::bail!("network state publishes a worker assignment but no schema bundle");
    }

    let current = NetworkPair {
        assignment_id: Some(assignment.id.clone()),
        bundle_hash: published_bundle.as_ref().map(|bundle| bundle.hash),
    };
    if *announced == current {
        return Ok(None);
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
    *announced = update.pair();
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
        let url = serve_responses(vec![
            http_ok(&a1b1),
            http_ok(&a2b1),
            http_ok(&a2b1),
            http_ok(&a2b2),
        ])
        .await;
        let client = test_client();
        let source = AssignmentSource::Worker;
        let mut announced = NetworkPair::default();

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

    #[tokio::test]
    async fn a_pair_already_announced_is_not_offered_again() {
        let state = worker_state_json("assignment-1", hash(0xaa));
        let url = serve_responses(vec![http_ok(&state), http_ok(&state)]).await;
        let client = test_client();
        let source = AssignmentSource::Worker;
        let mut announced = NetworkPair::default();

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
        let url = serve_responses(vec![http_ok(&state)]).await;
        let mut announced = NetworkPair {
            assignment_id: None,
            bundle_hash: Some(hash(0xaa)),
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

    #[tokio::test]
    async fn worker_mode_requires_a_bundle_when_the_assignment_is_unchanged() {
        let state = br#"{"network":"test","worker_assignment":{"id":"assignment-1","fb_url_v1":"http://example.com/a.fb.gz","effective_from":0}}"#;
        let url = serve_responses(vec![http_ok(state)]).await;
        let mut announced = NetworkPair {
            assignment_id: Some("assignment-1".to_owned()),
            bundle_hash: Some(hash(0xaa)),
        };

        let error = match poll_network_state(
            &url,
            &test_client(),
            AssignmentSource::Worker,
            &mut announced,
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("missing bundle must be rejected"),
        };

        assert!(error.to_string().contains("no schema bundle"));
    }

    #[tokio::test]
    async fn legacy_mode_ignores_the_schema_bundle() {
        let state = format!(
            r#"{{"network":"test","assignment":{{"id":"a1","fb_url_v1":"http://example.com/a1.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"sha256:aaa","url":"http://example.com/b.tar.gz"}}}}"#
        )
        .into_bytes();
        let url = serve_responses(vec![http_ok(&state), http_ok(&state)]).await;
        let client = test_client();
        let source = AssignmentSource::Legacy;
        let mut announced = NetworkPair::default();

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
