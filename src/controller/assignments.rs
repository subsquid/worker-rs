use std::{io::ErrorKind, time::Duration};

use async_stream::stream;
use futures::Stream;
use rand::Rng;
use sqd_contract_client::PeerId;
use tokio::time::MissedTickBehavior;

pub struct AssignmentUpdate {
    pub id: String,
    pub fb_url_v1: String,
    pub _effective_from: u64,
    /// Where to fetch the schema content the assignment references by id, as published alongside
    /// this assignment. Carried on the update so both are applied from one consistent network
    /// state rather than two independent polls.
    ///
    /// Known limitation: updates are deduplicated by assignment id, so a network state that
    /// changes only its `schema_bundle` yields nothing and the new bundle is picked up when the
    /// next assignment lands. Harmless while nothing reads the cached schemas; revisit before
    /// anything does.
    pub schema_bundle: Option<sqd_assignments::SchemaBundle>,
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

pub fn new_assignments_stream(
    url: String,
    frequency: Duration,
    timeout: Duration,
    max_delay: Duration,
    peer_id: PeerId,
    use_worker_assignments: bool,
) -> impl Stream<Item = AssignmentUpdate> {
    let mut timer = tokio::time::interval(frequency);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let reqwest_client = new_reqwest_client(timeout, peer_id);

    let mut last_id = None;

    stream! {
        loop {
            timer.tick().await;

            let mut current_delay = Duration::from_secs(1);
            loop {
                match update_assignment(&url, &reqwest_client, &mut last_id, use_worker_assignments).await {
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

async fn update_assignment(
    url: &str,
    reqwest_client: &reqwest::Client,
    last_id: &mut Option<String>,
    use_worker_assignments: bool,
) -> anyhow::Result<Option<AssignmentUpdate>> {
    tracing::debug!("Checking for new assignment: {url}");
    let mut network_state = fetch_network_state(url, reqwest_client).await?;
    let Some(visible) = visible_assignment(&network_state, use_worker_assignments) else {
        // Not an error worth retrying against: the state parsed fine, it just doesn't carry the
        // pointer this worker was configured to read. Warn rather than debug — a worker in this
        // position serves nothing new until the publisher catches up.
        tracing::warn!(
            expected = if use_worker_assignments {
                "worker_assignment"
            } else {
                "assignment"
            },
            "Network state carries no assignment for this worker's mode; waiting"
        );
        return Ok(None);
    };
    let assignment_id = visible.id.clone();
    if last_id.as_ref() == Some(&assignment_id) {
        tracing::debug!("Assignment has not been changed");
        return anyhow::Ok(None);
    }

    let fb_url_v1 = visible
        .fb_url_v1
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Missing fb_url_v1"))?;
    let _effective_from = visible.effective_from;
    *last_id = Some(assignment_id.clone());

    tracing::debug!("Discovered assignment \"{}\"", assignment_id);

    Ok(Some(AssignmentUpdate {
        id: assignment_id,
        fb_url_v1,
        _effective_from,
        // Only meaningful to the worker-assignment path; taken unconditionally so the field
        // reflects the state as published rather than the mode we happen to be in.
        schema_bundle: network_state.schema_bundle.take(),
    }))
}

/// Selects the assignment pointer to discover updates from.
///
/// Each mode reads exactly one pointer and never falls back to the other: mixing the two would
/// feed one format's bytes to the other's parser. Both are optional in a `NetworkState` — a
/// network mid-migration may publish either, both, or (for a mode it has retired) neither.
fn visible_assignment(
    network_state: &sqd_assignments::NetworkState,
    use_worker_assignments: bool,
) -> Option<&sqd_assignments::NetworkAssignment> {
    if use_worker_assignments {
        network_state.worker_assignment.as_ref()
    } else {
        network_state.assignment.as_ref()
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

/// Decodes the dedicated worker-oriented assignment (NET-1186).
///
/// Unlike [`fetch_assignment`], this validates the buffer instead of trusting it. A malformed
/// blob here would otherwise surface as a panic or garbage much later, while resolving a chunk's
/// tables through the inline rosters.
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

        assert_eq!(visible_assignment(&state, false).unwrap().id, "legacy");
    }

    #[test]
    fn visible_assignment_uses_worker_assignment_when_enabled() {
        let mut state = network_state();
        state.worker_assignment = Some(assignment("worker"));

        assert_eq!(visible_assignment(&state, true).unwrap().id, "worker");
    }

    /// Neither mode falls back to the other's pointer: feeding one format's bytes to the other's
    /// parser is worse than serving nothing.
    #[test]
    fn visible_assignment_never_falls_back_to_the_other_pointer() {
        let legacy_only = network_state();
        assert!(visible_assignment(&legacy_only, true).is_none());

        let mut worker_only = network_state();
        worker_only.assignment = None;
        worker_only.worker_assignment = Some(assignment("worker"));
        assert!(visible_assignment(&worker_only, false).is_none());
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

    fn network_state_json(id: &str, effective_from: u64) -> Vec<u8> {
        format!(
            r#"{{"network":"test","assignment":{{"id":"{id}","fb_url_v1":"http://example.com/{id}.fb.gz","effective_from":{effective_from}}}}}"#
        )
        .into_bytes()
    }

    fn test_client() -> reqwest::Client {
        new_reqwest_client(Duration::from_secs(5), PeerId::random())
    }

    // Known limitation (not yet fixed): the assignment id is recorded as seen the
    // moment the update is yielded — before anyone knows whether the assignment
    // can actually be applied. If registration fails downstream (corrupted
    // download, no entry for this worker, header decryption failure), the stream
    // never offers the same id again, so the worker idles on its old assignment
    // until the network publishes a *different* id.
    #[tokio::test]
    async fn assignment_id_is_consumed_before_the_assignment_is_known_to_apply() {
        let state = network_state_json("assignment-1", 0);
        let url = serve_responses(vec![http_ok(&state), http_ok(&state)]).await;
        let mut last_id = None;

        let first = update_assignment(&url, &test_client(), &mut last_id, false)
            .await
            .unwrap();
        assert_eq!(first.unwrap().id, "assignment-1");

        // The same id is now silently skipped — there is no way to ask for a retry
        let second = update_assignment(&url, &test_client(), &mut last_id, false)
            .await
            .unwrap();
        assert!(second.is_none());
    }

    // Known limitation (not yet fixed): the downloaded flatbuffer is never
    // validated (`from_owned_unchecked`) and carries no integrity check, so a
    // corrupted-but-gzip-valid body is accepted here and can panic or return
    // garbage at any later access.
    #[tokio::test]
    async fn fetch_assignment_accepts_bytes_that_fail_validation() {
        use async_compression::tokio::write::GzipEncoder;

        let garbage = b"definitely not a flatbuffer".to_vec();
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(&garbage).await.unwrap();
        encoder.shutdown().await.unwrap();
        let url = serve_responses(vec![http_ok(&encoder.into_inner())]).await;

        let fetched = fetch_assignment(&url, &test_client()).await;
        assert!(fetched.is_ok(), "unchecked parsing accepts arbitrary bytes");
        // The checked constructor rejects the very same bytes
        assert!(sqd_assignments::Assignment::from_owned(garbage).is_err());
    }
}
