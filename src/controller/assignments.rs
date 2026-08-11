use std::{io::ErrorKind, time::Duration};

use async_stream::stream;
use futures::Stream;
use rand::Rng;
use sqd_contract_client::PeerId;
use tokio::time::MissedTickBehavior;

/// The assignment and the schema bundle are versioned independently, and reported separately.
pub enum NetworkUpdate {
    /// A new assignment, carrying whatever bundle was published alongside it.
    Assignment(AssignmentUpdate),
    /// The assignment is unchanged; only the schema bundle moved.
    SchemaBundle(sqd_assignments::SchemaBundle),
}

pub struct AssignmentUpdate {
    pub id: String,
    pub fb_url_v1: String,
    pub _effective_from: u64,
    /// `None` outside worker-assignment mode.
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

/// Polls for assignment and schema bundle updates. Deduplicating the bundle against
/// `installed_bundle_hash` rather than a "last seen" marker re-offers one that failed to install.
pub fn new_assignments_stream(
    url: String,
    frequency: Duration,
    timeout: Duration,
    max_delay: Duration,
    peer_id: PeerId,
    use_worker_assignments: bool,
    installed_bundle_hash: impl Fn() -> Option<String>,
) -> impl Stream<Item = NetworkUpdate> {
    let mut timer = tokio::time::interval(frequency);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let reqwest_client = new_reqwest_client(timeout, peer_id);

    let mut last_id = None;

    stream! {
        loop {
            timer.tick().await;

            let mut current_delay = Duration::from_secs(1);
            loop {
                match poll_network_state(&url, &reqwest_client, &mut last_id, use_worker_assignments, &installed_bundle_hash).await {
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
    last_id: &mut Option<String>,
    use_worker_assignments: bool,
    installed_bundle_hash: &impl Fn() -> Option<String>,
) -> anyhow::Result<Option<NetworkUpdate>> {
    tracing::debug!("Checking network state: {url}");
    let mut network_state = fetch_network_state(url, reqwest_client).await?;
    let published_bundle = use_worker_assignments
        .then(|| network_state.schema_bundle.take())
        .flatten();

    match changed_assignment(&network_state, last_id.as_deref(), use_worker_assignments) {
        Some(assignment) => {
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
            *last_id = Some(update.id.clone());
            Ok(Some(NetworkUpdate::Assignment(update)))
        }
        None => Ok(published_bundle
            .filter(|bundle| installed_bundle_hash().as_deref() != Some(bundle.hash.as_str()))
            .map(NetworkUpdate::SchemaBundle)),
    }
}

/// The assignment this worker reads, unless it is the one already reported.
fn changed_assignment<'a>(
    network_state: &'a sqd_assignments::NetworkState,
    last_id: Option<&str>,
    use_worker_assignments: bool,
) -> Option<&'a sqd_assignments::NetworkAssignment> {
    let Some(assignment) = visible_assignment(network_state, use_worker_assignments) else {
        // Warn, not debug: a worker whose mode the publisher doesn't serve stands still.
        tracing::warn!(
            expected = if use_worker_assignments {
                "worker_assignment"
            } else {
                "assignment"
            },
            "Network state carries no assignment for this worker's mode; waiting"
        );
        return None;
    };
    if last_id == Some(assignment.id.as_str()) {
        tracing::debug!("Assignment has not been changed");
        return None;
    }
    Some(assignment)
}

/// Selects the assignment pointer to discover updates from.
///
/// Never falls back to the other mode's pointer: its bytes are a different format.
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

        assert_eq!(visible_assignment(&state, false).unwrap().id, "legacy");
    }

    #[test]
    fn visible_assignment_uses_worker_assignment_when_enabled() {
        let mut state = network_state();
        state.worker_assignment = Some(assignment("worker"));

        assert_eq!(visible_assignment(&state, true).unwrap().id, "worker");
    }

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

    fn worker_state_json(id: &str, bundle_hash: &str) -> Vec<u8> {
        format!(
            r#"{{"network":"test","worker_assignment":{{"id":"{id}","fb_url_v1":"http://example.com/{id}.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"{bundle_hash}","url":"http://example.com/bundle.tar.gz"}}}}"#
        )
        .into_bytes()
    }

    fn test_client() -> reqwest::Client {
        new_reqwest_client(Duration::from_secs(5), PeerId::random())
    }

    /// All four assignment/bundle change combinations are reported on their own terms.
    #[tokio::test]
    async fn assignment_and_bundle_are_versioned_independently() {
        let a1b1 = worker_state_json("assignment-1", "sha256:aaa");
        let a2b1 = worker_state_json("assignment-2", "sha256:aaa");
        let a2b2 = worker_state_json("assignment-2", "sha256:bbb");
        let url = serve_responses(vec![
            http_ok(&a1b1),
            http_ok(&a2b1),
            http_ok(&a2b1),
            http_ok(&a2b2),
        ])
        .await;
        let mut last_id = None;
        let installed = std::sync::Mutex::new(None::<String>);
        let installed_hash = || installed.lock().unwrap().clone();

        // Both new: one assignment, carrying its bundle.
        let update = poll_network_state(&url, &test_client(), &mut last_id, true, &installed_hash)
            .await
            .unwrap();
        let update = assignment_of(update);
        assert_eq!(update.id, "assignment-1");
        assert_eq!(update.schema_bundle.unwrap().hash, "sha256:aaa");
        *installed.lock().unwrap() = Some("sha256:aaa".to_owned());

        // New assignment, same bundle: the bundle rides along, already installed.
        let update = poll_network_state(&url, &test_client(), &mut last_id, true, &installed_hash)
            .await
            .unwrap();
        let update = assignment_of(update);
        assert_eq!(update.id, "assignment-2");
        assert_eq!(update.schema_bundle.unwrap().hash, "sha256:aaa");

        // Neither moved: nothing to report.
        let update = poll_network_state(&url, &test_client(), &mut last_id, true, &installed_hash)
            .await
            .unwrap();
        assert!(update.is_none());

        // New bundle, same assignment: reported on its own.
        let update = poll_network_state(&url, &test_client(), &mut last_id, true, &installed_hash)
            .await
            .unwrap();
        match update {
            Some(NetworkUpdate::SchemaBundle(bundle)) => assert_eq!(bundle.hash, "sha256:bbb"),
            other => panic!(
                "a bundle that moves on its own must be reported: got {}",
                describe(&other)
            ),
        }
    }

    /// A bundle that failed to install is offered again, not skipped as already seen.
    #[tokio::test]
    async fn an_uninstalled_bundle_is_offered_again() {
        let state = worker_state_json("assignment-1", "sha256:aaa");
        let url = serve_responses(vec![http_ok(&state), http_ok(&state)]).await;
        let mut last_id = None;

        let update = poll_network_state(&url, &test_client(), &mut last_id, true, &|| None)
            .await
            .unwrap();
        assert_eq!(assignment_of(update).id, "assignment-1");

        // The assignment id is consumed, but nothing was installed: the bundle comes back.
        let update = poll_network_state(&url, &test_client(), &mut last_id, true, &|| None)
            .await
            .unwrap();
        assert!(
            matches!(update, Some(NetworkUpdate::SchemaBundle(ref b)) if b.hash == "sha256:aaa"),
            "got {}",
            describe(&update)
        );
    }

    #[tokio::test]
    async fn legacy_mode_ignores_the_schema_bundle() {
        let state = format!(
            r#"{{"network":"test","assignment":{{"id":"a1","fb_url_v1":"http://example.com/a1.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"sha256:aaa","url":"http://example.com/b.tar.gz"}}}}"#
        )
        .into_bytes();
        let url = serve_responses(vec![http_ok(&state), http_ok(&state)]).await;
        let mut last_id = None;

        let update = poll_network_state(&url, &test_client(), &mut last_id, false, &|| None)
            .await
            .unwrap();
        let update = assignment_of(update);
        assert_eq!(update.id, "a1");
        assert!(
            update.schema_bundle.is_none(),
            "legacy mode drops the bundle"
        );

        // Nothing installed, yet still no bundle update.
        let update = poll_network_state(&url, &test_client(), &mut last_id, false, &|| None)
            .await
            .unwrap();
        assert!(update.is_none(), "got {}", describe(&update));
    }

    fn describe(update: &Option<NetworkUpdate>) -> String {
        match update {
            Some(NetworkUpdate::Assignment(a)) => format!("assignment {}", a.id),
            Some(NetworkUpdate::SchemaBundle(b)) => format!("schema bundle {}", b.hash),
            None => "no update".to_owned(),
        }
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

        let first = poll_network_state(&url, &test_client(), &mut last_id, false, &|| None)
            .await
            .unwrap();
        assert_eq!(assignment_of(first).id, "assignment-1");

        // The same id is now silently skipped — there is no way to ask for a retry
        let second = poll_network_state(&url, &test_client(), &mut last_id, false, &|| None)
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
