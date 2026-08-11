use std::{io::ErrorKind, time::Duration};

use async_stream::stream;
use futures::Stream;
use rand::Rng;
use sqd_contract_client::PeerId;
use tokio::time::MissedTickBehavior;

/// What one poll of the network state turned up. The assignment and the schema bundle are
/// versioned independently — the bundle can be revised (a schema clarified, a column
/// documented) without the assignment moving, and vice versa — so each is reported on its own
/// terms rather than one riding on the other's identity.
pub enum NetworkUpdate {
    /// A new assignment, carrying the bundle published alongside it so both are applied from one
    /// consistent network state.
    Assignment(AssignmentUpdate),
    /// The assignment is unchanged; only the schema bundle moved.
    SchemaBundle(sqd_assignments::SchemaBundle),
}

pub struct AssignmentUpdate {
    pub id: String,
    pub fb_url_v1: String,
    pub _effective_from: u64,
    /// Where to fetch the schema content the assignment references by id. `None` outside
    /// worker-assignment mode, which has no use for it.
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

/// Polls the network state, reporting the assignment and the schema bundle as they change.
///
/// `installed_bundle_hash` reports the bundle the worker currently has installed. Deduplicating
/// against that — rather than against a "last seen" marker kept here — means a bundle that fails
/// to install is offered again on the next poll instead of being silently skipped, and there is
/// no second copy of the same state to drift.
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

/// Reports a new assignment if there is one, otherwise a new schema bundle if only that moved.
///
/// A new assignment subsumes the bundle rather than reporting both: it carries whatever bundle
/// was published with it, and applying it installs that bundle first.
async fn poll_network_state(
    url: &str,
    reqwest_client: &reqwest::Client,
    last_id: &mut Option<String>,
    use_worker_assignments: bool,
    installed_bundle_hash: &impl Fn() -> Option<String>,
) -> anyhow::Result<Option<NetworkUpdate>> {
    tracing::debug!("Checking for new assignment: {url}");
    let mut network_state = fetch_network_state(url, reqwest_client).await?;
    // Legacy mode has no use for the bundle, so it never reports one.
    let published_bundle = use_worker_assignments
        .then(|| network_state.schema_bundle.take())
        .flatten();

    let new_assignment = match visible_assignment(&network_state, use_worker_assignments) {
        Some(visible) if last_id.as_deref() != Some(visible.id.as_str()) => Some(visible),
        Some(_) => {
            tracing::debug!("Assignment has not been changed");
            None
        }
        None => {
            // Not an error worth retrying against: the state parsed fine, it just doesn't carry
            // the pointer this worker was configured to read. Warn rather than debug — a worker
            // in this position serves nothing new until the publisher catches up.
            tracing::warn!(
                expected = if use_worker_assignments {
                    "worker_assignment"
                } else {
                    "assignment"
                },
                "Network state carries no assignment for this worker's mode; waiting"
            );
            None
        }
    };

    if let Some(visible) = new_assignment {
        let fb_url_v1 = visible
            .fb_url_v1
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Missing fb_url_v1"))?;
        let id = visible.id.clone();
        let _effective_from = visible.effective_from;
        *last_id = Some(id.clone());

        tracing::debug!("Discovered assignment \"{id}\"");
        return Ok(Some(NetworkUpdate::Assignment(AssignmentUpdate {
            id,
            fb_url_v1,
            _effective_from,
            schema_bundle: published_bundle,
        })));
    }

    // The assignment stood still, but the bundle can move on its own — schemas get clarified
    // without the chunk layout changing.
    if let Some(bundle) = published_bundle {
        if installed_bundle_hash().as_deref() != Some(bundle.hash.as_str()) {
            tracing::debug!(hash = %bundle.hash, "Discovered new schema bundle");
            return Ok(Some(NetworkUpdate::SchemaBundle(bundle)));
        }
        tracing::debug!("Schema bundle has not been changed");
    }

    Ok(None)
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

    /// A worker-mode state: the `worker_assignment` pointer plus a schema bundle.
    fn worker_state_json(id: &str, bundle_hash: &str) -> Vec<u8> {
        format!(
            r#"{{"network":"test","worker_assignment":{{"id":"{id}","fb_url_v1":"http://example.com/{id}.fb.gz","effective_from":0}},"schema_bundle":{{"hash":"{bundle_hash}","url":"http://example.com/bundle.tar.gz"}}}}"#
        )
        .into_bytes()
    }

    fn test_client() -> reqwest::Client {
        new_reqwest_client(Duration::from_secs(5), PeerId::random())
    }

    /// The assignment and the bundle move independently, so all four combinations have to be
    /// reported on their own terms — a revised schema must not need a new assignment to land,
    /// and a new assignment must not force a bundle it already has to be re-downloaded.
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
        // Stands in for the store: what the worker currently has installed.
        let installed = std::sync::Mutex::new(None::<String>);
        let installed_hash = || installed.lock().unwrap().clone();

        // Both new: reported as one assignment carrying its bundle.
        let update = poll_network_state(&url, &test_client(), &mut last_id, true, &installed_hash)
            .await
            .unwrap();
        let update = assignment_of(update);
        assert_eq!(update.id, "assignment-1");
        assert_eq!(update.schema_bundle.unwrap().hash, "sha256:aaa");
        *installed.lock().unwrap() = Some("sha256:aaa".to_owned());

        // New assignment, same bundle: still one assignment, and because the hash matches what
        // is installed, applying it re-downloads nothing.
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

        // New bundle, same assignment: reported on its own, without re-fetching the assignment.
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

    /// A bundle that fails to install stays uninstalled, so the next poll offers it again
    /// instead of skipping it as already seen.
    #[tokio::test]
    async fn an_uninstalled_bundle_is_offered_again() {
        let state = worker_state_json("assignment-1", "sha256:aaa");
        let url = serve_responses(vec![http_ok(&state), http_ok(&state)]).await;
        let mut last_id = None;

        let update = poll_network_state(&url, &test_client(), &mut last_id, true, &|| None)
            .await
            .unwrap();
        assert_eq!(assignment_of(update).id, "assignment-1");

        // The assignment id is consumed, but nothing was installed, so the bundle comes back.
        let update = poll_network_state(&url, &test_client(), &mut last_id, true, &|| None)
            .await
            .unwrap();
        assert!(
            matches!(update, Some(NetworkUpdate::SchemaBundle(ref b)) if b.hash == "sha256:aaa"),
            "got {}",
            describe(&update)
        );
    }

    /// Legacy mode has no use for the bundle and must not be disturbed by one being published.
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

        // Nothing installed, but legacy mode must still not report a bundle update.
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
