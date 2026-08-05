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
    /// Whether this update was discovered via the dedicated `worker_assignment` pointer rather
    /// than the legacy shared `assignment` (NET-1186). Always `false` outside `mvcc-chunks`.
    #[cfg(feature = "mvcc-chunks")]
    pub is_worker_assignment: bool,
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
                match update_assignment(&url, &reqwest_client, &mut last_id).await {
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
) -> anyhow::Result<Option<AssignmentUpdate>> {
    tracing::debug!("Checking for new assignment: {url}");
    let network_state = fetch_network_state(&url, &reqwest_client).await?;
    #[cfg_attr(not(feature = "mvcc-chunks"), allow(unused_variables))]
    let (visible, is_worker_assignment) = visible_assignment(&network_state);
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
        #[cfg(feature = "mvcc-chunks")]
        is_worker_assignment,
    }))
}

/// Selects the assignment pointer to discover updates from.
///
/// `mvcc-chunks` builds prefer the dedicated `worker_assignment` pointer but fall back to the
/// legacy `assignment` so rollouts tolerate a scheduler that hasn't started publishing
/// `worker_assignment` yet — mirrors sqd-portal's `visible_assignment` (NET-1186). Non-`mvcc-chunks`
/// builds always use the legacy pointer; there is no other concept of assignment to prefer.
fn visible_assignment(
    network_state: &sqd_assignments::NetworkState,
) -> (&sqd_assignments::NetworkAssignment, bool) {
    #[cfg(feature = "mvcc-chunks")]
    {
        match network_state.worker_assignment.as_ref() {
            Some(assignment) => (assignment, true),
            None => {
                tracing::debug!(
                    "worker_assignment missing in network state; falling back to legacy assignment"
                );
                (&network_state.assignment, false)
            }
        }
    }
    #[cfg(not(feature = "mvcc-chunks"))]
    {
        (&network_state.assignment, false)
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

/// Decodes the dedicated worker-oriented assignment (NET-1186). Parsing only — wiring the result
/// into `DatasetsIndex`/serving state is out of scope until schema delivery exists (there is
/// nothing to derive a `WorkerAssignmentChunk`'s files from yet).
#[cfg(feature = "mvcc-chunks")]
pub async fn fetch_worker_assignment(
    url: &str,
    reqwest_client: &reqwest::Client,
) -> anyhow::Result<sqd_assignments::WorkerAssignment> {
    let buf = download_gzipped(url, reqwest_client).await?;
    Ok(sqd_assignments::WorkerAssignment::from_owned_unchecked(buf))
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
            assignment: assignment("legacy"),
            #[cfg(feature = "mvcc-chunks")]
            worker_assignment: None,
            #[cfg(feature = "mvcc-chunks")]
            portal_assignment: None,
        }
    }

    #[test]
    fn visible_assignment_uses_legacy_assignment() {
        let state = network_state();
        let (visible, is_worker_assignment) = visible_assignment(&state);

        assert_eq!(visible.id, "legacy");
        #[cfg(feature = "mvcc-chunks")]
        assert!(!is_worker_assignment);
        #[cfg(not(feature = "mvcc-chunks"))]
        let _ = is_worker_assignment;
    }

    #[cfg(feature = "mvcc-chunks")]
    #[test]
    fn visible_assignment_prefers_worker_assignment() {
        let mut state = network_state();
        state.worker_assignment = Some(assignment("worker"));

        let (visible, is_worker_assignment) = visible_assignment(&state);
        assert_eq!(visible.id, "worker");
        assert!(is_worker_assignment);
    }
}
