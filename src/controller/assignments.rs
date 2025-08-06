use std::{io::ErrorKind, time::Duration};

use async_stream::try_stream;
use futures::{Stream, StreamExt};
use tokio::time::MissedTickBehavior;

pub struct AssignmentUpdate {
    pub assignment: sqd_assignments::Assignment,
    pub id: String,
    pub effective_from: u64,
}

pub fn new_assignments_stream(
    url: String,
    frequency: Duration,
    timeout: Duration,
) -> impl Stream<Item = AssignmentUpdate> {
    let mut timer = tokio::time::interval(frequency);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let reqwest_client = reqwest::Client::builder().timeout(timeout).build().unwrap();

    let mut last_id = None;

    let with_errs = try_stream! {
        loop {
            timer.tick().await;
            tracing::debug!("Checking for new assignment");
            let network_state = fetch_network_state(&url, &reqwest_client).await?;
            let assignment_id = network_state.assignment.id;
            if last_id.as_ref() == Some(&assignment_id) {
                tracing::debug!("Assignment has not been changed");
                continue;
            }

            tracing::debug!("Downloading assignment \"{}\"", assignment_id);
            let assignment = fetch_assignment(
                &network_state
                    .assignment
                    .fb_url
                    .ok_or(anyhow::anyhow!("Missing fb_url"))?,
                &reqwest_client,
            )
            .await?;
            last_id = Some(assignment_id.clone());

            tracing::debug!("Downloaded assignment \"{}\"", assignment_id);

            yield AssignmentUpdate {
                assignment,
                id: assignment_id,
                effective_from: network_state.assignment.effective_from,
            };
        }
    };

    with_errs.filter_map(|res: anyhow::Result<AssignmentUpdate>| async move {
        match res {
            Ok(data) => Some(data),
            Err(e) => {
                tracing::warn!(error = %e, "Failed to update assignment, waiting for the next one");
                None
            }
        }
    })
}

async fn fetch_network_state(
    url: &str,
    reqwest_client: &reqwest::Client,
) -> anyhow::Result<sqd_messages::assignments::NetworkState> {
    let response = reqwest_client.get(url).send().await?;
    let network_state = response.json().await?;
    Ok(network_state)
}

async fn fetch_assignment(
    url: &str,
    reqwest_client: &reqwest::Client,
) -> anyhow::Result<sqd_assignments::Assignment> {
    use async_compression::tokio::bufread::GzipDecoder;
    use futures::TryStreamExt;
    use tokio::io::AsyncReadExt;
    use tokio_util::io::StreamReader;

    let response = reqwest_client.get(url).send().await?;
    let stream = response.bytes_stream();
    let reader = StreamReader::new(stream.map_err(|e| std::io::Error::new(ErrorKind::Other, e)));
    let mut buf = Vec::new();
    let mut decoder = GzipDecoder::new(reader);
    decoder
        .read_to_end(&mut buf)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to decompress assignment: {}", e))?;
    Ok(sqd_assignments::Assignment::from_owned(buf)?)
}
