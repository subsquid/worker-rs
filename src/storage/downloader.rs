use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use futures::{future::FusedFuture, stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt};
use rand::Rng;
use reqwest::Url;
use sqd_contract_client::PeerId;
use tokio_util::{io::StreamReader, sync::CancellationToken};
use tracing::instrument;

use crate::{cli, types::state::ChunkRef};

use super::{datasets_index::RemoteFile, guard::FsGuard, local_fs::add_temp_prefix};

const START_DELAY: Duration = Duration::from_millis(100);

pub struct ChunkDownloader {
    futures: FuturesUnordered<tokio::task::JoinHandle<(ChunkRef, Result<()>)>>,
    cancel_tokens: HashMap<ChunkRef, CancellationToken>,
    reqwest_client: reqwest::Client,
    current_delay: Duration,
    args: cli::Args,
}

impl ChunkDownloader {
    pub fn new(peer_id: PeerId, args: cli::Args) -> Self {
        let client = reqwest::ClientBuilder::new()
            .user_agent(format!("SQD Worker {peer_id}"))
            .timeout(args.s3_timeout)
            .read_timeout(args.s3_read_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("Can't create HTTP client");
        Self {
            futures: FuturesUnordered::default(),
            cancel_tokens: HashMap::default(),
            reqwest_client: client,
            current_delay: Duration::ZERO,
            args,
        }
    }
}

impl ChunkDownloader {
    pub fn start_download(
        &mut self,
        chunk: ChunkRef,
        dst: PathBuf,
        files: Vec<RemoteFile>,
        headers: reqwest::header::HeaderMap,
    ) {
        let cancel_token = CancellationToken::new();

        let previous = self
            .cancel_tokens
            .insert(chunk.clone(), cancel_token.clone());
        if previous.is_some() {
            panic!("Chunk {chunk} is already being downloaded");
        }

        let watchdog = watchdog_timeout(self.args.s3_timeout, files.len());
        let client = self.reqwest_client.clone();
        let current_delay = self.current_delay;
        self.futures.push(tokio::spawn(async move {
            if current_delay > Duration::ZERO {
                let sleep = rand::rng().random_range((current_delay / 2)..current_delay);
                tracing::debug!("Waiting for {:?} before the next download", sleep);
                tokio::select! {
                    _ = tokio::time::sleep(sleep) => {},
                    _ = cancel_token.cancelled() => {},
                }
            }
            tokio::select! {
                result = download_dir(files, dst, &client, &headers) => {
                    (chunk, result)
                }
                _ = tokio::time::sleep(watchdog) => {
                    (chunk, Err(anyhow!("Download timed out")))
                }
                _ = cancel_token.cancelled_owned() => {
                    (chunk, Err(anyhow!("Download cancelled")))
                }
            }
        }));
    }

    pub fn downloaded(&mut self) -> impl FusedFuture<Output = (ChunkRef, Result<()>)> + '_ {
        if self.futures.is_empty() {
            futures::future::Fuse::terminated()
        } else {
            self.futures
                .select_next_some()
                .map(|result| {
                    let (chunk, result) = result.expect("Download task panicked");
                    self.cancel_tokens.remove(&chunk);
                    match result {
                        Ok(()) => {
                            self.current_delay = Duration::from_secs(0);
                        }
                        Err(_) => {
                            if self.current_delay == Duration::ZERO {
                                self.current_delay = START_DELAY;
                            } else {
                                self.current_delay = std::cmp::min(
                                    self.current_delay * 2,
                                    self.args.downloads_max_delay,
                                );
                            }
                        }
                    }
                    (chunk, result)
                })
                .fuse()
        }
    }

    pub fn download_count(&self) -> usize {
        self.futures.len()
    }

    pub fn cancel(&mut self, chunk: &ChunkRef) {
        if let Some(cancel_token) = self.cancel_tokens.remove(chunk) {
            cancel_token.cancel();
        }
    }
}

/// Whole-chunk download bound: one per-file timeout per file. The file count comes from the
/// document, so it saturates rather than overflowing, and zero files still get one timeout.
fn watchdog_timeout(per_file: Duration, num_files: usize) -> Duration {
    per_file.saturating_mul(u32::try_from(num_files.max(1)).unwrap_or(u32::MAX))
}

/// Either downloads the entire directory or nothing at all.
/// This function is cancel-safe. If it is not awaited until the end,
/// it will clean up temporary results.
///
/// Careful: this function never removes any parent dirs so it can produce
/// a dangling empty dir after cleanup.
#[instrument(skip_all)]
async fn download_dir(
    files: Vec<RemoteFile>,
    dst_dir: PathBuf,
    client: &reqwest::Client,
    headers: &reqwest::header::HeaderMap,
) -> Result<()> {
    let tmp = &add_temp_prefix(&dst_dir)?;
    let mut guard = FsGuard::new(tmp)?;
    futures::future::try_join_all(files.into_iter().map(|file| async move {
        let dst_file = tmp.join(file.name.parse::<PathBuf>()?);
        download_one(file.url, &dst_file, client, headers.clone()).await
    }))
    .await?;
    guard.persist(dst_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FM-1: a document-chosen file count must not panic the task. `Duration * u32` does.
    #[test]
    fn watchdog_survives_any_file_count() {
        let per_file = Duration::from_secs(60);
        assert_eq!(watchdog_timeout(per_file, 3), Duration::from_secs(180));
        assert_eq!(watchdog_timeout(per_file, 0), per_file);
        assert_eq!(
            watchdog_timeout(per_file, usize::MAX),
            per_file.saturating_mul(u32::MAX)
        );
        assert_eq!(
            watchdog_timeout(Duration::MAX, usize::MAX),
            Duration::MAX,
            "the bound saturates instead of overflowing"
        );
    }
}

#[instrument(skip_all)]
pub async fn download_one(
    url: Url,
    dst_path: &Path,
    client: &reqwest::Client,
    headers: reqwest::header::HeaderMap,
) -> Result<()> {
    let mut writer = tokio::fs::File::create(dst_path)
        .await
        .with_context(|| format!("Couldn't create file '{dst_path}'"))?;
    let response = client
        .get(url)
        .headers(headers)
        .send()
        .await?
        .error_for_status()?;
    let stream = response.bytes_stream();
    let mut reader =
        StreamReader::new(stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    tokio::io::copy(&mut reader, &mut writer).await?;
    Ok(())
}
