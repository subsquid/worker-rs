use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use futures::{future::FusedFuture, stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt};
use rand::Rng;
use reqwest::Url;
use sqd_contract_client::PeerId;
use tokio_util::{io::StreamReader, sync::CancellationToken};
use tracing::instrument;

use crate::types::state::ChunkRef;

use super::{
    datasets_index::{DatasetsIndex, RemoteFile},
    guard::FsGuard,
    local_fs::add_temp_prefix,
};

lazy_static::lazy_static! {
    static ref S3_TIMEOUT: Duration = std::env::var("S3_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(60));
    static ref S3_READ_TIMEOUT: Duration = std::env::var("S3_READ_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(3));
}

pub struct ChunkDownloader {
    futures: FuturesUnordered<tokio::task::JoinHandle<(ChunkRef, Result<()>)>>,
    cancel_tokens: HashMap<ChunkRef, CancellationToken>,
    reqwest_client: reqwest::Client,
    current_delay: Duration,
}

impl ChunkDownloader {
    pub fn new(peer_id: PeerId) -> Self {
        let client = reqwest::ClientBuilder::new()
            .user_agent(format!("SQD Worker {peer_id}"))
            .timeout(*S3_TIMEOUT)
            .read_timeout(*S3_READ_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("Can't create HTTP client");
        Self {
            futures: FuturesUnordered::default(),
            cancel_tokens: HashMap::default(),
            reqwest_client: client,
            current_delay: Duration::ZERO,
        }
    }
}

impl ChunkDownloader {
    pub fn start_download(
        &mut self,
        chunk: ChunkRef,
        dst: PathBuf,
        datasets_index: &DatasetsIndex,
    ) {
        let cancel_token = CancellationToken::new();

        let previous = self
            .cancel_tokens
            .insert(chunk.clone(), cancel_token.clone());
        if previous.is_some() {
            panic!("Chunk {chunk} is already being downloaded");
        }

        let files = datasets_index
            .list_files(&chunk.dataset, &chunk.chunk)
            .unwrap_or_else(|| {
                panic!("Dataset {} not found", chunk.dataset);
            });
        let num_files = files.len();
        let headers = datasets_index.get_headers().clone();
        let client = self.reqwest_client.clone();
        let current_delay = self.current_delay;
        self.futures.push(tokio::spawn(async move {
            if current_delay > Duration::ZERO {
                tracing::debug!("Waiting for {:?} before the next download", current_delay);
                tokio::time::sleep(current_delay).await;
            }
            tokio::select! {
                result = download_dir(files, dst, &client, &headers) => {
                    (chunk, result)
                }
                _ = tokio::time::sleep(*S3_TIMEOUT * num_files as u32) => {
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
                            self.current_delay *= 2;
                            let jitter = Duration::from_millis(rand::rng().random_range(50..150));
                            self.current_delay = std::cmp::min(
                                self.current_delay + jitter,
                                Duration::from_secs(600),
                            );
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
