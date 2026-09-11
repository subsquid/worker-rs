use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use futures::{future::FusedFuture, stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt};
use parquet::file::metadata::ParquetMetaDataReader;
use rand::Rng;
use reqwest::Url;
use sqd_contract_client::PeerId;
use tokio_util::{io::StreamReader, sync::CancellationToken};
use tracing::instrument;

use crate::{cli, types::state::ChunkRef};

use super::{datasets_index::RemoteFile, guard::FsGuard, local_fs::add_temp_prefix};

const START_DELAY: Duration = Duration::from_millis(100);

/// The subset of [`cli::Args`] the downloader needs. Kept separate so tests
/// can construct it without going through clap.
#[derive(Debug, Clone, Copy)]
pub struct DownloadConfig {
    pub s3_timeout: Duration,
    pub s3_read_timeout: Duration,
    pub downloads_max_delay: Duration,
    pub max_download_attempts: u8,
}

impl From<&cli::Args> for DownloadConfig {
    fn from(args: &cli::Args) -> Self {
        Self {
            s3_timeout: args.s3_timeout,
            s3_read_timeout: args.s3_read_timeout,
            downloads_max_delay: args.downloads_max_delay,
            max_download_attempts: args.max_download_attempts,
        }
    }
}

pub struct ChunkDownloader {
    futures: FuturesUnordered<tokio::task::JoinHandle<(ChunkRef, Result<()>)>>,
    cancel_tokens: HashMap<ChunkRef, CancellationToken>,
    reqwest_client: reqwest::Client,
    current_delay: Duration,
    config: DownloadConfig,
}

impl ChunkDownloader {
    pub fn new(peer_id: PeerId, config: DownloadConfig) -> Self {
        let client = reqwest::ClientBuilder::new()
            .user_agent(format!("SQD Worker {peer_id}"))
            .timeout(config.s3_timeout)
            .read_timeout(config.s3_read_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("Can't create HTTP client");
        Self {
            futures: FuturesUnordered::default(),
            cancel_tokens: HashMap::default(),
            reqwest_client: client,
            current_delay: Duration::ZERO,
            config,
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

        let num_files = files.len();
        let client = self.reqwest_client.clone();
        let current_delay = self.current_delay;
        let s3_timeout = self.config.s3_timeout;
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
                _ = tokio::time::sleep(s3_timeout * num_files as u32) => {
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
                                    self.config.downloads_max_delay,
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
    // Read+write: the parquet check below reads the footer back through the same handle.
    let mut writer = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst_path)
        .await
        .with_context(|| format!("Couldn't create file '{dst_path}'"))?;
    let response = client
        .get(url)
        .headers(headers)
        .send()
        .await?
        .error_for_status()?;
    // `error_for_status` passes 3xx, and no redirect is followed, so the response would be
    // the redirect page rather than the file.
    let status = response.status();
    if !status.is_success() {
        bail!("Unexpected status {status} for '{dst_path}'");
    }
    let stream = response.bytes_stream();
    let mut reader =
        StreamReader::new(stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    tokio::io::copy(&mut reader, &mut writer).await?;

    if dst_path.extension() == Some(PARQUET_EXTENSION) {
        // Parses the footer, the same read that opens a chunk at query time, so a committed
        // file is one the read path can open. Page data is not decoded — the assignment
        // carries no size or digest to check the body itself against (GAP-5). Two small
        // reads of a file just written, so it is done inline like the other fs ops here.
        ParquetMetaDataReader::new()
            .parse_and_finish(&writer.into_std().await)
            .with_context(|| format!("Invalid parquet file '{dst_path}'"))?;
    }
    Ok(())
}

const PARQUET_EXTENSION: &str = "parquet";
