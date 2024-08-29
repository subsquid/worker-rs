use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_process::Command;
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use futures::{future::FusedFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use reqwest::header::HeaderMap;
use reqwest::Url;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};

use crate::types::state::ChunkRef;

use super::{
    datasets_index::{DatasetsIndex, RemoteFile},
    guard::FsGuard,
    local_fs::add_temp_prefix,
};

lazy_static::lazy_static! {
    static ref S3_TIMEOUT: std::time::Duration = std::env::var("S3_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(60));
    static ref S3_READ_TIMEOUT: std::time::Duration = std::env::var("S3_READ_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(3));
}

#[derive(Default)]
pub struct ChunkDownloader {
    futures: FuturesUnordered<tokio::task::JoinHandle<(ChunkRef, Result<()>)>>,
    cancel_tokens: HashMap<ChunkRef, CancellationToken>,
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
        self.futures.push(tokio::spawn(async move {
            tokio::select! {
                result = download_dir(files, dst, headers) => {
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
async fn download_dir(files: Vec<RemoteFile>, dst_dir: PathBuf, headers: HeaderMap) -> Result<()> {
    let tmp = &add_temp_prefix(&dst_dir)?;
    let mut guard = FsGuard::new(tmp)?;
    futures::future::try_join_all(files.into_iter().map(|file| {
        let headers = headers.clone();
        async move {
            let dst_file = tmp.join(file.name.parse::<PathBuf>()?);
            download_one(file.url, &dst_file, headers.clone()).await
        }
    }))
    .await?;
    guard.persist(dst_dir)?;
    Ok(())
}

#[instrument(skip_all)]
pub async fn download_one(url: Url, dst_path: &Path, headers: HeaderMap) -> Result<()> {
    let mut command = Command::new("curl");
    command
        .arg("-sfL")
        .args(headers.into_iter().flat_map(|(header_name, header_value)| {
            let header_value = header_value.to_str().expect("invalid header").to_string();
            let header_arg = match header_name {
                None => header_value,
                Some(name) => format!("{name}: {header_value}"),
            };
            ["--header".to_string(), header_arg]
        }))
        .arg("--max-time")
        .arg(S3_READ_TIMEOUT.as_secs().to_string())
        .arg("--output")
        .arg(dst_path)
        .arg(url.as_str());
    debug!("Running {command:?}");
    let exit_status = command.spawn()?.status().await?;
    anyhow::ensure!(exit_status.success(), "Download failed");
    Ok(())
}
