use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use futures::{future::FusedFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use reqwest::Url;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::types::state::ChunkRef;

use super::{
    datasets_index::{DatasetsIndex, RemoteFile},
    guard::FsGuard,
    local_fs::add_temp_prefix,
};

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
        let headers = datasets_index.get_headers();
        let client = reqwest::ClientBuilder::new()
            .default_headers(headers.clone())
            .build()
            .expect("Can't create HTTP client");
        self.futures.push(tokio::spawn(async move {
            tokio::select! {
                result = download_dir(files, dst, &client) => {
                    (chunk, result)
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
async fn download_dir(
    files: Vec<RemoteFile>,
    dst_dir: PathBuf,
    client: &reqwest::Client,
) -> Result<()> {
    let tmp = &add_temp_prefix(&dst_dir)?;
    let mut guard = FsGuard::new(tmp)?;
    futures::future::try_join_all(files.into_iter().map(|file| async move {
        let dst_file = tmp.join(file.name.parse::<PathBuf>()?);
        download_one(file.url, &dst_file, client).await
    }))
    .await?;
    guard.persist(dst_dir)?;
    Ok(())
}

#[instrument(skip_all)]
pub async fn download_one(url: Url, dst_path: &Path, client: &reqwest::Client) -> Result<()> {
    let mut writer = tokio::fs::File::create(dst_path)
        .await
        .with_context(|| format!("Couldn't create file '{dst_path}'"))?;
    let response = client.get(url).send().await?.error_for_status()?;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        writer.write_all(&chunk).await?;
    }
    Ok(())
}
