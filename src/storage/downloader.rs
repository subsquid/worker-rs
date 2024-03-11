use std::collections::HashMap;

use anyhow::{anyhow, Result};
use camino::Utf8PathBuf as PathBuf;
use futures::{future::FusedFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::types::state::ChunkRef;

use super::s3_fs::S3Filesystem;

#[derive(Default)]
pub struct ChunkDownloader {
    futures: FuturesUnordered<tokio::task::JoinHandle<(ChunkRef, Result<()>)>>,
    cancel_tokens: HashMap<ChunkRef, CancellationToken>,
}

impl ChunkDownloader {
    pub fn start_download(&mut self, chunk: ChunkRef, dst: PathBuf) {
        let cancel_token = CancellationToken::new();

        let previous = self
            .cancel_tokens
            .insert(chunk.clone(), cancel_token.clone());
        if previous.is_some() {
            panic!("Chunk {chunk} is already being downloaded");
        }

        self.futures.push(tokio::spawn(async move {
            let rfs = match S3Filesystem::with_bucket(&chunk.dataset) {
                Ok(rfs) => rfs,
                Err(e) => return (chunk, Err(e)),
            };
            tokio::select! {
                result = rfs.download_dir(chunk.chunk.path(), dst) => {
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
