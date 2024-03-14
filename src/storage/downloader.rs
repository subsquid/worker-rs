use std::collections::HashMap;

use anyhow::{Context, Result};
use camino::Utf8PathBuf as PathBuf;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::types::state::ChunkRef;

use super::s3_fs::S3Filesystem;

pub struct ChunkDownloader {
    running: Mutex<HashMap<ChunkRef, CancellationToken>>,
}

impl ChunkDownloader {
    pub fn new() -> Self {
        Self {
            running: Default::default(),
        }
    }

    pub async fn download(&self, chunk: &ChunkRef, dst: PathBuf) -> Result<()> {
        let rfs = S3Filesystem::with_bucket(&chunk.dataset)?;

        let cancel_token = CancellationToken::new();
        self.running
            .lock()
            .insert(chunk.clone(), cancel_token.clone())
            .unwrap_or_else(|| panic!("Chunk {chunk} is already being downloaded"));

        let downloaded = rfs.download_dir(chunk.chunk.path(), dst);

        tokio::select! {
            result = downloaded => {
                self.running
                    .lock()
                    .remove(chunk);
                result.with_context(|| format!("Could not download chunk {:?}", chunk))?;
            }
            _ = cancel_token.cancelled_owned() => {
                self.running
                    .lock()
                    .remove(chunk);
                info!("Downloading chunk {chunk} cancelled");
            }
        };

        Ok(())
    }

    pub fn cancel(&self, chunk: &ChunkRef) {
        if let Some(cancel_token) = self.running.lock().get(chunk) {
            cancel_token.cancel();
        }
    }
}
