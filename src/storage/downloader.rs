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
        let cancel_token = CancellationToken::new();
        self.running
            .lock()
            .insert(chunk.clone(), cancel_token.clone())
            .map(|_| panic!("Chunk {chunk} is already being downloaded"));

        let rfs = S3Filesystem::with_bucket(&chunk.dataset)?;
        let downloaded = rfs.download_dir(chunk.chunk.path(), dst);

        tokio::select! {
            result = downloaded => {
                result.with_context(|| format!("Could not download chunk {:?}", chunk))?;
            }
            _ = cancel_token.cancelled_owned() => {
                info!("Downloading chunk {chunk} cancelled");
            }
        };

        Ok(())
    }

    pub fn cancel(&self, chunk: &ChunkRef) {
        self.running
            .lock()
            .get(chunk)
            .map(|cancel_token| cancel_token.cancel());
    }
}
