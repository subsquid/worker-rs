use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};

use anyhow::Result;

pub mod datasets_index;
pub mod downloader;
pub mod guard;
pub mod layout;
pub mod local_fs;
pub mod manager;
pub mod state;

#[allow(async_fn_in_trait)]
pub trait Filesystem {
    // Returning a collection instead of iterator because partial results are useless
    async fn ls(&self, path: impl AsRef<Path>) -> Result<Vec<PathBuf>>;
    async fn ls_root(&self) -> Result<Vec<PathBuf>> {
        self.ls("").await
    }
}

#[cfg(test)]
pub mod tests;
