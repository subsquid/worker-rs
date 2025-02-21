use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};

use anyhow::Result;

pub mod chunk_ordinals;
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
pub mod tests {
    use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
    use std::collections::HashMap;

    use anyhow::Context;

    use super::Filesystem;

    pub struct TestFilesystem {
        pub files: HashMap<PathBuf, Vec<PathBuf>>,
    }

    impl Filesystem for TestFilesystem {
        async fn ls_root(&self) -> anyhow::Result<Vec<PathBuf>> {
            Ok(self.files.keys().cloned().collect())
        }

        async fn ls(&self, path: impl AsRef<Path>) -> anyhow::Result<Vec<PathBuf>> {
            self.files
                .get(path.as_ref())
                .cloned()
                .with_context(|| format!("Couldn't find top dir {}", path.as_ref()))
        }
    }
}
