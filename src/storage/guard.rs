use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};

pub struct FsGuard {
    path: Option<PathBuf>,
}

impl FsGuard {
    /// Creates a new dir that will be cleaned up when the guard is dropped
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if path.exists() {
            Err(anyhow!("Couldn't create new dir '{path}': path exists"))
        } else {
            std::fs::create_dir_all(&path)
                .with_context(|| format!("Couldn't create new dir '{path}'"))?;
            Ok(Self { path: Some(path) })
        }
    }

    /// Takes ownership of the existing directory.
    /// It is the caller responsibility to ensure that no other `FsGuard` is owning the same directory.
    pub fn _own(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if path.exists() {
            Ok(Self { path: Some(path) })
        } else {
            Err(anyhow!("Directory not found: '{path}'"))
        }
    }

    pub fn persist(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let current = self.path.as_ref().ok_or_else(|| {
            anyhow!(
                "Trying to persist already released dir to '{}'",
                path.as_ref()
            )
        })?;
        std::fs::rename(current, path.as_ref())
            .with_context(|| format!("Couldn't move dir to '{}'", path.as_ref()))?;
        self.release();
        Ok(())
    }

    pub fn release(&mut self) {
        self.path = None;
    }
}

impl Drop for FsGuard {
    fn drop(&mut self) {
        if let Some(path) = self.path.as_ref() {
            let result = std::fs::remove_dir_all(path);
            if let Err(e) = result {
                tracing::warn!("Couldn't remove dir '{path}' on cleanup: {e}");
            }
        }
    }
}
