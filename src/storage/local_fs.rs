use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use anyhow::Context;

use super::Filesystem;
use super::Result;

#[derive(Debug)]
pub struct LocalFs {
    pub root: PathBuf,
}

impl LocalFs {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

impl Default for LocalFs {
    fn default() -> Self {
        Self { root: ".".into() }
    }
}

impl Filesystem for LocalFs {
    async fn ls(&self, path: &str) -> Result<Vec<String>> {
        let dir = self.root.join(path);
        std::fs::read_dir(&dir)
            .with_context(|| format!("Couldn't open dir {}", dir.display()))?
            .map(|entry| try_into_string(entry?.path().into_os_string()))
            .collect()
    }
}

impl LocalFs {
    pub fn cd(&self, path: impl AsRef<Path>) -> Self {
        LocalFs {
            root: self.root.join(path),
        }
    }
}

pub fn add_temp_prefix(path: &Path) -> Result<PathBuf> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Invalid system time")
        .as_millis();
    let result: Option<_> = (|| {
        let name = path.file_name()?.to_str()?;
        let new_name = format!("temp-{}-{}", timestamp, name);
        Some(path.parent()?.join(new_name))
    })();
    result.ok_or_else(|| anyhow!("Invalid chunk path: {}", path.to_string_lossy()))
}

fn try_into_string(s: OsString) -> Result<String> {
    s.into_string()
        .map_err(|os_str| anyhow!("Couldn't convert filename {:?} into utf-8 string", os_str))
}
