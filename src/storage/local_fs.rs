use std::ffi::OsString;
use std::path::PathBuf;

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
        std::fs::read_dir(self.root.join(path))
            .with_context(|| format!("Couldn't open dir {}", path))?
            .map(|entry| try_into_string(entry?.path().into_os_string()))
            .collect()
    }

    async fn ls_root(&self) -> Result<Vec<String>> {
        self.ls(&try_into_string(self.root.as_os_str().to_owned())?)
            .await
    }
}

fn try_into_string(s: OsString) -> Result<String> {
    s.into_string()
        .map_err(|os_str| anyhow!("Couldn't convert filename {:?} into utf-8 string", os_str))
}
