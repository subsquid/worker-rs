use std::ffi::OsString;

use anyhow::anyhow;
use anyhow::Context;

use super::Filesystem;
use super::Result;

struct LocalFs {}

impl Filesystem for LocalFs {
    fn ls(&self, path: &str) -> Result<Vec<String>> {
        std::fs::read_dir(path)
            .with_context(|| format!("Couldn't open dir {}", path))?
            .map(|entry| try_into_string(entry?.file_name()))
            .collect()
    }
}

fn try_into_string(s: OsString) -> Result<String> {
    s.into_string()
        .map_err(|os_str| anyhow!("Couldn't convert filename {:?} into utf-8 string", os_str))
}
