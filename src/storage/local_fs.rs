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
    async fn ls(&self, path: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
        let dir = self.root.join(path);
        std::fs::read_dir(&dir)
            .with_context(|| format!("Couldn't open dir {}", dir.display()))?
            .map(|entry| Ok(entry?.path()))
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
        Some(path.with_file_name(new_name))
    })();
    result.ok_or_else(|| anyhow!("Invalid chunk path: {}", path.to_string_lossy()))
}

#[cfg(test)]
mod tests {
    use super::LocalFs;
    use crate::storage::Filesystem;
    use crate::util::tests::tests_data;

    #[tokio::test]
    async fn test_fs() {
        let tests_data = tests_data();
        let fs = LocalFs::new(tests_data.clone());
        assert_eq!(fs.ls_root().await.unwrap(), [tests_data.join("0017881390")]);
        assert_eq!(
            fs.ls("0017881390").await.unwrap(),
            [tests_data.join("0017881390/0017881390-0017882786-32ee9457")]
        );
        assert_eq!(
            fs.cd("0017881390")
                .ls("0017881390-0017882786-32ee9457")
                .await
                .unwrap()
                .iter()
                .map(|p| p.file_name().unwrap())
                .collect::<Vec<_>>(),
            [
                "blocks.parquet",
                "statediffs.parquet",
                "traces.parquet",
                "transactions.parquet",
                "logs.parquet"
            ]
        );
    }
}
