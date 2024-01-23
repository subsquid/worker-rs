use anyhow::Result;

pub mod downloader;
pub mod layout;
pub mod local_fs;
pub mod manager;
pub mod s3_fs;

pub trait Filesystem {
    // Returning a collection instead of iterator because partial results are useless
    async fn ls(&self, path: &str) -> Result<Vec<String>>;
    async fn ls_root(&self) -> Result<Vec<String>> {
        self.ls("").await
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use anyhow::Context;

    use super::Filesystem;

    pub struct TestFilesystem {
        pub files: HashMap<String, Vec<String>>,
    }

    impl Filesystem for TestFilesystem {
        async fn ls_root(&self) -> anyhow::Result<Vec<String>> {
            Ok(self.files.keys().cloned().collect())
        }

        async fn ls(&self, path: &str) -> anyhow::Result<Vec<String>> {
            self.files
                .get(path)
                .cloned()
                .with_context(|| format!("Couldn't find top dir {}", path))
        }
    }
}
