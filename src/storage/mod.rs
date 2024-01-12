use anyhow::Result;

mod layout;
mod local_fs;
mod s3_fs;

trait Filesystem {
    // Returning a collection instead of iterator because partial results are useless
    fn ls(&self, path: &str) -> Result<Vec<String>>;
    fn ls_root(&self) -> Result<Vec<String>>;
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
        fn ls_root(&self) -> anyhow::Result<Vec<String>> {
            Ok(self.files.keys().cloned().collect())
        }

        fn ls(&self, path: &str) -> anyhow::Result<Vec<String>> {
            self.files
                .get(path)
                .cloned()
                .with_context(|| format!("Couldn't find top dir {}", path))
        }
    }
}
