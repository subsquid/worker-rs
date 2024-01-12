use anyhow::Result;

mod local_fs;
mod s3_fs;

trait Filesystem {
    // Returning a collection instead of iterator because partial results are useless
    fn ls(&self, path: &str) -> Result<Vec<String>>;
}
