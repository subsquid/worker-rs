use anyhow::{Context, Result};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use lazy_static::lazy_static;
use s3::{creds::Credentials, Bucket};
use std::{env, time::Duration};
use tracing::{info, instrument};

use super::local_fs::add_temp_prefix;
use super::{guard::FsGuard, Filesystem};

lazy_static! {
    static ref AWS_REGION: String = env::var("AWS_REGION").unwrap_or("auto".to_owned());
    static ref AWS_ENDPOINT: String =
        env::var("AWS_S3_ENDPOINT").expect("AWS_S3_ENDPOINT not found");
    static ref REGION: s3::Region = s3::Region::Custom {
        region: AWS_REGION.to_owned(),
        endpoint: AWS_ENDPOINT.to_owned(),
    };
    static ref TIMEOUT: u64 = env::var("S3_TIMEOUT")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(10);
}

#[derive(Debug, Clone)]
pub struct S3Filesystem {
    pub bucket: Bucket,
}

impl S3Filesystem {
    pub fn with_bucket(name: &str) -> Result<Self> {
        let name = name.strip_prefix("s3://").unwrap_or(name);
        let bucket = Bucket::new(name, REGION.to_owned(), Credentials::from_env()?)?
            .with_request_timeout(Duration::from_secs(TIMEOUT.to_owned()));
        Ok(Self { bucket })
    }

    #[instrument(skip_all)]
    pub async fn download_one(&self, path: &str, dst_path: &Path) -> Result<()> {
        // TODO: check resulting file size
        let mut writer = tokio::fs::File::create(dst_path)
            .await
            .with_context(|| format!("Couldn't create file '{dst_path}'"))?;
        self.bucket.get_object_to_writer(path, &mut writer).await?;
        Ok(())
    }

    /// Either downloads the entire directory or nothing at all.
    /// This function is cancel-safe. If it is not awaited until the end,
    /// it will clean up temporary results.
    ///
    /// Careful: this function never removes any parent dirs so it can produce
    /// a dangling empty dir after cleanup.
    #[instrument(skip_all)]
    pub async fn download_dir(&self, src: String, dst: PathBuf) -> Result<()> {
        let tmp = &add_temp_prefix(&dst)?;
        let files = self.ls(&src).await?;
        let mut guard = FsGuard::new(tmp)?;
        futures::future::try_join_all(files.into_iter().map(|file| async move {
            let dst_file = tmp.join(
                file.file_name()
                    .unwrap_or_else(|| panic!("Couldn't parse S3 file name: '{file}'")),
            );
            self.download_one(file.as_str(), &dst_file)
                .await
        }))
        .await?;
        guard.persist(dst)?;
        Ok(())
    }

    #[instrument(skip(self), name = "ls", level = "debug")]
    async fn ls_raw(&self, path: String) -> Result<Vec<PathBuf>> {
        let list = self.bucket.list(path, Some("/".to_owned())).await?;
        Ok(list
            .into_iter()
            .flat_map(|item| {
                let dirs =
                    item.common_prefixes
                        .unwrap_or_default()
                        .into_iter()
                        .map(|x| {
                            PathBuf::from(x.prefix.strip_suffix('/').unwrap_or_else(|| {
                                panic!("Unexpected S3 prefix name: '{}'", x.prefix)
                            }))
                        });
                let files = item.contents.into_iter().map(|x| PathBuf::from(x.key));
                dirs.chain(files)
            })
            .collect())
    }
}

impl Filesystem for S3Filesystem {
    async fn ls(&self, path: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
        self.ls_raw(format!("{}/", path.as_ref())).await
    }

    async fn ls_root(&self) -> Result<Vec<PathBuf>> {
        self.ls_raw(String::from("")).await
    }
}
