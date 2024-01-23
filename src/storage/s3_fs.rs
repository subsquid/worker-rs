use anyhow::{Context, Result};
use lazy_static::lazy_static;
use s3::{creds::Credentials, Bucket};
use std::{env, path::Path, time::Duration};
use tracing::instrument;

use super::Filesystem;

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
        let bucket = Bucket::new(name, REGION.to_owned(), Credentials::from_env()?)?
            .with_request_timeout(Duration::from_secs(TIMEOUT.to_owned()));
        Ok(Self { bucket })
    }

    #[instrument(err, skip(self))]
    pub async fn download_one(&self, path: &str, dst_path: &Path) -> Result<()> {
        // TODO: check resulting file size
        let mut writer = tokio::fs::File::create(dst_path)
            .await
            .with_context(|| format!("Couldn't create file '{}'", dst_path.to_string_lossy()))?;
        self.bucket.get_object_to_writer(path, &mut writer).await?;
        Ok(())
    }

    #[instrument(err, ret, skip(self), name = "ls", level = "debug")]
    async fn ls_raw(&self, path: String) -> Result<Vec<String>> {
        let list = self.bucket.list(path, Some("/".to_owned())).await?;
        Ok(list
            .into_iter()
            .flat_map(|item| {
                let dirs = item
                    .common_prefixes
                    .unwrap_or_default()
                    .into_iter()
                    .map(|x| {
                        x.prefix
                            .strip_suffix('/')
                            .unwrap_or_else(|| panic!("Unexpected S3 prefix name: '{}'", x.prefix))
                            .to_owned()
                    });
                let files = item.contents.into_iter().map(|x| x.key);
                dirs.chain(files)
            })
            .collect())
    }
}

impl Filesystem for S3Filesystem {
    async fn ls(&self, path: &str) -> Result<Vec<String>> {
        self.ls_raw(format!("{}/", path)).await
    }

    async fn ls_root(&self) -> Result<Vec<String>> {
        self.ls_raw(String::from("")).await
    }
}
