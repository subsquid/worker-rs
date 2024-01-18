use anyhow::{anyhow, Result};
use futures::StreamExt;
use itertools::Itertools;
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, instrument, Instrument};

use super::{s3_fs::S3Filesystem, Filesystem};

/// Handles limited background download pool and allows scheduling new chunk downloads
// TODO: prioritize short jobs over long ones
#[derive(Debug)]
pub struct Downloader {
    tx: tokio::sync::mpsc::Sender<Job>,
}

struct Job {
    connection: S3Filesystem,
    from: String,
    to: PathBuf,
    resp_tx: oneshot::Sender<Result<()>>,
    parent_span: tracing::Span,
}

impl Downloader {
    #[instrument(name = "downloader_process")]
    pub fn new(concurrency: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(concurrency * 2);
        tokio::spawn(
            async move {
                ReceiverStream::new(rx)
                    .for_each_concurrent(concurrency, |job: Job| async move {
                        let span = tracing::trace_span!("download_job");
                        span.follows_from(job.parent_span);
                        let result = job
                            .connection
                            .download_one(&job.from, &job.to)
                            .instrument(span)
                            .await;
                        job.resp_tx
                            .send(result)
                            .expect("Download result could not be sent to the channel");
                    })
                    .await;
            }
            .in_current_span(),
        );
        Self { tx }
    }

    /// Starts download in the background pool and completes when it's done
    #[instrument(err, ret, skip(self, rfs))]
    pub async fn download_dir(&self, rfs: &S3Filesystem, src: String, dst: PathBuf) -> Result<()> {
        let tmp = &add_temp_prefix(&dst)?;
        tokio::fs::create_dir_all(&tmp).await?;
        let files = rfs.ls(&src).await?;
        info!("Scheduling download: {:?}", files);
        let results = futures::future::join_all(files.into_iter().map(|file| async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            let dst_file = tmp.join(
                PathBuf::from(&file)
                    .file_name()
                    .unwrap_or_else(|| panic!("Couldn't parse S3 file name: '{}'", &file)),
            );
            self.tx
                .send(Job {
                    connection: rfs.clone(),
                    from: file,
                    to: dst_file,
                    resp_tx,
                    parent_span: tracing::Span::current(),
                })
                .await
                .expect("Downloader routine died");
            resp_rx.await.expect("Downloader dropped without returning")
        }))
        .await;
        results.into_iter().try_collect()?;
        tokio::fs::rename(tmp, dst).await?;
        Ok(())
    }
}

fn add_temp_prefix(path: &Path) -> Result<PathBuf> {
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
