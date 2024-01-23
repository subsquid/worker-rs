use anyhow::{anyhow, Result};
use futures::StreamExt;
use itertools::Itertools;
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, instrument, Instrument};

use super::{s3_fs::S3Filesystem, Filesystem};
use super::local_fs::add_temp_prefix;

/// Handles limited background download pool and allows scheduling new chunk downloads
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
    #[instrument(err, ret, skip(self))]
    pub async fn download_dir(&self, bucket: &str, src: String, dst: PathBuf) -> Result<()> {
        let rfs = &S3Filesystem::with_bucket(bucket)?;
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

