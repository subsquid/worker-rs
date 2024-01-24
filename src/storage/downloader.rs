use anyhow::Result;
use futures::StreamExt;
use std::path::PathBuf;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, instrument, Instrument};

use crate::types::os_str::try_into_string;

use super::local_fs::add_temp_prefix;
use super::{s3_fs::S3Filesystem, Filesystem};

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
        let files = rfs.ls(&src).await?;
        info!("Scheduling download: {:?}", files);
        tokio::fs::create_dir_all(&tmp).await?;
        let results = futures::future::join_all(files.into_iter().map(|file| async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            let dst_file = tmp
                .join(file.file_name().unwrap_or_else(|| {
                    panic!("Couldn't parse S3 file name: '{}'", file.display())
                }));
            self.tx
                .send(Job {
                    connection: rfs.clone(),
                    from: try_into_string(file.into_os_string())?,
                    to: dst_file,
                    resp_tx,
                    parent_span: tracing::Span::current(),
                })
                .await
                .expect("Downloader routine died");
            resp_rx.await.expect("Downloader dropped without returning")
        }))
        .await;
        match results.into_iter().collect::<Result<Vec<()>>>() {
            Ok(_) => tokio::fs::rename(tmp, dst).await?,
            Err(e) => {
                tokio::fs::remove_dir_all(tmp)
                    .await
                    .expect("Couldn't clean up temp dir");
                return Err(e);
            }
        }
        Ok(())
    }
}
