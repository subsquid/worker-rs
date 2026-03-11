use std::{env, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use camino::Utf8PathBuf as PathBuf;
use futures::{AsyncReadExt, AsyncWriteExt, Stream, StreamExt};
use parking_lot::RwLock;
use prost::Message;
use sqd_messages::{
    query_error, query_executed, BitString, LogsRequest, Query, QueryExecuted, QueryLogs,
    TimeReport, WorkerStatus,
};
use sqd_network_transport::{
    protocol, IncomingStreams, Keypair, P2PTransportBuilder, PeerId, QueueFull, ResponseChannel,
    Stream as P2PStream, WorkerConfig, WorkerEvent, WorkerTransportHandle,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn, Instrument};

use crate::{
    cli::Args,
    compute_units::{
        self,
        allocations_checker::{self, AllocationsChecker},
    },
    controller::worker::QueryType,
    logs_storage::LogsStorage,
    metrics,
    query::result::{QueryError, QueryResult},
    run_all,
    util::{timestamp_now_ms, UseOnce},
};

use super::worker::Worker;

const WORKER_VERSION: &str = env!("CARGO_PKG_VERSION");
const LOG_REQUESTS_QUEUE_SIZE: usize = 4;
const DEFAULT_BACKOFF: Duration = Duration::from_secs(1);
const LOGS_KEEP_DURATION: Duration = Duration::from_secs(3600 * 2);
const LOGS_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(60);
// TODO: find out why the margin is required
const MAX_LOGS_SIZE: usize =
    sqd_network_transport::protocol::MAX_LOGS_RESPONSE_SIZE as usize - 100 * 1024;

pub struct P2PController<EventStream> {
    worker: Arc<Worker>,
    worker_status: RwLock<WorkerStatus>,
    assignment_check_interval: Duration,
    assignment_fetch_timeout: Duration,
    assignment_fetch_max_delay: Duration,
    raw_event_stream: UseOnce<EventStream>,
    transport_handle: WorkerTransportHandle,
    logs_storage: LogsStorage,
    allocations_checker: AllocationsChecker,
    worker_id: PeerId,
    keypair: Keypair,
    assignment_url: String,
    query_streams: UseOnce<IncomingStreams>,
    sql_query_streams: UseOnce<IncomingStreams>,
    active_queries: AtomicUsize,
    max_queries: usize,
    log_requests_tx: mpsc::Sender<(LogsRequest, ResponseChannel<QueryLogs>)>,
    log_requests_rx: UseOnce<mpsc::Receiver<(LogsRequest, ResponseChannel<QueryLogs>)>>,
}

pub async fn create_p2p_controller(
    worker: Arc<Worker>,
    transport_builder: P2PTransportBuilder,
    args: Args,
) -> Result<P2PController<impl Stream<Item = WorkerEvent>>> {
    let worker_id = transport_builder.local_peer_id();
    let keypair = transport_builder.keypair();
    info!("Local peer ID: {worker_id}");
    check_peer_id(worker_id, args.data_dir.join("peer_id"));

    let worker_status = get_worker_status(&worker).await;

    let allocations_checker = allocations_checker::AllocationsChecker::new(
        transport_builder.contract_client(),
        worker_id,
        args.network_polling_interval,
    )
    .await?;

    let mut config = WorkerConfig::default();
    config.status_queue_size = std::env::var("WORKER_STATUS_QUEUE_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);
    let (event_stream, transport_handle, query_streams, sql_query_streams) =
        transport_builder.build_worker(config).await?;

    let (log_requests_tx, log_requests_rx) = mpsc::channel(LOG_REQUESTS_QUEUE_SIZE);

    Ok(P2PController {
        worker,
        worker_status: RwLock::new(worker_status),
        assignment_check_interval: args.assignment_check_interval,
        assignment_fetch_timeout: args.assignment_fetch_timeout,
        assignment_fetch_max_delay: args.assignment_fetch_max_delay,
        raw_event_stream: UseOnce::new(event_stream),
        transport_handle,
        logs_storage: LogsStorage::new(args.data_dir.join("logs.db").as_str()).await?,
        allocations_checker,
        worker_id,
        keypair,
        assignment_url: args.assignment_url,
        query_streams: UseOnce::new(query_streams),
        sql_query_streams: UseOnce::new(sql_query_streams),
        active_queries: AtomicUsize::new(0),
        max_queries: args.parallel_queries,
        log_requests_tx,
        log_requests_rx: UseOnce::new(log_requests_rx),
    })
}

impl<EventStream: Stream<Item = WorkerEvent> + Send + 'static> P2PController<EventStream> {
    pub async fn run(self: Arc<Self>, cancellation_token: CancellationToken) {
        let this = self.clone();
        let token = cancellation_token.child_token();
        let event_task = tokio::spawn(async move { this.run_event_loop(token).await });

        let this = self.clone();
        let token = cancellation_token.child_token();
        let query_incoming = this.query_streams.take().unwrap();
        let query_streams_task = tokio::spawn(async move {
            this.run_query_accept_loop(query_incoming, QueryType::PlainQuery, token)
                .await
        });

        let this = self.clone();
        let token = cancellation_token.child_token();
        let sql_query_incoming = this.sql_query_streams.take().unwrap();
        let sql_query_streams_task = tokio::spawn(async move {
            this.run_query_accept_loop(sql_query_incoming, QueryType::SqlQuery, token)
                .await
        });

        let this = self.clone();
        let token = cancellation_token.child_token();
        let assignments_task = tokio::spawn(async move {
            this.run_assignments_loop(token, this.assignment_check_interval)
                .await
        });

        let this = self.clone();
        let token = cancellation_token.child_token();
        let logs_task: tokio::task::JoinHandle<()> =
            tokio::spawn(async move { this.run_logs_loop(token).await });

        let this = self.clone();
        let token = cancellation_token.child_token();
        let logs_cleanup_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            this.run_logs_cleanup_loop(token, LOGS_CLEANUP_INTERVAL)
                .await
        });

        let this = self.clone();
        let token = cancellation_token.child_token();
        let status_update_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            this.run_status_update_loop(token, STATUS_UPDATE_INTERVAL)
                .await
        });

        let this = self.clone();
        let token = cancellation_token.child_token();
        let worker_task: tokio::task::JoinHandle<()> =
            tokio::spawn(async move { this.worker.run(token).await });

        let this = self.clone();
        let token = cancellation_token.child_token();
        let allocations_task: tokio::task::JoinHandle<()> =
            tokio::spawn(async move { this.allocations_checker.run(token).await });

        let _ = run_all!(
            cancellation_token,
            event_task,
            query_streams_task,
            sql_query_streams_task,
            assignments_task,
            logs_task,
            logs_cleanup_task,
            status_update_task,
            worker_task,
            allocations_task,
        );
    }

    /// Accept incoming query streams and spawn a task per stream.
    /// Uses an atomic counter to limit concurrent queries. When at capacity,
    /// incoming streams get an immediate ServiceOverloaded error response.
    async fn run_query_accept_loop(
        self: Arc<Self>,
        mut incoming: IncomingStreams,
        query_type: QueryType,
        cancellation_token: CancellationToken,
    ) {
        loop {
            tokio::select! {
                stream = incoming.next() => {
                    let Some((peer_id, mut stream)) = stream else { break };

                    if self.active_queries.fetch_add(1, Ordering::Relaxed) >= self.max_queries {
                        self.active_queries.fetch_sub(1, Ordering::Relaxed);
                        let _ = self.write_error_to_stream(
                            &mut stream,
                            "",
                            QueryError::ServiceOverloaded,
                            Some(DEFAULT_BACKOFF),
                        ).await;
                        continue;
                    }

                    let this = self.clone();
                    tokio::spawn(async move {
                        this.handle_query_stream(peer_id, stream, query_type).await;
                        this.active_queries.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                _ = cancellation_token.cancelled() => break,
            }
        }
        info!("Query accept loop finished (type: {query_type:?})");
    }

    /// Handle a single query stream: read request, process, write response.
    #[instrument(skip_all, fields(peer_id = %peer_id))]
    async fn handle_query_stream(
        &self,
        peer_id: PeerId,
        mut stream: P2PStream,
        query_type: QueryType,
    ) {
        // 1. Read the query from the stream (with size limit)
        let query = match read_query_from_stream(&mut stream).await {
            Ok(q) => q,
            Err(e) => {
                warn!("Failed to read query from {peer_id}: {e}");
                return;
            }
        };

        // Drop empty messages (same as old behaviour)
        if query == Query::default() {
            return;
        }

        // 2. Validate signature and timestamp
        if !self.validate_query(&query, peer_id) {
            // Write error response
            let _ = self
                .write_error_to_stream(
                    &mut stream,
                    &query.query_id,
                    QueryError::BadRequest("Invalid signature or timestamp".to_string()),
                    None,
                )
                .await;
            return;
        }

        let query_id = query.query_id.clone();
        let compression = query.compression();

        // 3. Process query (CU check + execution)
        let (mut result, retry_after) = self.process_query(peer_id, &query, query_type).await;
        if let Err(e) = &result {
            warn!("Query {query_id} by {peer_id} execution failed: {e:?}");
        }

        metrics::query_executed(&result);

        // 4. Build response message, compress, sign, and write to stream
        match self
            .build_and_write_response(
                &mut stream,
                query_id,
                result.clone(),
                retry_after,
                compression,
            )
            .await
        {
            Ok((compression_duration, signing_duration)) => {
                let _ = result.as_mut().map(|v| {
                    v.time_report.compression_time = compression_duration;
                    v.time_report.signing_time = signing_duration;
                });
            }
            Err(e) => {
                tracing::error!("Couldn't write query result to stream: {e:?}");
                return;
            }
        }

        // 5. Save query log
        if let Some(log) = self.generate_log(&result, query, peer_id).await {
            if log.encoded_len() > MAX_LOGS_SIZE {
                warn!("Query log is too big: {log:?}");
                return;
            }
            let result = self.logs_storage.save_log(log).await;
            if let Err(e) = result {
                warn!("Couldn't save query log: {e:?}");
            }
        }
    }

    /// Build a signed QueryResult protobuf and write it to the stream.
    /// Returns (compression_duration, signing_duration).
    #[instrument(skip_all)]
    async fn build_and_write_response(
        &self,
        stream: &mut P2PStream,
        query_id: String,
        result: QueryResult,
        retry_after: Option<Duration>,
        compression: sqd_messages::Compression,
    ) -> Result<(Duration, Duration)> {
        let compression_timer = std::time::Instant::now();
        let query_result = match result {
            Ok(result) => {
                let data = match compression {
                    sqd_messages::Compression::None => result.data,
                    sqd_messages::Compression::Gzip => result.data_gzip().await,
                    sqd_messages::Compression::Zstd => result.data_zstd().await,
                };
                sqd_messages::query_result::Result::Ok(sqd_messages::QueryOk {
                    data,
                    last_block: result.last_block,
                })
            }
            Err(e) => query_error::Err::from(e).into(),
        };
        let compression_duration = compression_timer.elapsed();

        let mut msg = sqd_messages::QueryResult {
            query_id,
            result: Some(query_result),
            retry_after_ms: retry_after.map(|duration| duration.as_millis() as u32),
            signature: Default::default(),
        };

        let signing_timer = std::time::Instant::now();
        tokio::task::block_in_place(|| msg.sign(&self.keypair).map_err(|e| anyhow!(e)))?;
        let signing_duration = signing_timer.elapsed();

        let result_size = msg.encoded_len() as u64;
        if result_size > protocol::MAX_QUERY_RESULT_SIZE {
            anyhow::bail!("query result size too large: {result_size}");
        }

        let bytes = msg.encode_to_vec();
        stream
            .write_all(&bytes)
            .await
            .map_err(|e| anyhow!("Failed to write response: {e}"))?;
        stream
            .close()
            .await
            .map_err(|e| anyhow!("Failed to close stream: {e}"))?;

        Ok((compression_duration, signing_duration))
    }

    /// Write an error response to the stream.
    async fn write_error_to_stream(
        &self,
        stream: &mut P2PStream,
        query_id: &str,
        error: QueryError,
        retry_after: Option<Duration>,
    ) -> Result<()> {
        let msg = build_error_response_message(
            query_id.to_string(),
            error,
            retry_after,
            &self.keypair,
        )?;
        let bytes = msg.encode_to_vec();
        stream
            .write_all(&bytes)
            .await
            .map_err(|e| anyhow!("Failed to write error response: {e}"))?;
        stream
            .close()
            .await
            .map_err(|e| anyhow!("Failed to close stream: {e}"))?;
        Ok(())
    }

    async fn run_assignments_loop(
        &self,
        cancellation_token: CancellationToken,
        assignment_check_interval: Duration,
    ) {
        super::assignments::new_assignments_stream(
            self.assignment_url.clone(),
            assignment_check_interval,
            self.assignment_fetch_timeout,
            self.assignment_fetch_max_delay,
            self.worker_id,
        )
        .take_until(cancellation_token.cancelled_owned())
        .for_each(|update| async move {
            let worker = self.worker.clone();
            let keypair = self.keypair.clone();
            let id = update.id.clone();
            tokio::task::spawn_blocking(move || {
                worker.register_assignment(update.assignment, update.id, &keypair);
            })
            .instrument(tracing::info_span!("set_assignment", id))
            .await
            .expect("register_assignment shouldn't panic");
        })
        .await;
        info!("Assignment processing task finished");
    }

    async fn run_logs_loop(&self, cancellation_token: CancellationToken) {
        let requests = self.log_requests_rx.take().unwrap();
        ReceiverStream::new(requests)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|(request, resp_chan)| async move {
                if let Err(e) = self.handle_logs_request(request, resp_chan).await {
                    warn!("Error handling logs request: {e:?}");
                }
            })
            .await;
        info!("Logs processing task finished");
    }

    async fn run_status_update_loop(
        &self,
        cancellation_token: CancellationToken,
        interval: Duration,
    ) {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(timer)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|_| async move {
                let status = get_worker_status(&self.worker).await;
                *self.worker_status.write() = status;
            })
            .await;
        info!("Status update task finished");
    }

    async fn run_logs_cleanup_loop(
        &self,
        cancellation_token: CancellationToken,
        cleanup_interval: Duration,
    ) {
        let mut timer = tokio::time::interval(cleanup_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(timer)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|_| async move {
                let timestamp = (std::time::SystemTime::now() - LOGS_KEEP_DURATION)
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Invalid current time")
                    .as_millis() as u64;
                self.logs_storage
                    .cleanup(timestamp)
                    .await
                    .unwrap_or_else(|e| {
                        warn!("Couldn't cleanup logs: {e:?}");
                    });
            })
            .await;
        info!("Logs cleanup task finished");
    }

    async fn run_event_loop(&self, cancellation_token: CancellationToken) {
        let event_stream = self
            .raw_event_stream
            .take()
            .unwrap()
            .take_until(cancellation_token.cancelled_owned());
        tokio::pin!(event_stream);

        while let Some(ev) = event_stream.next().await {
            match ev {
                WorkerEvent::LogsRequest { request, resp_chan } => {
                    match self.log_requests_tx.try_send((request, resp_chan)) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!("There are too many ongoing log requests");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            break;
                        }
                    }
                }
                WorkerEvent::StatusRequest { peer_id, resp_chan } => {
                    let status = self.worker_status.read().clone();
                    match self.transport_handle.send_status(status, resp_chan) {
                        Ok(_) => {}
                        Err(QueueFull) => {
                            warn!("Couldn't respond with status to {peer_id}: out queue full");
                        }
                    }
                }
            }
        }

        info!("Transport event loop finished");
    }

    #[instrument(skip_all)]
    fn validate_query(&self, query: &Query, peer_id: PeerId) -> bool {
        if query.query.len() as u64 > protocol::MAX_RAW_QUERY_SIZE {
            tracing::warn!(
                "Rejected query with body too large ({} bytes) from {}",
                query.query.len(),
                peer_id
            );
            return false;
        }
        if !query.verify_signature(peer_id, self.worker_id) {
            tracing::warn!("Rejected query with invalid signature from {}", peer_id);
            return false;
        }
        if query.timestamp_ms.abs_diff(timestamp_now_ms()) as u128
            > protocol::MAX_TIME_LAG.as_millis()
        {
            tracing::warn!(
                "Rejected query with invalid timestamp ({}) from {}",
                query.timestamp_ms,
                peer_id
            );
            return false;
        }
        true
    }

    /// Returns query result and the time to wait before sending the next query
    #[instrument(skip_all)]
    async fn process_query(
        &self,
        peer_id: PeerId,
        query: &Query,
        query_type: QueryType,
    ) -> (QueryResult, Option<Duration>) {
        match query.compression {
            c if c == sqd_messages::Compression::Gzip as i32
                || c == sqd_messages::Compression::Zstd as i32
                || c == sqd_messages::Compression::None as i32 => {}
            _ => {
                return (
                    Err(QueryError::BadRequest(
                        "Unsupported compression type".to_owned(),
                    )),
                    None,
                );
            }
        }

        let status = match self.allocations_checker.try_spend(peer_id) {
            compute_units::RateLimitStatus::NoAllocation => {
                return (Err(QueryError::NoAllocation), None)
            }
            status => status,
        };
        let mut retry_after = status.retry_after();

        let block_range = query
            .block_range
            .map(|sqd_messages::Range { begin, end }| (begin, end));
        let result = self
            .worker
            .run_query(
                &query.query,
                query.dataset.clone(),
                block_range,
                &query.chunk_id,
                Some(peer_id),
                query_type,
            )
            .await;

        if let Err(QueryError::ServiceOverloaded) = result {
            self.allocations_checker.refund(peer_id);
            retry_after = Some(DEFAULT_BACKOFF);
        }
        (result, retry_after)
    }

    #[instrument(skip_all)]
    async fn generate_log(
        &self,
        query_result: &QueryResult,
        query: Query,
        client_id: PeerId,
    ) -> Option<QueryExecuted> {
        use query_executed::Result;

        let result = match query_result {
            Ok(result) => Result::Ok(sqd_messages::QueryOkSummary {
                uncompressed_data_size: result.data.len() as u64,
                data_hash: result.sha3_256().await,
                last_block: result.last_block,
            }),
            Err(QueryError::NoAllocation) => return None,
            Err(e) => query_error::Err::from(e).into(),
        };

        let exec_time_report = match query_result {
            Ok(result) => Some(TimeReport {
                parsing_time_micros: result.time_report.parsing_time.as_micros() as u32,
                execution_time_micros: result.time_report.execution_time.as_micros() as u32,
                compression_time_micros: result.time_report.compression_time.as_micros() as u32,
                signing_time_micros: result.time_report.signing_time.as_micros() as u32,
                serialization_time_micros: result.time_report.serialization_time.as_micros() as u32,
            }),
            Err(_) => None, // TODO: always measure execution time
        };

        Some(QueryExecuted {
            client_id: client_id.to_string(),
            query: Some(query),
            exec_time_micros: exec_time_report
                .as_ref()
                .map_or(0, |report| report.execution_time_micros),
            exec_time_report,
            timestamp_ms: timestamp_now_ms(), // TODO: use time of receiving query
            result: Some(result),
            worker_version: WORKER_VERSION.to_string(),
        })
    }

    async fn handle_logs_request(
        &self,
        request: LogsRequest,
        resp_chan: ResponseChannel<QueryLogs>,
    ) -> Result<()> {
        let msg = self
            .logs_storage
            .get_logs(
                request.from_timestamp_ms,
                timestamp_now_ms() - protocol::MAX_TIME_LAG.as_millis() as u64,
                request.last_received_query_id,
                MAX_LOGS_SIZE,
            )
            .await?;
        info!(
            "Sending {} logs ({} bytes)",
            msg.queries_executed.len(),
            msg.encoded_len()
        );
        self.transport_handle.send_logs(msg, resp_chan)?;
        Ok(())
    }
}

/// Read a protobuf-encoded Query from a libp2p stream with size limit.
async fn read_query_from_stream(stream: &mut P2PStream) -> Result<Query> {
    let mut buf = Vec::new();
    let max_size = protocol::MAX_QUERY_MSG_SIZE;
    let bytes_read = stream
        .take(max_size + 1)
        .read_to_end(&mut buf)
        .await
        .map_err(|e| anyhow!("Failed to read from stream: {e}"))?;
    if bytes_read as u64 > max_size {
        anyhow::bail!("Query message too large ({bytes_read} bytes, max {max_size})");
    }
    let query = Query::decode(buf.as_slice())
        .map_err(|e| anyhow!("Failed to decode query: {e}"))?;
    Ok(query)
}

#[tracing::instrument(skip_all)]
async fn get_worker_status(worker: &Worker) -> sqd_messages::WorkerStatus {
    let status = worker.status().await;
    let assignment_id = status.assignment_id.unwrap_or_default();
    sqd_messages::WorkerStatus {
        assignment_id,
        missing_chunks: Some(BitString::new(&status.unavailability_map)),
        version: WORKER_VERSION.to_string(),
        stored_bytes: Some(status.stored_bytes),
    }
}

fn check_peer_id(peer_id: PeerId, filename: PathBuf) {
    use std::fs::File;
    use std::io::{Read, Write};

    if filename.exists() {
        let mut file = File::open(&filename).expect("Couldn't open peer_id file");
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("Couldn't read peer_id file");
        if contents.trim() != peer_id.to_string() {
            panic!(
                "Data dir {} is already used by peer ID {}",
                &filename.parent().unwrap(),
                contents.trim()
            );
        }
    } else {
        let mut file = File::create(&filename).expect("Couldn't create peer_id file");
        file.write_all(peer_id.to_string().as_bytes())
            .expect("Couldn't write peer_id file");
    }
}

/// Build a signed error-only `sqd_messages::QueryResult` protobuf message.
/// Used for immediate error responses from the event loop (NoAllocation, ServiceOverloaded).
pub fn build_error_response_message(
    query_id: String,
    error: QueryError,
    retry_after: Option<Duration>,
    keypair: &Keypair,
) -> Result<sqd_messages::QueryResult> {
    let query_result: sqd_messages::query_result::Result = query_error::Err::from(error).into();
    let mut msg = sqd_messages::QueryResult {
        query_id,
        result: Some(query_result),
        retry_after_ms: retry_after.map(|duration| duration.as_millis() as u32),
        signature: Default::default(),
    };
    tokio::task::block_in_place(|| msg.sign(keypair).map_err(|e| anyhow!(e)))?;
    Ok(msg)
}
