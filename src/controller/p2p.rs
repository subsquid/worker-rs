use std::collections::VecDeque;
use std::{env, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use camino::Utf8PathBuf as PathBuf;
use futures::{FutureExt, Stream, StreamExt};
use parking_lot::RwLock;
use sqd_messages::{
    query_error, query_executed, BitString, LogsRequest, ProstMsg, Query, QueryExecuted, QueryLogs,
    TimeReport, WorkerStatus,
};
use sqd_network_transport::{
    protocol, Keypair, P2PTransportBuilder, PeerId, QueueFull, ResponseChannel, WorkerConfig,
    WorkerEvent, WorkerTransportHandle,
};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn, Instrument};

use crate::{
    cli::Args,
    compute_units::{
        allocations_checker::{self, AllocationsChecker},
        RateLimitStatus,
    },
    controller::worker::QueryType,
    logs_storage::LogsStorage,
    metrics,
    query::result::{QueryError, QueryResult},
    run_all,
    storage::layout::DataChunk,
    util::{timestamp_now_ms, UseOnce},
};

use super::query_deps::{CuChecker, QueryRunner};
use super::worker::Worker;

const WORKER_VERSION: &str = env!("CARGO_PKG_VERSION");
const LOG_REQUESTS_QUEUE_SIZE: usize = 4;
const QUERIES_POOL_SIZE: usize = 16;
const CONCURRENT_QUERY_MESSAGES: usize = 32;
const DEFAULT_BACKOFF: Duration = Duration::from_secs(1);
const LOGS_KEEP_DURATION: Duration = Duration::from_secs(3600 * 2);
const LOGS_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(60);
const MAX_PENDING_ASSIGNMENTS: usize = 5;
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
    queries_tx: mpsc::Sender<(PeerId, Query, ResponseChannel<sqd_messages::QueryResult>)>,
    queries_rx:
        UseOnce<mpsc::Receiver<(PeerId, Query, ResponseChannel<sqd_messages::QueryResult>)>>,
    sql_queries_tx: mpsc::Sender<(PeerId, Query, ResponseChannel<sqd_messages::QueryResult>)>,
    sql_queries_rx:
        UseOnce<mpsc::Receiver<(PeerId, Query, ResponseChannel<sqd_messages::QueryResult>)>>,
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

    let allocations_checker = allocations_checker::AllocationsChecker::new(
        transport_builder.contract_client(),
        worker_id,
        args.network_polling_interval,
    )
    .await?;

    let worker_status = get_worker_status(&worker, allocations_checker.current_epoch()).await;

    let mut config = WorkerConfig::default();
    config.status_queue_size = std::env::var("WORKER_STATUS_QUEUE_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);
    let (event_stream, transport_handle) = transport_builder.build_worker(config).await?;

    let (queries_tx, queries_rx) = mpsc::channel(QUERIES_POOL_SIZE);
    let (sql_queries_tx, sql_queries_rx) = mpsc::channel(QUERIES_POOL_SIZE);
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
        queries_tx,
        queries_rx: UseOnce::new(queries_rx),
        sql_queries_tx,
        sql_queries_rx: UseOnce::new(sql_queries_rx),
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
        let queries_task = tokio::spawn(async move { this.run_queries_loop(token).await });

        let this = self.clone();
        let token = cancellation_token.child_token();
        let sql_queries_task = tokio::spawn(async move { this.run_sql_queries_loop(token).await });

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
            queries_task,
            sql_queries_task,
            assignments_task,
            logs_task,
            logs_cleanup_task,
            status_update_task,
            worker_task,
            allocations_task,
        );
    }

    async fn run_queries_loop(self: Arc<Self>, cancellation_token: CancellationToken) {
        let queries_rx = self.queries_rx.take().unwrap();
        ReceiverStream::new(queries_rx)
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(CONCURRENT_QUERY_MESSAGES, |(peer_id, query, resp_chan)| {
                let this = self.clone();
                tokio::spawn(async move {
                    this.handle_query(peer_id, query, resp_chan, QueryType::PlainQuery)
                        .await;
                })
                .map(|r| r.unwrap())
            })
            .await;
        info!("Query processing task finished");
    }

    async fn run_sql_queries_loop(self: Arc<Self>, cancellation_token: CancellationToken) {
        let sql_queries_rx = self.sql_queries_rx.take().unwrap();
        ReceiverStream::new(sql_queries_rx)
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(CONCURRENT_QUERY_MESSAGES, |(peer_id, query, resp_chan)| {
                let this = self.clone();
                tokio::spawn(async move {
                    this.handle_query(peer_id, query, resp_chan, QueryType::SqlQuery)
                        .await;
                })
                .map(|r| r.unwrap())
            })
            .await;
        info!("SQL Query processing task finished");
    }

    async fn run_assignments_loop(
        &self,
        cancellation_token: CancellationToken,
        assignment_check_interval: Duration,
    ) {
        let mut assignments = Box::pin(
            super::assignments::new_assignments_stream(
                self.assignment_url.clone(),
                assignment_check_interval,
                self.assignment_fetch_timeout,
                self.assignment_fetch_max_delay,
                self.worker_id,
            )
            .take_until(cancellation_token.clone().cancelled_owned())
            .fuse(),
        );
        let assignment_client =
            super::assignments::new_reqwest_client(self.assignment_fetch_timeout, self.worker_id);
        let mut pending: VecDeque<super::assignments::AssignmentUpdate> = VecDeque::new();
        #[cfg(feature = "mvcc-chunks")]
        let mut processing_id: Option<String> = None;
        #[cfg(not(feature = "mvcc-chunks"))]
        let processing_id: Option<String> = None;

        'assignments: loop {
            if processing_id.is_none() {
                if let Some(update) = pending.pop_front() {
                    tracing::debug!("Downloading assignment \"{}\"", update.id);
                    let assignment = match super::assignments::fetch_assignment(
                        &update.fb_url_v1,
                        &assignment_client,
                    )
                    .await
                    {
                        Ok(assignment) => assignment,
                        Err(e) => {
                            warn!(assignment_id = %update.id, error = %e, "Failed to download assignment");
                            let retry_delay = tokio::time::sleep(DEFAULT_BACKOFF);
                            tokio::pin!(retry_delay);
                            let mut skip_retry = false;
                            loop {
                                tokio::select! {
                                    next_update = assignments.next() => {
                                        let Some(next_update) = next_update else {
                                            break 'assignments;
                                        };
                                        if push_pending_assignment(&mut pending, next_update) {
                                            skip_retry = true;
                                        }
                                    }
                                    _ = &mut retry_delay => break,
                                    _ = cancellation_token.cancelled() => break 'assignments,
                                }
                            }
                            if !skip_retry {
                                requeue_pending_assignment(&mut pending, update);
                            }
                            continue;
                        }
                    };
                    tracing::debug!("Downloaded assignment \"{}\"", update.id);

                    let worker = self.worker.clone();
                    let keypair = self.keypair.clone();
                    let id = update.id.clone();
                    let registered = tokio::task::spawn_blocking(move || {
                        worker.register_assignment(assignment, update.id, &keypair)
                    })
                    .instrument(tracing::info_span!("set_assignment", id))
                    .await
                    .expect("register_assignment shouldn't panic");

                    #[cfg(feature = "mvcc-chunks")]
                    if registered {
                        processing_id = Some(id);
                    }
                    #[cfg(not(feature = "mvcc-chunks"))]
                    let _ = registered;

                    continue;
                }
            }

            #[cfg(feature = "mvcc-chunks")]
            match processing_id.clone() {
                Some(id) => {
                    tokio::select! {
                        update = assignments.next() => {
                            let Some(update) = update else {
                                break;
                            };
                            if push_pending_assignment(&mut pending, update) {
                                warn!(assignment_id = %id, "Skipping current assignment because assignment queue exceeded {MAX_PENDING_ASSIGNMENTS}");
                                processing_id = None;
                            }
                        }
                        applied = self.worker.wait_until_assignment_applied(&id, cancellation_token.clone()) => {
                            if !applied {
                                break;
                            }
                            processing_id = None;
                        }
                    }
                }
                None => {
                    tokio::select! {
                        update = assignments.next() => {
                            let Some(update) = update else {
                                break;
                            };
                            push_pending_assignment(&mut pending, update);
                        }
                        _ = cancellation_token.cancelled() => break,
                    }
                }
            }

            #[cfg(not(feature = "mvcc-chunks"))]
            tokio::select! {
                update = assignments.next() => {
                    let Some(update) = update else {
                        break;
                    };
                    push_pending_assignment(&mut pending, update);
                }
                _ = cancellation_token.cancelled() => break,
            }
        }
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
        // Refresh on the periodic timer and, additionally, when an assignment
        // becomes fully applied (prompt last_applied_assignment_id, via the
        // existing applied signal).
        #[cfg(feature = "mvcc-chunks")]
        let mut assignment_applied = Some(self.worker.subscribe_assignment_applied());
        #[cfg(not(feature = "mvcc-chunks"))]
        let mut assignment_applied: Option<tokio::sync::watch::Receiver<Option<String>>> = None;
        loop {
            tokio::select! {
                _ = timer.tick() => {}
                _ = wait_for_assignment_applied(&mut assignment_applied) => {}
                _ = cancellation_token.cancelled() => break,
            }
            let status =
                get_worker_status(&self.worker, self.allocations_checker.current_epoch()).await;
            *self.worker_status.write() = status;
        }
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
                WorkerEvent::Query {
                    peer_id,
                    query,
                    resp_chan,
                } => {
                    if !self.validate_query(&query, peer_id) {
                        continue;
                    }
                    match self.queries_tx.try_send((peer_id, query, resp_chan)) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!("Queries queue is full. Dropping query from {peer_id}");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            break;
                        }
                    }
                }
                WorkerEvent::SqlQuery {
                    peer_id,
                    query,
                    resp_chan,
                } => {
                    if !self.validate_query(&query, peer_id) {
                        continue;
                    }
                    match self.sql_queries_tx.try_send((peer_id, query, resp_chan)) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!("SQL Queries queue is full. Dropping query from {peer_id}");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            break;
                        }
                    }
                }
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
        // TODO: check rate limits here
        // TODO: check that query_id has not been used before
        true
    }

    #[instrument(skip_all, fields(query_id = %query.query_id, peer_id = %peer_id, dataset = %query.dataset))]
    async fn handle_query(
        &self,
        peer_id: PeerId,
        query: Query,
        resp_chan: ResponseChannel<sqd_messages::QueryResult>,
        query_type: QueryType,
    ) {
        let query_id = query.query_id.clone();
        let compression = query.compression();

        let (mut result, retry_after) = process_query(
            &*self.worker,
            &self.allocations_checker,
            peer_id,
            &query,
            query_type,
        )
        .await;
        if let Err(e) = &result {
            warn!("Query {query_id} by {peer_id} execution failed: {e:?}");
        }

        metrics::query_executed(&result);

        // Cloning is much cheaper than hash computation and we need to keep the result for logging
        match self
            .send_query_result(
                query_id,
                result.clone(),
                resp_chan,
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
            Err(e) => tracing::error!("Couldn't send query result: {e:?}"),
        }

        if let Some(log) = generate_log(&result, query, peer_id).await {
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

    #[instrument(skip_all)]
    async fn send_query_result(
        &self,
        query_id: String,
        result: QueryResult,
        resp_chan: ResponseChannel<sqd_messages::QueryResult>,
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
        let _span = tracing::debug_span!("sign_query_result");
        tokio::task::block_in_place(|| msg.sign(&self.keypair).map_err(|e| anyhow!(e)))?;
        drop(_span);
        let signing_duration = signing_timer.elapsed();

        let result_size = msg.encoded_len() as u64;
        if result_size > protocol::MAX_QUERY_RESULT_SIZE {
            anyhow::bail!("query result size too large: {result_size}");
        }

        tracing::trace!("Sending query result");
        // TODO: propagate backpressure from the transport lib
        self.transport_handle
            .send_query_result(msg, resp_chan)
            .map_err(|_| anyhow!("queue full"))?;

        Ok((compression_duration, signing_duration))
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

// Resolves when the current assignment becomes fully applied. `applied` is `None`
// in non-mvcc builds (no "applied" concept), so it never resolves and the status
// loop relies on the periodic timer only.
async fn wait_for_assignment_applied(
    applied: &mut Option<tokio::sync::watch::Receiver<Option<String>>>,
) {
    match applied {
        Some(applied) => applied
            .changed()
            .await
            .expect("assignment_applied sender outlives the status loop"),
        None => std::future::pending::<()>().await,
    }
}

/// Run a query and account for its compute units: claim one up front, refund the unused chunk
/// fraction after. Generic over the engine and rate limiter for mock-based tests (see
/// [`super::query_deps`]).
#[instrument(skip_all)]
async fn process_query<W: QueryRunner, A: CuChecker>(
    worker: &W,
    allocations_checker: &A,
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

    let Some(block_range) = query
        .block_range
        .map(|sqd_messages::Range { begin, end }| (begin, end))
    else {
        return (
            Err(QueryError::BadRequest("block_range is required".to_owned())),
            None,
        );
    };

    let mut allocation_chip = 1.0f32;

    if let Ok(chunk) = query.chunk_id.parse::<DataChunk>() {
        let (begin, end) = block_range;
        let active_len = std::cmp::min(chunk.last_block.into(), end)
            .saturating_sub(std::cmp::max(chunk.first_block.into(), begin))
            .max(1);
        let chunk_len = Into::<u64>::into(chunk.last_block)
            .saturating_sub(chunk.first_block.into())
            .max(1);
        allocation_chip = active_len as f32 / chunk_len as f32;
    };

    // We claim 1. allocation first and refund unused allocation later. It's done to prevent burst overloading with small requests.
    let status = match allocations_checker.try_spend(peer_id, 1.) {
        RateLimitStatus::NoAllocation => {
            // This error means that we don't have any allocation for particular peer_id at all (e.g. no allocation on contract)
            return (Err(QueryError::NoAllocation), None);
        }
        RateLimitStatus::Paused(retry_after) => {
            // This error means that we don't have allocation at the moment, but it may be available after retry_after ms
            return (Err(QueryError::NoAllocation), Some(retry_after));
        }
        status => status,
    };
    let mut retry_after = status.retry_after();

    let result = worker
        .run_query(
            &query.query,
            query.dataset.clone(),
            block_range,
            &query.chunk_id,
            Some(peer_id),
            query_type,
        )
        .await
        .inspect_err(|err| tracing::error!("error processing query: {err}"));

    if let Err(QueryError::ServiceOverloaded) = result {
        // Refund everything as we were not able to process request
        allocations_checker.refund(peer_id, 1.);
        retry_after = Some(DEFAULT_BACKOFF);
    } else if allocation_chip < 1. {
        // We refund unused allocation
        allocations_checker.refund(peer_id, 1. - allocation_chip);
    }
    (result, retry_after)
}

#[instrument(skip_all)]
async fn generate_log(
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

#[tracing::instrument(skip_all)]
async fn get_worker_status(
    worker: &Worker,
    current_epoch: Option<u32>,
) -> sqd_messages::WorkerStatus {
    let status = worker.status().await;
    let assignment_id = status.assignment_id.unwrap_or_default();
    sqd_messages::WorkerStatus {
        assignment_id,
        missing_chunks: Some(BitString::new(&status.unavailability_map)),
        version: WORKER_VERSION.to_string(),
        stored_bytes: Some(status.stored_bytes),
        current_epoch,
        #[cfg(feature = "mvcc-chunks")]
        last_applied_assignment_id: status.last_applied_assignment_id,
        #[cfg(not(feature = "mvcc-chunks"))]
        last_applied_assignment_id: None,
    }
}

fn push_pending_assignment(
    pending: &mut VecDeque<super::assignments::AssignmentUpdate>,
    update: super::assignments::AssignmentUpdate,
) -> bool {
    if let Some(skipped) = push_pending_item(pending, update) {
        warn!(
            "Skipping {skipped} pending assignments because assignment queue exceeded {MAX_PENDING_ASSIGNMENTS}"
        );
        true
    } else {
        false
    }
}

fn push_pending_item<T>(pending: &mut VecDeque<T>, item: T) -> Option<usize> {
    pending.push_back(item);
    #[cfg(feature = "mvcc-chunks")]
    if pending.len() > MAX_PENDING_ASSIGNMENTS {
        Some(keep_only_latest_pending_assignment(pending))
    } else {
        None
    }

    #[cfg(not(feature = "mvcc-chunks"))]
    {
        None
    }
}

fn requeue_pending_assignment(
    pending: &mut VecDeque<super::assignments::AssignmentUpdate>,
    update: super::assignments::AssignmentUpdate,
) -> bool {
    if let Some(skipped) = requeue_pending_item(pending, update) {
        warn!(
            "Skipping {skipped} pending assignments because assignment queue exceeded {MAX_PENDING_ASSIGNMENTS}"
        );
        true
    } else {
        false
    }
}

fn requeue_pending_item<T>(pending: &mut VecDeque<T>, item: T) -> Option<usize> {
    pending.push_front(item);
    #[cfg(feature = "mvcc-chunks")]
    if pending.len() > MAX_PENDING_ASSIGNMENTS {
        Some(keep_only_latest_pending_assignment(pending))
    } else {
        None
    }

    #[cfg(not(feature = "mvcc-chunks"))]
    {
        None
    }
}

#[cfg(feature = "mvcc-chunks")]
fn keep_only_latest_pending_assignment<T>(pending: &mut VecDeque<T>) -> usize {
    let latest = pending
        .pop_back()
        .expect("pending queue was just checked as non-empty");
    let skipped = pending.len();
    pending.clear();
    pending.push_back(latest);
    skipped
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

#[cfg(all(test, feature = "mvcc-chunks"))]
mod assignment_tests {
    use super::*;

    #[test]
    fn keep_only_latest_pending_assignment_drops_intermediate_assignments() {
        let mut pending = VecDeque::from([1, 2, 3, 4, 5, 6]);

        let skipped = keep_only_latest_pending_assignment(&mut pending);

        assert_eq!(skipped, 5);
        assert_eq!(pending.into_iter().collect::<Vec<_>>(), vec![6]);
    }

    #[test]
    fn pending_assignments_below_threshold_keep_fifo_order() {
        let mut pending = VecDeque::new();

        for assignment in 1..=MAX_PENDING_ASSIGNMENTS {
            assert_eq!(push_pending_item(&mut pending, assignment), None);
        }

        assert_eq!(
            pending.into_iter().collect::<Vec<_>>(),
            (1..=MAX_PENDING_ASSIGNMENTS).collect::<Vec<_>>()
        );
    }

    #[test]
    fn failed_assignment_requeue_overflow_keeps_latest_pending_assignment() {
        let mut pending = VecDeque::from([2, 3, 4, 5, 6]);

        let skipped = requeue_pending_item(&mut pending, 1);

        assert_eq!(skipped, Some(5));
        assert_eq!(pending.into_iter().collect::<Vec<_>>(), vec![6]);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqd_messages::{query_executed, Compression, Range};

    use crate::{
        controller::query_deps::mocks::{MockChecker, MockWorker},
        query::result::QueryOk,
    };

    use super::*;

    // first_block = 100, last_block = 200 → chunk length 100.
    const CHUNK_ID: &str = "0000000000/0000000100-0000000200-abcde";

    fn query_with(compression: i32, block_range: Option<(u64, u64)>, chunk_id: &str) -> Query {
        Query {
            query_id: "00000000-0000-0000-0000-000000000000".to_owned(),
            dataset: "s3://dataset".to_owned(),
            query: "{}".to_owned(),
            chunk_id: chunk_id.to_owned(),
            block_range: block_range.map(|(begin, end)| Range { begin, end }),
            compression,
            ..Default::default()
        }
    }

    fn ok_result() -> QueryResult {
        Ok(QueryOk::new(
            b"hello".to_vec(),
            1,
            200,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        ))
    }

    // An overloaded engine is fully refunded, so the query consumes no compute unit.
    #[tokio::test(flavor = "multi_thread")]
    async fn overloaded_query_consumes_no_cu() {
        let checker = MockChecker::new(RateLimitStatus::Spent(None));
        let worker = MockWorker::new(Err(QueryError::ServiceOverloaded));
        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);

        let (result, retry) =
            process_query(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(matches!(result, Err(QueryError::ServiceOverloaded)));
        assert_eq!(retry, Some(DEFAULT_BACKOFF));
        assert_eq!(checker.net_spent(), 0.0);
        assert_eq!(worker.calls(), 1);
    }

    // Malformed params are rejected before the CU spend: no retry hint, no charge, engine not run.
    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_query_is_not_charged() {
        let checker = MockChecker::new(RateLimitStatus::Spent(Some(Duration::from_secs(5))));
        let worker = MockWorker::new(ok_result());
        let query = query_with(999, Some((100, 200)), CHUNK_ID);

        let (result, retry) =
            process_query(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(matches!(result, Err(QueryError::BadRequest(_))));
        assert_eq!(retry, None);
        assert_eq!(checker.net_spent(), 0.0);
        assert_eq!(worker.calls(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_full_chunk_consumes_one_cu() {
        let checker = MockChecker::new(RateLimitStatus::Spent(None));
        let worker = MockWorker::new(ok_result());
        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);

        let (result, retry) =
            process_query(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(result.is_ok());
        assert_eq!(retry, None);
        assert_eq!(checker.net_spent(), 1.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partial_chunk_refunds_unused_fraction() {
        let checker = MockChecker::new(RateLimitStatus::Spent(None));
        let worker = MockWorker::new(ok_result());
        let query = query_with(Compression::None as i32, Some((100, 150)), CHUNK_ID);

        let (result, _) =
            process_query(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(result.is_ok());
        assert!(
            (checker.net_spent() - 0.5).abs() < 1e-6,
            "expected 0.5 CU, got {}",
            checker.net_spent()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_allocation_is_rejected_without_running() {
        let checker = MockChecker::new(RateLimitStatus::NoAllocation);
        let worker = MockWorker::new(ok_result());
        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);

        let (result, retry) =
            process_query(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(matches!(result, Err(QueryError::NoAllocation)));
        assert_eq!(retry, None);
        assert_eq!(worker.calls(), 0);
        assert_eq!(checker.net_spent(), 0.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rate_limited_is_rejected_with_retry() {
        let retry = Duration::from_secs(2);
        let checker = MockChecker::new(RateLimitStatus::Paused(retry));
        let worker = MockWorker::new(ok_result());
        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);

        let (result, got_retry) =
            process_query(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(matches!(result, Err(QueryError::NoAllocation)));
        assert_eq!(got_retry, Some(retry));
        assert_eq!(worker.calls(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_allocation_is_not_logged() {
        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);
        let log = generate_log(&Err(QueryError::NoAllocation), query, PeerId::random()).await;
        assert!(log.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn log_for_success_summarizes_result() {
        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);
        let log = generate_log(&ok_result(), query, PeerId::random())
            .await
            .expect("success should be logged");
        match log.result.expect("log should carry a result") {
            query_executed::Result::Ok(summary) => {
                assert_eq!(summary.last_block, 200);
                assert_eq!(summary.uncompressed_data_size, 5);
                assert!(!summary.data_hash.is_empty());
            }
            other => panic!("expected an Ok summary, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn log_for_error_records_the_error() {
        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);
        let log = generate_log(
            &Err(QueryError::BadRequest("bad".to_owned())),
            query,
            PeerId::random(),
        )
        .await
        .expect("errors past the CU bar are logged");
        match log.result.expect("log should carry a result") {
            query_executed::Result::Err(e) => {
                assert!(matches!(e.err, Some(query_error::Err::BadRequest(_))));
            }
            other => panic!("expected an Err result, got {other:?}"),
        }
    }
}
