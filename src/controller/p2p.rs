use std::collections::VecDeque;
use std::{env, sync::Arc, time::Duration};

use anyhow::Result;
use camino::Utf8PathBuf as PathBuf;
use futures::{FutureExt, Stream, StreamExt};
use parking_lot::RwLock;
use sqd_messages::{
    query_error, query_executed, BitString, LogsRequest, ProstMsg, Query, QueryExecuted,
    TimeReport, WorkerStatus,
};
use sqd_network_transport::{
    protocol, Keypair, P2PTransportBuilder, PeerId, ResponseSender, WorkerConfig, WorkerEvent,
    WorkerTransportHandle,
};
use tokio::{
    sync::{mpsc, Semaphore},
    time::MissedTickBehavior,
};
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
/// Caps concurrent reject sends. Past this, the response is dropped — a cheap stream reset — rather
/// than spawning unbounded signing tasks under a flood.
const MAX_CONCURRENT_REJECTS: usize = 64;
const LOGS_KEEP_DURATION: Duration = Duration::from_secs(3600 * 2);
const LOGS_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(60);
const MAX_PENDING_ASSIGNMENTS: usize = 5;
// TODO: find out why the margin is required
const MAX_LOGS_SIZE: usize =
    sqd_network_transport::protocol::MAX_LOGS_RESPONSE_SIZE as usize - 100 * 1024;

/// The trailing `Option<Duration>` is the rate-limit backoff hint carried from admission.
type AdmittedQuery = (PeerId, Query, ResponseSender, Option<Duration>);

/// What the log records for a served query, built alongside the wire message in [`build_delivery`]
/// so the two can't diverge — a downgraded (oversized or unsignable) result is `Err` in both.
enum Logged {
    Ok {
        data_hash: Vec<u8>,
        uncompressed_size: u64,
        last_block: u64,
        timings: TimeReport,
    },
    Err(query_error::Err),
}

pub struct P2PController<EventStream> {
    worker: Arc<Worker>,
    worker_status: RwLock<WorkerStatus>,
    assignment_check_interval: Duration,
    assignment_fetch_timeout: Duration,
    assignment_fetch_max_delay: Duration,
    raw_event_stream: UseOnce<EventStream>,
    // Held to keep the transport running; it stops when the last handle drops. Responses go through
    // each request's `ResponseSender`, never this handle.
    _transport_handle: WorkerTransportHandle,
    logs_storage: LogsStorage,
    allocations_checker: AllocationsChecker,
    worker_id: PeerId,
    keypair: Keypair,
    assignment_url: String,
    queries_tx: mpsc::Sender<AdmittedQuery>,
    queries_rx: UseOnce<mpsc::Receiver<AdmittedQuery>>,
    sql_queries_tx: mpsc::Sender<AdmittedQuery>,
    sql_queries_rx: UseOnce<mpsc::Receiver<AdmittedQuery>>,
    log_requests_tx: mpsc::Sender<(LogsRequest, ResponseSender)>,
    log_requests_rx: UseOnce<mpsc::Receiver<(LogsRequest, ResponseSender)>>,
    // Caps concurrent reject sends; see `spawn_error_response`.
    reject_semaphore: Arc<Semaphore>,
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

    let config = WorkerConfig::default();
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
        _transport_handle: transport_handle,
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
        reject_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_REJECTS)),
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
            .for_each_concurrent(
                CONCURRENT_QUERY_MESSAGES,
                |(peer_id, query, resp_chan, retry_after)| {
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.handle_query(
                            peer_id,
                            query,
                            resp_chan,
                            retry_after,
                            QueryType::PlainQuery,
                        )
                        .await;
                    })
                    .map(|r| r.unwrap())
                },
            )
            .await;
        info!("Query processing task finished");
    }

    async fn run_sql_queries_loop(self: Arc<Self>, cancellation_token: CancellationToken) {
        let sql_queries_rx = self.sql_queries_rx.take().unwrap();
        ReceiverStream::new(sql_queries_rx)
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(
                CONCURRENT_QUERY_MESSAGES,
                |(peer_id, query, resp_chan, retry_after)| {
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.handle_query(
                            peer_id,
                            query,
                            resp_chan,
                            retry_after,
                            QueryType::SqlQuery,
                        )
                        .await;
                    })
                    .map(|r| r.unwrap())
                },
            )
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

    async fn run_event_loop(self: Arc<Self>, cancellation_token: CancellationToken) {
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
                    request,
                    resp_chan,
                } => {
                    let query = match Query::decode(request.as_ref()) {
                        Ok(query) => query,
                        Err(e) => {
                            // Without a decodable query_id there's nothing to bind a signed
                            // response to.
                            warn!("Failed to decode query from {peer_id}: {e}");
                            continue;
                        }
                    };
                    if !self.admit_query(peer_id, query, resp_chan, &self.queries_tx, "Query") {
                        break;
                    }
                }
                WorkerEvent::SqlQuery {
                    peer_id,
                    request,
                    resp_chan,
                } => {
                    let query = match Query::decode(request.as_ref()) {
                        Ok(query) => query,
                        Err(e) => {
                            // Without a decodable query_id there's nothing to bind a signed
                            // response to.
                            warn!("Failed to decode SQL query from {peer_id}: {e}");
                            continue;
                        }
                    };
                    if !self.admit_query(peer_id, query, resp_chan, &self.sql_queries_tx, "SQL query")
                    {
                        break;
                    }
                }
                WorkerEvent::LogsRequest {
                    peer_id,
                    request,
                    resp_chan,
                } => {
                    let request = match LogsRequest::decode(request.as_ref()) {
                        Ok(request) => request,
                        Err(e) => {
                            warn!("Failed to decode logs request from {peer_id}: {e}");
                            continue;
                        }
                    };
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
                    tokio::spawn(async move {
                        tracing::debug!("Sending worker status to {peer_id}");
                        if let Err(e) = resp_chan.send(&status.encode_to_vec()).await {
                            warn!("Couldn't respond with status to {peer_id}: {e:?}");
                        }
                    });
                }
            }
        }

        info!("Transport event loop finished");
    }

    /// The admission bar: reserve a queue slot, then spend one compute unit. Past it a query is
    /// billable and always logged; rejected here (queue full, no allocation, rate limited) it gets a
    /// typed error but no log. Reserving before spending guarantees a spent unit always enqueues.
    /// Returns `false` when the receiver is gone, to stop the event loop.
    fn admit_query(
        self: &Arc<Self>,
        peer_id: PeerId,
        query: Query,
        resp_chan: ResponseSender,
        queue_tx: &mpsc::Sender<AdmittedQuery>,
        protocol: &str,
    ) -> bool {
        if let Err(err) = self.validate_query(&query, peer_id) {
            self.clone()
                .spawn_error_response(query.query_id, err, None, resp_chan);
            return true;
        }
        let permit = match queue_tx.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(())) => {
                warn!("{protocol} queue is full. Rejecting query from {peer_id}");
                self.clone().spawn_error_response(
                    query.query_id,
                    query_error::Err::TooManyRequests(()),
                    Some(DEFAULT_BACKOFF),
                    resp_chan,
                );
                return true;
            }
            Err(mpsc::error::TrySendError::Closed(())) => return false,
        };
        // Claim a full unit up front, refund the unused fraction after; otherwise a burst of small
        // requests could overload the worker.
        match self.allocations_checker.try_spend(peer_id, 1.) {
            RateLimitStatus::NoAllocation => {
                self.clone().spawn_error_response(
                    query.query_id,
                    query_error::Err::TooManyRequests(()),
                    None,
                    resp_chan,
                );
            }
            RateLimitStatus::Paused(retry_after) => {
                self.clone().spawn_error_response(
                    query.query_id,
                    query_error::Err::TooManyRequests(()),
                    Some(retry_after),
                    resp_chan,
                );
            }
            RateLimitStatus::Spent(retry_after) => {
                permit.send((peer_id, query, resp_chan, retry_after));
            }
        }
        true
    }

    /// Send a signed error, so a rejected query gets a typed reason instead of an opaque stream
    /// reset. Best-effort: on a signing or send failure it drops the response, resetting the stream.
    async fn send_error_result(
        &self,
        query_id: String,
        err: query_error::Err,
        retry_after: Option<Duration>,
        resp_chan: ResponseSender,
    ) {
        let mut msg = sqd_messages::QueryResult {
            query_id,
            result: Some(err.into()),
            retry_after_ms: retry_after.map(|d| d.as_millis() as u32),
            signature: Default::default(),
        };
        if let Err(e) = msg.sign(&self.keypair) {
            warn!("Couldn't sign error response: {e}");
            return;
        }
        if let Err(e) = resp_chan.send(&msg.encode_to_vec()).await {
            warn!("Couldn't send error response: {e:?}");
        }
    }

    /// Run [`Self::send_error_result`] off the event loop so the write can't stall it. `reject_semaphore`
    /// caps in-flight sends; past the limit the response is dropped instead of spawning unbounded tasks.
    fn spawn_error_response(
        self: Arc<Self>,
        query_id: String,
        err: query_error::Err,
        retry_after: Option<Duration>,
        resp_chan: ResponseSender,
    ) {
        let Ok(permit) = self.reject_semaphore.clone().try_acquire_owned() else {
            warn!("Too many pending rejections, dropping response for query {query_id}");
            return;
        };
        tokio::spawn(async move {
            let _permit = permit;
            self.send_error_result(query_id, err, retry_after, resp_chan)
                .await;
        });
    }

    /// Checks the signature and timestamp freshness; the `Err` is the response to send back.
    #[instrument(skip_all)]
    fn validate_query(&self, query: &Query, peer_id: PeerId) -> Result<(), query_error::Err> {
        if !query.verify_signature(peer_id, self.worker_id) {
            warn!("Rejected query with invalid signature from {peer_id}");
            return Err(query_error::Err::BadRequest(
                "invalid query signature".to_owned(),
            ));
        }
        if query.timestamp_ms.abs_diff(timestamp_now_ms()) as u128
            > protocol::MAX_TIME_LAG.as_millis()
        {
            warn!(
                "Rejected query with invalid timestamp ({}) from {peer_id}",
                query.timestamp_ms
            );
            return Err(query_error::Err::BadRequest(
                "timestamp out of allowed range".to_owned(),
            ));
        }
        // TODO: check rate limits here
        // TODO: check that query_id has not been used before
        Ok(())
    }

    /// Serve an admitted query. It already spent a compute unit, so it always produces a response
    /// and a matching log — both come from the same outcome, so they can't diverge.
    #[instrument(skip_all, fields(query_id = %query.query_id, peer_id = %peer_id, dataset = %query.dataset))]
    async fn handle_query(
        &self,
        peer_id: PeerId,
        query: Query,
        resp_chan: ResponseSender,
        retry_after: Option<Duration>,
        query_type: QueryType,
    ) {
        let outcome = execute(
            &*self.worker,
            &self.allocations_checker,
            peer_id,
            &query,
            query_type,
        )
        .await;
        if let Err(e) = &outcome {
            warn!("Query {} by {peer_id} execution failed: {e:?}", query.query_id);
        }
        metrics::query_executed(&outcome);

        // Overload is transient, so hint a retry; otherwise keep the admission hint.
        let retry_after = if matches!(outcome, Err(QueryError::ServiceOverloaded)) {
            Some(DEFAULT_BACKOFF)
        } else {
            retry_after
        };

        let compression = query.compression();
        let (message, logged) =
            build_delivery(&self.keypair, &query.query_id, outcome, retry_after, compression).await;

        // Send before logging: the unit was spent at admission so the log happens regardless, and
        // the SQLite write stays off the response path.
        if let Err(e) = resp_chan.send(&message.encode_to_vec()).await {
            warn!("Couldn't send query result to {peer_id}: {e:?}");
        }

        let log = build_log(query, peer_id, logged);
        if log.encoded_len() > MAX_LOGS_SIZE {
            warn!("Query log is too big: {log:?}");
            return;
        }
        if let Err(e) = self.logs_storage.save_log(log).await {
            warn!("Couldn't save query log: {e:?}");
        }
    }

    async fn handle_logs_request(
        &self,
        request: LogsRequest,
        resp_chan: ResponseSender,
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
        resp_chan.send(&msg.encode_to_vec()).await?;
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

/// Run an admitted query and refund the unused chunk fraction. The unit was spent at admission, so
/// malformed params here are still charged and logged. Generic over the engine and rate limiter for
/// mock-based tests (see [`super::query_deps`]).
#[instrument(skip_all)]
async fn execute<W: QueryRunner, A: CuChecker>(
    worker: &W,
    allocations_checker: &A,
    peer_id: PeerId,
    query: &Query,
    query_type: QueryType,
) -> QueryResult {
    match query.compression {
        c if c == sqd_messages::Compression::Gzip as i32
            || c == sqd_messages::Compression::Zstd as i32
            || c == sqd_messages::Compression::None as i32 => {}
        _ => {
            return Err(QueryError::BadRequest(
                "Unsupported compression type".to_owned(),
            ));
        }
    }

    let Some(block_range) = query
        .block_range
        .map(|sqd_messages::Range { begin, end }| (begin, end))
    else {
        return Err(QueryError::BadRequest("block_range is required".to_owned()));
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

    if allocation_chip < 1. {
        allocations_checker.refund(peer_id, 1. - allocation_chip);
    }
    result
}

/// Build the wire message and the log record from one outcome, so they can't diverge: an oversized
/// or unsignable success downgrades to a signed `server_error` in both.
#[instrument(skip_all)]
async fn build_delivery(
    keypair: &Keypair,
    query_id: &str,
    outcome: QueryResult,
    retry_after: Option<Duration>,
    compression: sqd_messages::Compression,
) -> (sqd_messages::QueryResult, Logged) {
    let retry_after_ms = retry_after.map(|d| d.as_millis() as u32);
    let qok = match outcome {
        Ok(qok) => qok,
        Err(e) => {
            let err = query_error::Err::from(e);
            return (
                signed_result(keypair, query_id, err.clone().into(), retry_after_ms),
                Logged::Err(err),
            );
        }
    };

    let data_hash = qok.sha3_256().await;
    let uncompressed_size = qok.data.len() as u64;
    let last_block = qok.last_block;
    let time_report = qok.time_report.clone();

    let compression_timer = std::time::Instant::now();
    let data = match compression {
        sqd_messages::Compression::None => qok.data,
        sqd_messages::Compression::Gzip => qok.data_gzip().await,
        sqd_messages::Compression::Zstd => qok.data_zstd().await,
    };
    let compression_time = compression_timer.elapsed();

    let mut msg = sqd_messages::QueryResult {
        query_id: query_id.to_owned(),
        result: Some(sqd_messages::query_result::Result::Ok(sqd_messages::QueryOk {
            data,
            last_block,
        })),
        retry_after_ms,
        signature: Default::default(),
    };
    let signing_timer = std::time::Instant::now();
    let _span = tracing::debug_span!("sign_query_result").entered();
    let sign_result = tokio::task::block_in_place(|| msg.sign(keypair));
    drop(_span);
    let signing_time = signing_timer.elapsed();

    let too_large = msg.encoded_len() as u64 > protocol::MAX_QUERY_RESULT_SIZE;
    if let Err(e) = &sign_result {
        // Unreachable given a 36-byte query_id and 32-byte hash, but downgrade defensively so
        // client and log still agree.
        warn!("Couldn't sign query result for {query_id}: {e}");
    }
    if too_large {
        warn!("Query result for {query_id} is too large: {} bytes", msg.encoded_len());
    }
    if sign_result.is_err() || too_large {
        let reason = if sign_result.is_err() {
            "failed to sign query result"
        } else {
            "query result too large"
        };
        let err = query_error::Err::ServerError(reason.to_owned());
        // A downgraded result carries no retry hint.
        return (
            signed_result(keypair, query_id, err.clone().into(), None),
            Logged::Err(err),
        );
    }

    let timings = TimeReport {
        parsing_time_micros: time_report.parsing_time.as_micros() as u32,
        execution_time_micros: time_report.execution_time.as_micros() as u32,
        compression_time_micros: compression_time.as_micros() as u32,
        signing_time_micros: signing_time.as_micros() as u32,
        serialization_time_micros: time_report.serialization_time.as_micros() as u32,
    };
    (
        msg,
        Logged::Ok {
            data_hash,
            uncompressed_size,
            last_block,
            timings,
        },
    )
}

/// Sign a prepared `QueryResult`. Best-effort: a signing failure is logged and the message goes out
/// unsigned, which the client rejects.
fn signed_result(
    keypair: &Keypair,
    query_id: &str,
    result: sqd_messages::query_result::Result,
    retry_after_ms: Option<u32>,
) -> sqd_messages::QueryResult {
    let mut msg = sqd_messages::QueryResult {
        query_id: query_id.to_owned(),
        result: Some(result),
        retry_after_ms,
        signature: Default::default(),
    };
    if let Err(e) = msg.sign(keypair) {
        warn!("Couldn't sign query result for {query_id}: {e}");
    }
    msg
}

/// Build the log from the delivery projection. Every admitted query is logged — no skip path — and
/// the projection already carries whatever error the client saw.
fn build_log(query: Query, client_id: PeerId, logged: Logged) -> QueryExecuted {
    let (result, exec_time_report) = match logged {
        Logged::Ok {
            data_hash,
            uncompressed_size,
            last_block,
            timings,
        } => (
            query_executed::Result::Ok(sqd_messages::QueryOkSummary {
                uncompressed_data_size: uncompressed_size,
                data_hash,
                last_block,
            }),
            Some(timings),
        ),
        Logged::Err(err) => (query_executed::Result::from(err), None),
    };

    QueryExecuted {
        client_id: client_id.to_string(),
        query: Some(query),
        exec_time_micros: exec_time_report
            .as_ref()
            .map_or(0, |report| report.execution_time_micros),
        exec_time_report,
        timestamp_ms: timestamp_now_ms(), // TODO: use time of receiving query
        result: Some(result),
        worker_version: WORKER_VERSION.to_string(),
    }
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

    use sqd_messages::{query_result, Compression, Range};

    use crate::{
        controller::query_deps::mocks::{MockChecker, MockWorker},
        query::result::QueryOk,
    };

    use super::*;

    // first_block = 100, last_block = 200 → chunk length 100.
    const CHUNK_ID: &str = "0000000000/0000000100-0000000200-abcde";
    // 36 chars — the UUID length the signature scheme requires.
    const QUERY_ID: &str = "00000000-0000-0000-0000-000000000000";

    fn query_with(compression: i32, block_range: Option<(u64, u64)>, chunk_id: &str) -> Query {
        Query {
            query_id: QUERY_ID.to_owned(),
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

    // An overloaded engine keeps the compute unit: admission spent it and the served stage doesn't
    // refund on overload.
    #[tokio::test(flavor = "multi_thread")]
    async fn overloaded_query_consumes_a_cu() {
        let checker = MockChecker::new(RateLimitStatus::Spent(None));
        checker.try_spend(1.0); // admission
        let worker = MockWorker::new(Err(QueryError::ServiceOverloaded));
        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);

        let outcome =
            execute(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(matches!(outcome, Err(QueryError::ServiceOverloaded)));
        assert_eq!(checker.net_spent(), 1.0);
        assert_eq!(worker.calls(), 1);
    }

    // Malformed params are validated after admission, so they're charged the full unit and never
    // reach the engine.
    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_query_is_charged_a_cu() {
        let checker = MockChecker::new(RateLimitStatus::Spent(Some(Duration::from_secs(5))));
        checker.try_spend(1.0);
        let worker = MockWorker::new(ok_result());
        let query = query_with(999, Some((100, 200)), CHUNK_ID);

        let outcome =
            execute(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(matches!(outcome, Err(QueryError::BadRequest(_))));
        assert_eq!(worker.calls(), 0);
        assert_eq!(checker.net_spent(), 1.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_full_chunk_consumes_one_cu() {
        let checker = MockChecker::new(RateLimitStatus::Spent(None));
        checker.try_spend(1.0);
        let worker = MockWorker::new(ok_result());
        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);

        let outcome =
            execute(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(outcome.is_ok());
        assert_eq!(checker.net_spent(), 1.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partial_chunk_refunds_unused_fraction() {
        let checker = MockChecker::new(RateLimitStatus::Spent(None));
        checker.try_spend(1.0);
        let worker = MockWorker::new(ok_result());
        // 30% of [100, 200] — asymmetric, so a flipped fraction would fail.
        let query = query_with(Compression::None as i32, Some((100, 130)), CHUNK_ID);

        let outcome =
            execute(&worker, &checker, PeerId::random(), &query, QueryType::PlainQuery).await;

        assert!(outcome.is_ok());
        assert!(
            (checker.net_spent() - 0.3).abs() < 1e-6,
            "expected 0.3 CU, got {}",
            checker.net_spent()
        );
    }

    // The admission backoff hint rides along with a BadRequest response.
    #[tokio::test(flavor = "multi_thread")]
    async fn bad_request_response_carries_retry_after() {
        let keypair = Keypair::generate_ed25519();
        let outcome = Err(QueryError::BadRequest("timestamp out of allowed range".to_owned()));

        let (message, logged) = build_delivery(
            &keypair,
            QUERY_ID,
            outcome,
            Some(Duration::from_secs(5)),
            Compression::None,
        )
        .await;

        assert_eq!(message.retry_after_ms, Some(5000));
        assert!(matches!(
            message.result,
            Some(query_result::Result::Err(sqd_messages::QueryError {
                err: Some(query_error::Err::BadRequest(_)),
            }))
        ));
        assert!(matches!(
            logged,
            Logged::Err(query_error::Err::BadRequest(_))
        ));
    }

    // A result that can't be signed (forced here with a non-UUID query_id) downgrades to
    // server_error in both the response and the log.
    #[tokio::test(flavor = "multi_thread")]
    async fn unsignable_result_downgrades_response_and_log_together() {
        let keypair = Keypair::generate_ed25519();

        let (message, logged) =
            build_delivery(&keypair, "not-a-uuid", ok_result(), None, Compression::None).await;

        assert!(matches!(
            message.result,
            Some(query_result::Result::Err(sqd_messages::QueryError {
                err: Some(query_error::Err::ServerError(_)),
            }))
        ));
        assert!(matches!(
            logged,
            Logged::Err(query_error::Err::ServerError(_))
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_delivery_logs_a_summary() {
        let keypair = Keypair::generate_ed25519();
        let (_message, logged) =
            build_delivery(&keypair, QUERY_ID, ok_result(), None, Compression::None).await;

        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);
        let log = build_log(query, PeerId::random(), logged);
        match log.result.expect("log should carry a result") {
            query_executed::Result::Ok(summary) => {
                assert_eq!(summary.last_block, 200);
                assert_eq!(summary.uncompressed_data_size, 5);
                assert!(!summary.data_hash.is_empty());
            }
            other => panic!("expected an Ok summary, got {other:?}"),
        }
    }

    // Every admitted query is logged; admission already guaranteed it's billable, so there's no
    // skip path.
    #[tokio::test(flavor = "multi_thread")]
    async fn admitted_error_is_always_logged() {
        let keypair = Keypair::generate_ed25519();
        let outcome = Err(QueryError::BadRequest("bad".to_owned()));
        let (_message, logged) =
            build_delivery(&keypair, QUERY_ID, outcome, None, Compression::None).await;

        let query = query_with(Compression::None as i32, Some((100, 200)), CHUNK_ID);
        let log = build_log(query, PeerId::random(), logged);
        match log.result.expect("errors past the CU bar are logged") {
            query_executed::Result::Err(e) => {
                assert!(matches!(e.err, Some(query_error::Err::BadRequest(_))));
            }
            other => panic!("expected an Err result, got {other:?}"),
        }
    }
}
