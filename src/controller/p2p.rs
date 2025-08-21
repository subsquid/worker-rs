use std::{env, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use camino::Utf8PathBuf as PathBuf;
use futures::{Stream, StreamExt};
use parking_lot::RwLock;
use sqd_messages::{
    query_error, query_executed, BitString, LogsRequest, ProstMsg, Query, QueryExecuted, QueryLogs,
    WorkerStatus,
};
use sqd_network_transport::{
    protocol, Keypair, P2PTransportBuilder, PeerId, QueueFull, ResponseChannel, WorkerConfig,
    WorkerEvent, WorkerTransportHandle,
};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    cli::Args,
    compute_units::{
        self,
        allocations_checker::{self, AllocationsChecker},
    },
    logs_storage::LogsStorage,
    metrics,
    query::result::{QueryError, QueryResult},
    run_all,
    util::{timestamp_now_ms, UseOnce},
};

use super::worker::Worker;

const WORKER_VERSION: &str = env!("CARGO_PKG_VERSION");
const LOG_REQUESTS_QUEUE_SIZE: usize = 4;
const QUERIES_POOL_SIZE: usize = 16;
const CONCURRENT_QUERY_MESSAGES: usize = 32;
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
    queries_tx: mpsc::Sender<(PeerId, Query, ResponseChannel<sqd_messages::QueryResult>)>,
    queries_rx:
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

    let worker_status = get_worker_status(&worker);

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
    let (event_stream, transport_handle) = transport_builder.build_worker(config).await?;

    let (queries_tx, queries_rx) = mpsc::channel(QUERIES_POOL_SIZE);
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
            assignments_task,
            logs_task,
            logs_cleanup_task,
            status_update_task,
            worker_task,
            allocations_task,
        );
    }

    async fn run_queries_loop(&self, cancellation_token: CancellationToken) {
        let queries_rx = self.queries_rx.take().unwrap();
        ReceiverStream::new(queries_rx)
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(
                CONCURRENT_QUERY_MESSAGES,
                |(peer_id, query, resp_chan)| async move {
                    self.handle_query(peer_id, query, resp_chan).await;
                },
            )
            .await;
        info!("Query processing task finished");
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
        )
        .take_until(cancellation_token.cancelled_owned())
        .for_each(|update| async move {
            let status = match update.assignment.get_worker(self.worker_id) {
                Some(worker) => worker.status(),
                None => {
                    error!("No assignment found for this worker, waiting for the next epoch");
                    metrics::set_status(metrics::WorkerStatus::NotRegistered);
                    return;
                }
            };

            if !self
                .worker
                .register_assignment(update.assignment, update.id, &self.keypair)
            {
                return;
            }

            match status {
                sqd_assignments::WorkerStatus::Ok => {
                    info!("New assignment applied");
                    metrics::set_status(metrics::WorkerStatus::Active);
                }
                sqd_assignments::WorkerStatus::Unreliable => {
                    warn!("Worker is considered unreliable");
                    metrics::set_status(metrics::WorkerStatus::Unreliable);
                }
                sqd_assignments::WorkerStatus::DeprecatedVersion => {
                    warn!("Worker should be updated");
                    metrics::set_status(metrics::WorkerStatus::DeprecatedVersion);
                }
                sqd_assignments::WorkerStatus::UnsupportedVersion => {
                    warn!("Worker version is unsupported");
                    metrics::set_status(metrics::WorkerStatus::UnsupportedVersion);
                }
            }
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
                let status = get_worker_status(&self.worker);
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

    async fn handle_query(
        &self,
        peer_id: PeerId,
        query: Query,
        resp_chan: ResponseChannel<sqd_messages::QueryResult>,
    ) {
        let query_id = query.query_id.clone();

        let (result, retry_after) = self.process_query(peer_id, &query).await;
        if let Err(e) = &result {
            warn!("Query {query_id} by {peer_id} execution failed: {e:?}");
        }

        metrics::query_executed(&result);
        let log = self.generate_log(&result, query, peer_id);

        if let Err(e) = self.send_query_result(query_id, result, resp_chan, retry_after) {
            tracing::error!("Couldn't send query result: {e:?}, query log: {log:?}");
        }

        if let Some(log) = log {
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

    /// Returns query result and the time to wait before sending the next query
    async fn process_query(
        &self,
        peer_id: PeerId,
        query: &Query,
    ) -> (QueryResult, Option<Duration>) {
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
            )
            .await;

        if let Err(QueryError::ServiceOverloaded) = result {
            self.allocations_checker.refund(peer_id);
            retry_after = Some(DEFAULT_BACKOFF);
        }
        (result, retry_after)
    }

    fn send_query_result(
        &self,
        query_id: String,
        result: QueryResult,
        resp_chan: ResponseChannel<sqd_messages::QueryResult>,
        retry_after: Option<Duration>,
    ) -> Result<()> {
        let query_result = match result {
            Ok(result) => {
                let data = result.compressed_data();
                sqd_messages::query_result::Result::Ok(sqd_messages::QueryOk {
                    data,
                    last_block: result.last_block,
                })
            }
            Err(e) => query_error::Err::from(e).into(),
        };
        let mut msg = sqd_messages::QueryResult {
            query_id,
            result: Some(query_result),
            retry_after_ms: retry_after.map(|duration| duration.as_millis() as u32),
            signature: Default::default(),
        };
        msg.sign(&self.keypair).map_err(|e| anyhow!(e))?;

        let result_size = msg.encoded_len() as u64;
        if result_size > protocol::MAX_QUERY_RESULT_SIZE {
            anyhow::bail!("query result size too large: {result_size}");
        }

        // TODO: propagate backpressure from the transport lib
        self.transport_handle
            .send_query_result(msg, resp_chan)
            .map_err(|_| anyhow!("queue full"))?;

        Ok(())
    }

    fn generate_log(
        &self,
        query_result: &QueryResult,
        query: Query,
        client_id: PeerId,
    ) -> Option<QueryExecuted> {
        use query_executed::Result;

        let result = match query_result {
            Ok(result) => Result::Ok(sqd_messages::QueryOkSummary {
                uncompressed_data_size: result.data.len() as u64,
                data_hash: result.sha3_256(),
                last_block: result.last_block,
            }),
            Err(QueryError::NoAllocation) => return None,
            Err(e) => query_error::Err::from(e).into(),
        };
        let exec_time = match query_result {
            Ok(result) => result.exec_time.as_micros() as u32,
            Err(_) => 0, // TODO: always measure execution time
        };

        Some(QueryExecuted {
            client_id: client_id.to_string(),
            query: Some(query),
            exec_time_micros: exec_time,
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

#[tracing::instrument(skip_all)]
fn get_worker_status(worker: &Worker) -> sqd_messages::WorkerStatus {
    let status = worker.status();
    let assignment_id = match status.assignment_id {
        Some(assignment_id) => assignment_id,
        None => String::new(),
    };
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
