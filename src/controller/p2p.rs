use std::{env, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use camino::Utf8PathBuf as PathBuf;
use futures::{Stream, StreamExt};
use sqd_contract_client::Network;
use sqd_messages::{
    query_error, query_executed, BitString, Heartbeat, LogsRequest, ProstMsg, Query, QueryExecuted,
    QueryLogs,
};
use sqd_network_transport::{
    protocol, Keypair, P2PTransportBuilder, PeerId, ResponseChannel, WorkerConfig, WorkerEvent,
    WorkerTransportHandle,
};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    cli::Args,
    gateway_allocations::{
        self,
        allocations_checker::{self, AllocationsChecker},
    },
    logs_storage::{LoadedLogs, LogsStorage},
    metrics,
    query::result::{QueryError, QueryResult},
    run_all,
    storage::datasets_index::DatasetsIndex,
    util::{timestamp_now_ms, UseOnce},
};

use sqd_messages::assignments::Assignment;

use super::worker::Worker;

const WORKER_VERSION: &str = env!("CARGO_PKG_VERSION");
const LOG_REQUESTS_QUEUE_SIZE: usize = 4;
const QUERIES_POOL_SIZE: usize = 16;
const CONCURRENT_QUERY_MESSAGES: usize = 32;
const DEFAULT_BACKOFF: Duration = Duration::from_secs(1);
const LOGS_KEEP_DURATION: Duration = Duration::from_secs(3600 * 2);
const LOGS_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const MAX_LOGS_SIZE: usize =
    sqd_network_transport::protocol::MAX_LOGS_RESPONSE_SIZE as usize - 1024;

pub struct P2PController<EventStream> {
    worker: Arc<Worker>,
    heartbeat_interval: Duration,
    assignment_check_interval: Duration,
    raw_event_stream: UseOnce<EventStream>,
    transport_handle: WorkerTransportHandle,
    logs_storage: LogsStorage,
    allocations_checker: AllocationsChecker,
    worker_id: PeerId,
    keypair: Keypair,
    private_key: Vec<u8>,
    network: Network,
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
    let network = args.transport.rpc.network;

    let worker_id = transport_builder.local_peer_id();
    let keypair = transport_builder.keypair();
    let private_key = transport_builder
        .keypair()
        .try_into_ed25519()
        .unwrap()
        .secret()
        .as_ref()
        .to_vec();
    info!("Local peer ID: {worker_id}");
    check_peer_id(worker_id, args.data_dir.join("peer_id"));

    let allocations_checker = allocations_checker::AllocationsChecker::new(
        transport_builder.contract_client(),
        worker_id,
        args.network_polling_interval,
    )
    .await?;

    let (event_stream, transport_handle) =
        transport_builder.build_worker(WorkerConfig::new()).await?;

    let (queries_tx, queries_rx) = mpsc::channel(QUERIES_POOL_SIZE);
    let (log_requests_tx, log_requests_rx) = mpsc::channel(LOG_REQUESTS_QUEUE_SIZE);

    Ok(P2PController {
        worker,
        heartbeat_interval: args.heartbeat_interval,
        assignment_check_interval: args.assignment_check_interval,
        raw_event_stream: UseOnce::new(event_stream),
        transport_handle,
        logs_storage: LogsStorage::new(args.data_dir.join("logs.db").as_str()).await?,
        allocations_checker,
        worker_id,
        keypair,
        private_key,
        network,
        queries_tx,
        queries_rx: UseOnce::new(queries_rx),
        log_requests_tx,
        log_requests_rx: UseOnce::new(log_requests_rx),
    })
}

impl<EventStream: Stream<Item = WorkerEvent>> P2PController<EventStream> {
    pub async fn run(&self, cancellation_token: CancellationToken) {
        run_all!(
            cancellation_token,
            self.run_event_loop(cancellation_token.child_token()),
            self.run_queries_loop(cancellation_token.child_token()),
            self.run_heartbeat_loop(cancellation_token.child_token(), self.heartbeat_interval),
            self.run_assignments_loop(
                cancellation_token.child_token(),
                self.assignment_check_interval
            ),
            self.run_logs_loop(cancellation_token.child_token()),
            self.run_logs_cleanup_loop(cancellation_token.child_token(), LOGS_CLEANUP_INTERVAL),
            self.worker.run(cancellation_token.child_token()),
            self.allocations_checker
                .run(cancellation_token.child_token()),
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

    async fn run_heartbeat_loop(
        &self,
        cancellation_token: CancellationToken,
        heartbeat_interval: Duration,
    ) {
        let mut timer = tokio::time::interval_at(
            tokio::time::Instant::now() + heartbeat_interval,
            heartbeat_interval,
        );
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(timer)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|_| async move {
                tracing::debug!("Sending heartbeat");
                let status = self.worker.status();
                let assignment_id = match status.assignment_id {
                    Some(assignment_id) => assignment_id,
                    None => {
                        tracing::info!("Skipping heartbeat as assignment is not ready");
                        return;
                    }
                };
                let heartbeat = Heartbeat {
                    assignment_id,
                    missing_chunks: Some(BitString::new(&status.unavailability_map)),
                    version: WORKER_VERSION.to_string(),
                    stored_bytes: Some(status.stored_bytes),
                };
                let result = self.transport_handle.send_heartbeat(heartbeat);
                if let Err(err) = result {
                    warn!("Couldn't send heartbeat: {:?}", err);
                }
            })
            .await;
        info!("Heartbeat processing task finished");
    }

    async fn run_assignments_loop(
        &self,
        cancellation_token: CancellationToken,
        assignment_check_interval: Duration,
    ) {
        let mut timer =
            tokio::time::interval_at(tokio::time::Instant::now(), assignment_check_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(timer)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|_| async move {
                tracing::debug!("Checking assignment");
                let network_state_filename = match self.network {
                    Network::Tethys => "network-state-tethys.json",
                    Network::Mainnet => "network-state-mainnet.json",
                };
                let network_state_url =
                    format!("https://metadata.sqd-datasets.io/{network_state_filename}");

                let latest_assignment = self.worker.get_assignment_id();
                let assignment =
                    match Assignment::try_download(network_state_url, latest_assignment).await {
                        Ok(Some(assignment)) => assignment,
                        Ok(None) => {
                            info!("Assignment has not been changed");
                            return;
                        }
                        Err(err) => {
                            error!("Unable to get assignment: {err:?}");
                            return;
                        }
                    };
                let peer_id = self.worker_id;
                let calculated_chunks =
                    match assignment.dataset_chunks_for_peer_id(&peer_id.to_string()) {
                        Some(chunks) => chunks,
                        None => {
                            error!("Can not get assigned chunks.");
                            return;
                        }
                    };
                let headers =
                    match assignment.headers_for_peer_id(&peer_id.to_string(), &self.private_key) {
                        Ok(headers) => headers,
                        Err(error) => {
                            error!("Can not get assigned headers: {error:?}");
                            return;
                        }
                    };
                let status = match assignment.worker_status(peer_id.to_string()) {
                    Ok(status) => status,
                    Err(error) => {
                        error!("Can not get assigned headers: {error:?}");
                        return;
                    }
                };
                let datasets_index = DatasetsIndex::from(calculated_chunks, headers, assignment.id);
                let chunks = datasets_index.create_chunks_set();
                self.worker.set_datasets_index(datasets_index);
                self.worker.set_desired_chunks(chunks);
                if status.to_ascii_lowercase() == "ok" {
                    info!("New assignment applied");
                } else {
                    warn!("Worker is considered unreliable: {status}");
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
            }
        }

        info!("Transport event loop finished");
    }

    fn validate_query(&self, query: &Query, peer_id: PeerId) -> bool {
        if !query.verify_signature(peer_id, self.worker_id) {
            tracing::warn!("Rejected query with invalid signature from {}", peer_id);
            return false;
        }
        if query.timestamp_ms.abs_diff(timestamp_now_ms()) as u128 > protocol::MAX_TIME_LAG.as_millis() {
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
            warn!("Query {query_id} execution failed: {e:?}");
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
            gateway_allocations::RateLimitStatus::NoAllocation => {
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

    pub fn local_peer_id(&self) -> PeerId {
        self.worker_id
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
        let msg = match self
            .logs_storage
            .get_logs(
                request.from_timestamp_ms,
                timestamp_now_ms() - protocol::MAX_TIME_LAG.as_millis() as u64,
                request.last_received_query_id,
                MAX_LOGS_SIZE,
            )
            .await?
        {
            LoadedLogs::All(logs) => sqd_messages::QueryLogs {
                queries_executed: logs,
                has_more: false,
            },
            LoadedLogs::Partial(logs) => sqd_messages::QueryLogs {
                queries_executed: logs,
                has_more: true,
            },
        };
        info!("Sending {} logs", msg.queries_executed.len());
        self.transport_handle.send_logs(msg, resp_chan)?;
        Ok(())
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
