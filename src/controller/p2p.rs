use std::{env, sync::Arc, time::Duration};

use anyhow::Result;
use camino::Utf8PathBuf as PathBuf;
use futures::{Stream, StreamExt};
use lazy_static::lazy_static;
use sqd_messages::{
    query_executed, DatasetRanges, InputAndOutput, Ping, Pong, Query, QueryExecuted, SizeAndHash,
};
use sqd_network_transport::{
    P2PTransportBuilder, PeerId, WorkerConfig, WorkerEvent, WorkerTransportHandle,
};
use tokio::{
    sync::{mpsc, watch},
    time::MissedTickBehavior,
};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, log, warn};

use crate::{
    gateway_allocations::{self, allocations_checker::AllocationsChecker},
    logs_storage::{self, LogsStorage},
    metrics,
    query::result::{QueryError, QueryResult},
    run_all,
    storage::datasets_index::parse_assignment,
    util::{hash::sha3_256, UseOnce},
};

use super::worker::Worker;

const QUERIES_POOL_SIZE: usize = 16;
const CONCURRENT_QUERY_MESSAGES: usize = 32;

lazy_static! {
    static ref LOGS_SEND_INTERVAL: Duration = Duration::from_secs(
        env::var("LOGS_SEND_INTERVAL_SEC")
            .map(|s| s.parse().expect("Invalid LOGS_SEND_INTERVAL_SEC"))
            .unwrap_or(600)
    );
}

pub struct P2PController<EventStream> {
    worker: Arc<Worker>,
    ping_interval: Duration,
    raw_event_stream: UseOnce<EventStream>,
    transport_handle: WorkerTransportHandle,
    logs_storage: LogsStorage,
    allocations_checker: AllocationsChecker,
    worker_id: PeerId,
    queries_tx: mpsc::Sender<(PeerId, Query)>,
    queries_rx: UseOnce<mpsc::Receiver<(PeerId, Query)>>,
    last_collected_log_tx: watch::Sender<Option<u64>>,
}

pub async fn create_p2p_controller(
    worker: Arc<Worker>,
    transport_builder: P2PTransportBuilder,
    allocations_checker: AllocationsChecker,
    scheduler_id: PeerId,
    logs_collector_id: PeerId,
    data_dir: PathBuf,
    ping_interval: Duration,
) -> Result<P2PController<impl Stream<Item = WorkerEvent>>> {
    let worker_id = transport_builder.local_peer_id();
    info!("Local peer ID: {worker_id}");
    check_peer_id(worker_id, data_dir.join("peer_id"));

    let (event_stream, transport_handle) = transport_builder
        .build_worker(WorkerConfig::new(scheduler_id, logs_collector_id))
        .await?;

    let (queries_tx, queries_rx) = mpsc::channel(QUERIES_POOL_SIZE);
    let (last_collected_log_tx, _) = watch::channel(None);

    Ok(P2PController {
        worker,
        ping_interval,
        raw_event_stream: UseOnce::new(event_stream),
        transport_handle,
        logs_storage: LogsStorage::new(data_dir.join("logs.db").as_str()).await?,
        allocations_checker,
        worker_id,
        queries_tx,
        queries_rx: UseOnce::new(queries_rx),
        last_collected_log_tx,
    })
}

impl<EventStream: Stream<Item = WorkerEvent>> P2PController<EventStream> {
    pub async fn run(&self, cancellation_token: CancellationToken) {
        run_all!(
            cancellation_token,
            self.run_event_loop(cancellation_token.child_token()),
            self.run_queries_loop(cancellation_token.child_token()),
            self.run_ping_loop(cancellation_token.child_token(), self.ping_interval),
            self.run_logs_loop(cancellation_token.child_token(), *LOGS_SEND_INTERVAL),
            self.worker.run(cancellation_token.child_token()),
            self.allocations_checker
                .run(cancellation_token.child_token()),
        );
    }

    async fn run_queries_loop(&self, cancellation_token: CancellationToken) {
        let queries_rx = self.queries_rx.take().unwrap();
        ReceiverStream::new(queries_rx)
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(CONCURRENT_QUERY_MESSAGES, |(peer_id, query)| async move {
                self.handle_query(peer_id, query).await;
            })
            .await;
        info!("Query processing task finished");
    }

    async fn run_ping_loop(&self, cancellation_token: CancellationToken, ping_interval: Duration) {
        let mut timer =
            tokio::time::interval_at(tokio::time::Instant::now() + ping_interval, ping_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(timer)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|_| async move {
                tracing::debug!("Sending ping");
                let status = self.worker.status();
                let ping = Ping {
                    stored_ranges: status
                        .available
                        .into_iter()
                        .map(|(dataset, ranges)| DatasetRanges {
                            url: dataset,
                            ranges: ranges.ranges,
                        })
                        .collect(),
                    worker_id: Some(self.worker_id.to_string()),
                    version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    stored_bytes: Some(status.stored_bytes),
                    ..Default::default()
                };
                let result = self.transport_handle.send_ping(ping);
                if let Err(err) = result {
                    warn!("Couldn't send ping: {:?}", err);
                }
            })
            .await;
        info!("Pings processing task finished");
    }

    async fn run_logs_loop(
        &self,
        cancellation_token: CancellationToken,
        interval: std::time::Duration,
    ) {
        let mut last_collected_log_rx = self.last_collected_log_tx.subscribe();

        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = timer.tick() => {
                    if !self.logs_storage.is_initialized() {
                        continue;
                    }
                    let mut last_seq_no = None;
                    loop {
                        let logs = match self.logs_storage.get_logs(last_seq_no).await {
                            Ok(logs) => logs,
                            Err(e) => {
                                warn!("Couldn't get logs from storage: {e:?}");
                                break;
                            }
                        };
                        let total_logs = logs.len();
                        if let Some(last_log) = logs.last() {
                            last_seq_no = Some(last_log.seq_no.unwrap());
                        } else {
                            break;
                        }

                        self.try_send_logs(logs);

                        if total_logs < logs_storage::LOGS_PER_PAGE {
                            break;
                        }
                    }
                },
                _ = last_collected_log_rx.changed() => {
                    let last_seq_no = *last_collected_log_rx.borrow_and_update();
                    self.logs_storage.logs_collected(last_seq_no).await;
                }
                _ = cancellation_token.cancelled() => {
                    break;
                },
            };
        }
        info!("Sending logs task finished");
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
                WorkerEvent::Pong(pong) => self.handle_pong(pong),
                WorkerEvent::Query { peer_id, query } => {
                    match self.queries_tx.try_send((peer_id, query)) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!("Queries queue is full. Dropping query from {peer_id}");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            break;
                        }
                    }
                }
                WorkerEvent::LogsCollected { last_seq_no } => {
                    let result = self.last_collected_log_tx.send(last_seq_no);
                    if result.is_err() {
                        break;
                    }
                }
            }
        }

        info!("Transport event loop finished");
    }

    fn handle_pong(&self, pong: Pong) {
        use sqd_messages::pong::Status;
        match pong.status {
            Some(Status::UnsupportedVersion(())) => {
                error!("Worker version not supported by the scheduler");
                metrics::set_status(metrics::WorkerStatus::UnsupportedVersion);
            }
            Some(Status::Jailed(reason)) => {
                warn!("Worker jailed until the end of epoch: {reason}");
                self.worker.stop_downloads();
                metrics::set_status(metrics::WorkerStatus::Jailed);
            }
            Some(Status::Active(assignment)) => {
                info!("Received pong from the scheduler");
                match parse_assignment(assignment) {
                    Ok((chunks, datasets_index)) => {
                        self.worker.set_datasets_index(datasets_index);
                        self.worker.set_desired_chunks(chunks);
                    }
                    Err(e) => warn!("Invalid assignment: {e:?}"),
                }
                metrics::set_status(metrics::WorkerStatus::Active);
            }
            None => {
                warn!("Invalid pong message: no status field");
            }
        }
    }

    async fn handle_query(&self, peer_id: PeerId, query: Query) {
        let query_id = query.query_id.clone().expect("got query without query_id");

        if !self.logs_storage.is_initialized() {
            warn!("Logs storage not initialized. Cannot execute queries yet.");
            return;
        }

        let result = self.process_query(peer_id, &query).await;
        if let Err(e) = &result {
            warn!("Query {query_id} execution failed: {e:?}");
        }
        metrics::query_executed(&result);

        let log = if let Err(QueryError::NoAllocation) = result {
            None
        } else {
            Some(self.generate_log(&result, query, peer_id))
        };
        self.send_query_result(query_id, result);
        if let Some(log) = log {
            let result = self.logs_storage.save_log(log).await;
            if let Err(e) = result {
                warn!("Couldn't save query log: {e:?}");
            }
        }
    }

    async fn process_query(&self, peer_id: PeerId, query: &Query) -> QueryResult {
        let (dataset, query_str) = validate_query(query)
            .map_err(|e| QueryError::BadRequest(format!("Query field missing: {e}")))?;
        let block_range = query
            .block_range
            .map(|sqd_messages::Range { begin, end }| (begin as u64, end as u64));
        match self.allocations_checker.try_spend(peer_id) {
            gateway_allocations::Status::Spent => {}
            gateway_allocations::Status::NotEnoughCU => return Err(QueryError::NoAllocation),
        };
        let result = self
            .worker
            .run_query(query_str, dataset, block_range, Some(peer_id))
            .await;
        if let Err(QueryError::ServiceOverloaded) = result {
            self.allocations_checker.refund(peer_id);
        }
        result
    }

    fn send_query_result(&self, query_id: String, result: QueryResult) {
        use sqd_messages::query_result;
        let query_result = match result {
            Ok(result) => query_result::Result::Ok(sqd_messages::OkResult {
                data: result.compressed_data(),
                exec_plan: None,
                last_block: result.last_block,
            }),
            Err(e @ QueryError::NotFound) => query_result::Result::BadRequest(e.to_string()),
            Err(QueryError::NoAllocation) => query_result::Result::NoAllocation(()),
            Err(QueryError::BadRequest(e)) => query_result::Result::BadRequest(e),
            Err(e @ QueryError::ServiceOverloaded) => {
                query_result::Result::ServerError(e.to_string())
            }
            Err(QueryError::Other(e)) => query_result::Result::ServerError(e.to_string()),
        };
        let query_result = sqd_messages::QueryResult {
            query_id,
            result: Some(query_result),
        };
        self.transport_handle
            .send_query_result(query_result)
            .unwrap_or_else(|_| error!("Cannot send query result: queue full"));
        // TODO: add retries
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.worker_id
    }

    fn try_send_logs(&self, logs: Vec<QueryExecuted>) {
        info!("Sending {} logs to the logs collector", logs.len());
        self.transport_handle
            .send_logs(logs)
            .unwrap_or_else(|_| log::warn!("Cannot send query logs: queue full"));
    }

    fn generate_log(
        &self,
        query_result: &QueryResult,
        query: Query,
        client_id: PeerId,
    ) -> QueryExecuted {
        use query_executed::Result;

        let result = match query_result {
            Ok(result) => Result::Ok(InputAndOutput {
                num_read_chunks: Some(result.num_read_chunks as u32),
                output: Some(SizeAndHash {
                    size: Some(result.data.len() as u32),
                    sha3_256: result.sha3_256(),
                }),
            }),
            Err(e @ QueryError::NotFound) => Result::BadRequest(e.to_string()),
            Err(QueryError::BadRequest(e)) => Result::BadRequest(e.clone()),
            Err(e @ QueryError::ServiceOverloaded) => Result::ServerError(e.to_string()),
            Err(QueryError::Other(e)) => Result::ServerError(e.to_string()),
            Err(QueryError::NoAllocation) => panic!("Shouldn't send logs with NoAllocation error"),
        };
        let exec_time = match query_result {
            Ok(result) => result.exec_time.as_millis() as u32,
            Err(_) => 0,
        };
        let query_hash = sha3_256(
            query
                .query
                .as_ref()
                .expect("Hashing empty query")
                .as_bytes(),
        );

        QueryExecuted {
            client_id: client_id.to_string(),
            worker_id: self.worker_id.to_string(),
            query_hash,
            query: Some(query),
            result: Some(result),
            exec_time_ms: Some(exec_time),
            ..Default::default()
        }
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

/// Validate if all required query fields are present, return (dataset, query_str)
fn validate_query(query: &Query) -> Result<(String, String), &'static str> {
    let dataset = query.dataset.as_ref().ok_or("dataset")?.clone();
    let query_str = query.query.as_ref().ok_or("query")?.clone();
    _ = query.query_id.as_ref().ok_or("query_id")?;
    _ = query.profiling.as_ref().ok_or("profiling")?;
    _ = query.client_state_json.as_ref().ok_or("client_state")?;
    Ok((dataset, query_str))
}
