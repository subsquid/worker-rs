use std::{env, time::Duration};

use anyhow::Result;
use camino::Utf8PathBuf as PathBuf;
use futures::{Stream, StreamExt};
use lazy_static::lazy_static;
use subsquid_messages::{
    query_executed, DatasetRanges, InputAndOutput, Ping, Pong, Query, QueryExecuted, SizeAndHash,
    WorkerAssignment,
};
use subsquid_network_transport::{
    P2PTransportBuilder, PeerId, TransportArgs, WorkerConfig, WorkerEvent, WorkerTransportHandle,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, log, warn};

use crate::{
    logs_storage::LogsStorage,
    metrics,
    query::{error::QueryError, result::QueryResult},
    util::{hash::sha3_256, UseOnce},
};

use super::QueryTask;

const SERVICE_QUEUE_SIZE: usize = 16;
const CONCURRENT_MESSAGES: usize = 32;

lazy_static! {
    static ref LOGS_SEND_INTERVAL: Duration = Duration::from_secs(
        env::var("LOGS_SEND_INTERVAL_SEC")
            .map(|s| s.parse().expect("Invalid LOGS_SEND_INTERVAL_SEC"))
            .unwrap_or(600)
    );
}

pub struct P2PTransport<EventStream> {
    raw_event_stream: UseOnce<EventStream>,
    transport_handle: WorkerTransportHandle,
    logs_storage: LogsStorage,
    worker_id: PeerId,
    assignments_tx: watch::Sender<WorkerAssignment>,
    assignments_rx: UseOnce<watch::Receiver<WorkerAssignment>>,
    queries_tx: mpsc::Sender<QueryTask>,
    queries_rx: UseOnce<mpsc::Receiver<QueryTask>>,
}

pub async fn create_p2p_transport(
    args: TransportArgs,
    scheduler_id: PeerId,
    logs_collector_id: PeerId,
    logs_db_path: PathBuf,
) -> Result<P2PTransport<impl Stream<Item = WorkerEvent>>> {
    let transport_builder = P2PTransportBuilder::from_cli(args).await?;
    let worker_id = transport_builder.local_peer_id();
    info!("Local peer ID: {worker_id}");

    let (assignments_tx, assignments_rx) = watch::channel(Default::default());
    let (queries_tx, queries_rx) = mpsc::channel(SERVICE_QUEUE_SIZE);
    let (event_stream, transport_handle) =
        transport_builder.build_worker(WorkerConfig::new(scheduler_id, logs_collector_id))?;

    Ok(P2PTransport {
        raw_event_stream: UseOnce::new(event_stream),
        transport_handle,
        logs_storage: LogsStorage::new(logs_db_path.as_str()).await?,
        worker_id,
        assignments_tx,
        assignments_rx: UseOnce::new(assignments_rx),
        queries_tx,
        queries_rx: UseOnce::new(queries_rx),
    })
}

impl<EventStream: Stream<Item = WorkerEvent>> P2PTransport<EventStream> {
    async fn run_receive_loop(&self, cancellation_token: CancellationToken) {
        let event_stream = self.raw_event_stream.take().unwrap();
        event_stream
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(CONCURRENT_MESSAGES, |ev| async move {
                match ev {
                    WorkerEvent::Pong(pong) => self.handle_pong(pong),
                    WorkerEvent::Query { peer_id, query } => {
                        self.handle_query(peer_id, query).await
                    }
                    WorkerEvent::LogsCollected { last_seq_no } => {
                        self.handle_logs_collected(last_seq_no).await
                    }
                }
            })
            .await;
    }

    async fn run_send_logs_loop(
        &self,
        cancellation_token: CancellationToken,
        interval: std::time::Duration,
    ) {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select!(
                _ = timer.tick() => {},
                _ = cancellation_token.cancelled() => {
                    break;
                },
            );

            if !self.logs_storage.is_initialized().await {
                continue;
            }
            let logs = match self.logs_storage.get_logs().await {
                Ok(logs) => logs,
                Err(e) => {
                    warn!("Couldn't get logs from storage: {e:?}");
                    continue;
                }
            };

            self.try_send_logs(logs);
        }
    }

    fn handle_pong(&self, pong: Pong) {
        use subsquid_messages::pong::Status;
        match pong.status {
            Some(Status::NotRegistered(())) => {
                error!("Worker not registered on chain");
                metrics::set_status(metrics::WorkerStatus::NotRegistered);
            }
            Some(Status::UnsupportedVersion(())) => {
                error!("Worker version not supported by the scheduler");
                metrics::set_status(metrics::WorkerStatus::UnsupportedVersion);
            }
            Some(Status::Jailed(reason)) => {
                warn!("Worker jailed until the end of epoch: {reason}");
                metrics::set_status(metrics::WorkerStatus::Jailed);
            }
            Some(Status::Active(_)) => {
                error!("Deprecated pong message format");
            }
            Some(Status::ActiveV2(assignment)) => {
                info!("Received pong from the scheduler");
                self.assignments_tx
                    .send(assignment)
                    .expect("Assignment subscriber dropped");
                metrics::set_status(metrics::WorkerStatus::Active);
            }
            None => {
                warn!("Invalid pong message: no status field");
            }
        }
    }

    async fn handle_logs_collected(&self, last_collected_seq_no: Option<u64>) {
        self.logs_storage
            .logs_collected(last_collected_seq_no)
            .await;
    }

    // Completes only when the query is processed and the result is sent
    async fn handle_query(&self, peer_id: PeerId, query: Query) {
        let query_id = query.query_id.clone().expect("got query without query_id");

        if !self.logs_storage.is_initialized().await {
            warn!("Logs storage not initialized. Cannot execute queries yet.");
            return;
        }

        let result = self.process_query(peer_id, &query).await;
        if let Err(e) = &result {
            warn!("Query {query_id} execution failed: {e:?}");
        }

        let log = if let Err(QueryError::NoAllocation) = result {
            None
        } else {
            Some(self.generate_log(&result, query, peer_id))
        };
        self.send_query_result(query_id, result).await;
        if let Some(log) = log {
            let result = self.logs_storage.save_log(log).await;
            if let Err(e) = result {
                warn!("Couldn't save query log: {e:?}");
            }
        }
    }

    async fn process_query(
        &self,
        peer_id: PeerId,
        query: &Query,
    ) -> std::result::Result<QueryResult, QueryError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        if let (Some(dataset), Some(query_str)) = (&query.dataset, &query.query) {
            let query = serde_json::from_str(query_str)
                .map_err(|e| QueryError::BadRequest(e.to_string()))?;
            match self.queries_tx.try_send(QueryTask {
                dataset: dataset.clone(),
                peer_id,
                query,
                response_sender: resp_tx,
            }) {
                Err(mpsc::error::TrySendError::Full(_)) => {
                    return Err(QueryError::ServiceOverloaded);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    panic!("Query subscriber dropped");
                }
                Ok(_) => {
                    metrics::PENDING_QUERIES.inc();
                }
            }
        } else {
            Err(QueryError::BadRequest(
                "Some fields are missing in proto message".to_owned(),
            ))?;
        }
        resp_rx
            .await
            .expect("Query processor didn't produce a result")
    }

    async fn send_query_result(
        &self,
        query_id: String,
        result: std::result::Result<QueryResult, QueryError>,
    ) {
        use subsquid_messages::query_result;
        let query_result = match result {
            Ok(result) => query_result::Result::Ok(subsquid_messages::OkResult {
                data: result.compressed_data,
                exec_plan: None,
            }),
            Err(e @ QueryError::NotFound) => query_result::Result::BadRequest(e.to_string()),
            Err(QueryError::NoAllocation) => query_result::Result::NoAllocation(()),
            Err(QueryError::BadRequest(e)) => query_result::Result::BadRequest(e),
            Err(e @ QueryError::ServiceOverloaded) => {
                query_result::Result::ServerError(e.to_string())
            }
            Err(QueryError::Other(e)) => query_result::Result::ServerError(e.to_string()),
        };
        let query_result = subsquid_messages::QueryResult {
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
        query_result: &std::result::Result<QueryResult, QueryError>,
        query: Query,
        client_id: PeerId,
    ) -> QueryExecuted {
        let result = match query_result {
            Ok(result) => query_executed::Result::Ok(InputAndOutput {
                num_read_chunks: Some(result.num_read_chunks as u32),
                output: Some(SizeAndHash {
                    size: Some(result.data_size as u32),
                    sha3_256: result.data_sha3_256.clone(),
                }),
            }),
            Err(e @ QueryError::NotFound) => query_executed::Result::BadRequest(e.to_string()),
            Err(QueryError::BadRequest(e)) => query_executed::Result::BadRequest(e.clone()),
            Err(e @ QueryError::ServiceOverloaded) => {
                query_executed::Result::ServerError(e.to_string())
            }
            Err(QueryError::Other(e)) => query_executed::Result::ServerError(e.to_string()),
            Err(QueryError::NoAllocation) => panic!("Shouldn't send logs with NoAllocation error"),
        };
        let query_hash = sha3_256(
            query
                .query
                .as_ref()
                .expect("Hashing empty query")
                .as_bytes(),
        );
        QueryExecuted {
            client_id: client_id.to_base58(),
            worker_id: self.worker_id.to_base58(),
            query_hash,
            query: Some(query),
            result: Some(result),
            exec_time_ms: Some(0), // TODO: measure execution time
            ..Default::default()
        }
    }
}

impl<EventStream: Stream<Item = WorkerEvent> + Send> super::Transport
    for P2PTransport<EventStream>
{
    async fn send_ping(&self, state: super::State) -> Result<()> {
        let ping = Ping {
            stored_ranges: state
                .datasets
                .into_iter()
                .map(|(dataset, ranges)| DatasetRanges {
                    url: dataset,
                    ranges: ranges.ranges,
                })
                .collect(),
            worker_id: Some(self.worker_id.to_string()),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            stored_bytes: Some(state.stored_bytes),
            ..Default::default()
        };
        self.transport_handle.send_ping(ping)?;
        Ok(())
    }

    fn stream_assignments(&self) -> impl futures::Stream<Item = WorkerAssignment> + 'static {
        let rx = self.assignments_rx.take().unwrap();
        WatchStream::from_changes(rx)
    }

    fn stream_queries(&self) -> impl futures::Stream<Item = super::QueryTask> + 'static {
        let rx = self.queries_rx.take().unwrap();
        ReceiverStream::new(rx).inspect(|_| {
            metrics::PENDING_QUERIES.dec();
        })
    }

    async fn run(&self, cancellation_token: CancellationToken) {
        tokio::join!(
            self.run_receive_loop(cancellation_token.clone()),
            self.run_send_logs_loop(cancellation_token, *LOGS_SEND_INTERVAL),
        );
    }
}
