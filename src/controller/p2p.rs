use std::{env, io::Write, sync::Arc, time::Duration};

use anyhow::Result;
use camino::Utf8PathBuf as PathBuf;
use flate2::{write::DeflateEncoder, Compression};
use futures::{Stream, StreamExt};
use sqd_contract_client::Network;
use sqd_messages::{query_error, query_executed, BitString, Heartbeat, Query, QueryExecuted};
use sqd_network_transport::{
    P2PTransportBuilder, PeerId, WorkerConfig, WorkerEvent, WorkerTransportHandle,
};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use parking_lot::Mutex;

use crate::{
    cli::{self, Args},
    gateway_allocations::{self, allocations_checker::AllocationsChecker},
    logs_storage::LogsStorage,
    metrics,
    query::result::{QueryError, QueryResult},
    run_all,
    storage::datasets_index::DatasetsIndex,
    util::{assignment::{self, Assignment}, timestamp_now_ms, UseOnce},
};

use super::worker::Worker;

const WORKER_VERSION: &str = env!("CARGO_PKG_VERSION");
const QUERIES_POOL_SIZE: usize = 16;
const CONCURRENT_QUERY_MESSAGES: usize = 32;

pub struct P2PController<EventStream> {
    worker: Arc<Worker>,
    heartbeat_interval: Duration,
    assignment_check_interval: Duration,
    raw_event_stream: UseOnce<EventStream>,
    transport_handle: WorkerTransportHandle,
    logs_storage: LogsStorage,
    allocations_checker: AllocationsChecker,
    worker_id: PeerId,
    private_key: Vec<u8>,
    network: Network,
    latest_assignment: Mutex<Option<String>>,
    queries_tx: mpsc::Sender<(PeerId, Query)>,
    queries_rx: UseOnce<mpsc::Receiver<(PeerId, Query)>>,
}

pub async fn create_p2p_controller(
    worker: Arc<Worker>,
    transport_builder: P2PTransportBuilder,
    allocations_checker: AllocationsChecker,
    scheduler_id: PeerId,
    logs_collector_id: PeerId,
    args: Args,
) -> Result<P2PController<impl Stream<Item = WorkerEvent>>> {
    let data_dir = args.data_dir.clone();
    let heartbeat_interval = args.ping_interval;
    let assignment_check_interval = args.assignment_check_interval;
    let network: Network;
    if let cli::Mode::P2P(p2p_args) = &args.mode {
        network = p2p_args.transport.rpc.network;
    } else {
        panic!("P2PContoller needs p2p config");
    };

    let worker_id = transport_builder.local_peer_id();
    let private_key = transport_builder.keypair().try_into_ed25519().unwrap().secret().as_ref().to_vec();
    info!("Local peer ID: {worker_id}");
    check_peer_id(worker_id, data_dir.join("peer_id"));

    let mut config = WorkerConfig::new();
    config.service_nodes = vec![scheduler_id, logs_collector_id];
    let (event_stream, transport_handle) =
        transport_builder.build_worker(config).await?;

    let (queries_tx, queries_rx) = mpsc::channel(QUERIES_POOL_SIZE);

    Ok(P2PController {
        worker,
        heartbeat_interval,
        assignment_check_interval,
        raw_event_stream: UseOnce::new(event_stream),
        transport_handle,
        logs_storage: LogsStorage::new(data_dir.join("logs.db").as_str()).await?,
        allocations_checker,
        worker_id,
        private_key,
        network,
        latest_assignment: None.into(),
        queries_tx,
        queries_rx: UseOnce::new(queries_rx),
    })
}

impl<EventStream: Stream<Item = WorkerEvent>> P2PController<EventStream> {
    pub async fn run(&self, cancellation_token: CancellationToken) {
        run_all!(
            cancellation_token,
            self.run_event_loop(cancellation_token.child_token()),
            self.run_queries_loop(cancellation_token.child_token()),
            self.run_heartbeat_loop(cancellation_token.child_token(), self.heartbeat_interval),
            self.run_assignments_loop(cancellation_token.child_token(), self.assignment_check_interval),
            // self._run_logs_loop(cancellation_token.child_token(), *LOGS_SEND_INTERVAL),
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

    async fn run_heartbeat_loop(&self, cancellation_token: CancellationToken, heartbeat_interval: Duration) {
        let mut timer =
            tokio::time::interval_at(tokio::time::Instant::now() + heartbeat_interval, heartbeat_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(timer)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|_| async move {
                tracing::debug!("Sending heartbeat");
                let status = self.worker.status();
                let mut encoder = DeflateEncoder::new(Vec::new(), Compression::best());
                let _ = encoder.write_all(status.unavailability_map.iter().map(|v| *v as u8).collect::<Vec<u8>>().as_slice());
                let compressed_bytes = encoder.finish().unwrap();
                let data_len = status.unavailability_map.len();
                let heartbeat = Heartbeat {
                    assignment_id: "".to_owned(), // TODO
                    missing_chunks: Some(BitString {
                        data: compressed_bytes,
                        size: data_len as u64,
                    }),
                    version: WORKER_VERSION.to_string(),
                    stored_bytes: Some(status.stored_bytes),
                    ..Default::default()
                };
                let result = self.transport_handle.send_heartbeat(heartbeat);
                if let Err(err) = result {
                    warn!("Couldn't send heartbeat: {:?}", err);
                }
            })
            .await;
        info!("Heartbeat processing task finished");
    }

    async fn run_assignments_loop(&self, cancellation_token: CancellationToken, assignment_check_interval: Duration) {
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
                let network_state_url = format!("https://metadata.sqd-datasets.io/{network_state_filename}");
                let mut latest_assignment = self.latest_assignment.lock();
                if let Ok(assignment_option) = Assignment::try_download(network_state_url, latest_assignment.clone()).await {
                    if let Some(assignment) = assignment_option {
                        let peer_id = self.worker.peer_id.unwrap();
                        let private_key = self.private_key.clone();
                        let calculated_chunks = assignment.dataset_chunks_for_peer_id(peer_id.to_string()).unwrap();
                        let headers = assignment.headers_for_peer_id(peer_id.to_string(), private_key).unwrap();
                        let datasets_index = DatasetsIndex::from(calculated_chunks, headers);
                        let chunks = datasets_index.create_chunks_set();
                        self.worker.set_datasets_index(datasets_index);
                        self.worker.set_desired_chunks(chunks);
                        *latest_assignment = Some(assignment.id);
                        info!("New assignment applied");
                    };
                } else {
                    error!("Unable to get assignment");
                }
            })
            .await;
        info!("Assignment processing task finished");
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
                WorkerEvent::LogsRequest { .. } => todo!(),
            }
        }

        info!("Transport event loop finished");
    }

    async fn handle_query(&self, peer_id: PeerId, query: Query) {
        let query_id = query.query_id.clone();

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
        let block_range = query
            .block_range
            .map(|sqd_messages::Range { begin, end }| (begin as u64, end as u64));
        match self.allocations_checker.try_spend(peer_id) {
            gateway_allocations::Status::Spent => {}
            gateway_allocations::Status::NotEnoughCU => return Err(QueryError::NoAllocation),
        };
        let result = self
            .worker
            .run_query(
                query.query.clone(),
                query.dataset.clone(),
                block_range,
                Some(peer_id),
            )
            .await;
        if let Err(QueryError::ServiceOverloaded) = result {
            self.allocations_checker.refund(peer_id);
        }
        result
    }

    fn send_query_result(&self, query_id: String, result: QueryResult) {
        use sqd_messages::query_result;
        let query_result = match result {
            Ok(result) => {
                let data = result.compressed_data();
                query_result::Result::Ok(sqd_messages::QueryOk {
                    data,
                    last_block: result.last_block,
                })
            }
            Err(e @ QueryError::NotFound) => query_error::Err::BadRequest(e.to_string()).into(),
            Err(QueryError::NoAllocation) => query_error::Err::TooManyRequests(()).into(),
            Err(QueryError::BadRequest(e)) => query_error::Err::BadRequest(e).into(),
            Err(e @ QueryError::ServiceOverloaded) => {
                query_error::Err::ServerError(e.to_string()).into()
            }
            Err(QueryError::Other(e)) => query_error::Err::ServerError(e.to_string()).into(),
        };
        let query_result = sqd_messages::QueryResult {
            query_id,
            result: Some(query_result),
            retry_after_ms: None,
            signature: Default::default(),
        };
        self.transport_handle
            .send_query_result(query_result)
            .unwrap_or_else(|_| error!("Cannot send query result: queue full"));
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.worker_id
    }

    fn generate_log(
        &self,
        query_result: &QueryResult,
        query: Query,
        client_id: PeerId,
    ) -> QueryExecuted {
        use query_executed::Result;

        let result = match query_result {
            Ok(result) => Result::Ok(sqd_messages::QueryOkSummary {
                uncompressed_data_size: result.data.len() as u64,
                data_hash: result.sha3_256(),
                last_block: result.last_block,
            }),
            Err(e @ QueryError::NotFound) => query_error::Err::BadRequest(e.to_string()).into(),
            Err(QueryError::BadRequest(e)) => query_error::Err::BadRequest(e.clone()).into(),
            Err(e @ QueryError::ServiceOverloaded) => {
                query_error::Err::ServerError(e.to_string()).into()
            }
            Err(QueryError::Other(e)) => query_error::Err::ServerError(e.to_string()).into(),
            Err(QueryError::NoAllocation) => panic!("Shouldn't send logs with NoAllocation error"),
        };
        let exec_time = match query_result {
            Ok(result) => result.exec_time.as_micros() as u32,
            Err(_) => 0, // TODO: always measure execution time
        };

        QueryExecuted {
            client_id: client_id.to_string(),
            query: Some(query),
            exec_time_micros: exec_time,
            timestamp_ms: timestamp_now_ms(), // TODO: use time of receiving query
            result: Some(result),
            worker_version: WORKER_VERSION.to_string(),
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
