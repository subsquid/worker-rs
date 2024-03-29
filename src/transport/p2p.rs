use std::io::Write;

use anyhow::{bail, Result};
use futures::StreamExt;
use serde_json::Value as JsonValue;
use subsquid_messages::{
    envelope::Msg, signatures::SignedMessage, DatasetRanges, Ping, Pong, ProstMsg, Query,
};
use subsquid_network_transport::{
    cli::TransportArgs,
    transport::{P2PTransportBuilder, P2PTransportHandle},
    Keypair, PeerId,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{types::state::Ranges, util::UseOnce};

use super::QueryTask;

type MsgContent = Vec<u8>;
type Message = subsquid_network_transport::Message<MsgContent>;
const PING_TOPIC: &str = "worker_ping";
const LOGS_TOPIC: &str = "worker_query_logs";
const SERVICE_QUEUE_SIZE: usize = 16;
const CONCURRENT_MESSAGES: usize = 32;

pub struct P2PTransport {
    raw_msg_receiver: UseOnce<mpsc::Receiver<Message>>,
    transport_handle: P2PTransportHandle<MsgContent>,
    scheduler_id: PeerId,
    worker_id: PeerId,
    keypair: Keypair,
    assignments_tx: watch::Sender<Ranges>,
    assignments_rx: UseOnce<watch::Receiver<Ranges>>,
    queries_tx: mpsc::Sender<QueryTask>,
    queries_rx: UseOnce<mpsc::Receiver<QueryTask>>,
}

impl P2PTransport {
    pub async fn from_cli(args: TransportArgs, scheduler_id: String) -> Result<Self> {
        let transport_builder = P2PTransportBuilder::from_cli(args).await?;
        let worker_id = transport_builder.local_peer_id();
        let keypair = transport_builder.keypair();
        info!("Local peer ID: {worker_id}");

        let (assignments_tx, assignments_rx) = watch::channel(Default::default());
        let (queries_tx, queries_rx) = mpsc::channel(SERVICE_QUEUE_SIZE);
        let (msg_receiver, transport_handle) = transport_builder.run().await?;
        transport_handle.subscribe(PING_TOPIC).await?;
        transport_handle.subscribe(LOGS_TOPIC).await?;

        Ok(Self {
            raw_msg_receiver: UseOnce::new(msg_receiver),
            transport_handle,
            scheduler_id: scheduler_id.parse()?,
            worker_id,
            keypair,
            assignments_tx,
            assignments_rx: UseOnce::new(assignments_rx),
            queries_tx,
            queries_rx: UseOnce::new(queries_rx),
        })
    }

    // TODO: switch to latest implementation
    pub async fn stop(&self) -> Result<()> {
        Ok(self.transport_handle.stop().await?)
    }

    async fn run(&self, cancellation_token: CancellationToken) {
        let msg_receiver = self.raw_msg_receiver.take().unwrap();
        ReceiverStream::new(msg_receiver)
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(CONCURRENT_MESSAGES, |msg| async move {
                let envelope = match subsquid_messages::Envelope::decode(msg.content.as_slice()) {
                    Ok(envelope) => envelope,
                    Err(e) => {
                        warn!("Couldn't parse p2p message: {e}");
                        return;
                    }
                };
                let peer_id = match msg.peer_id {
                    Some(peer_id) => peer_id,
                    None => {
                        warn!("Received p2p message without peer_id: '{:?}'", msg);
                        return;
                    }
                };
                match envelope.msg {
                    Some(Msg::Pong(pong)) => {
                        self.handle_pong(peer_id, pong);
                    }
                    Some(Msg::Query(query)) => {
                        self.handle_query(peer_id, query).await;
                    }
                    _ => {
                        // ignore all other events
                    }
                }
            })
            .await;
    }

    fn handle_pong(&self, peer_id: PeerId, pong: Pong) {
        use subsquid_messages::pong::Status;
        if peer_id != self.scheduler_id {
            warn!("Wrong pong message origin: '{}'", peer_id.to_string());
        }
        match pong.status {
            Some(Status::NotRegistered(())) => {
                error!("Worker not registered on chain");
            }
            Some(Status::UnsupportedVersion(())) => {
                error!("Worker version not supported by the scheduler");
            }
            Some(Status::JailedV1(())) => {
                warn!("Worker jailed until the end of epoch");
            }
            Some(Status::Jailed(reason)) => {
                warn!("Worker jailed until the end of epoch: {reason}");
            }
            Some(Status::Active(assignment)) => {
                self.assignments_tx
                    .send(assignment.datasets)
                    .expect("Assignment subscriber dropped");
            }
            None => {
                warn!("Invalid pong message: no status field");
            }
        }
    }

    // Completes only when the query is processed and the result is sent
    async fn handle_query(&self, peer_id: PeerId, mut query: Query) {
        if !query.verify_signature(&peer_id) {
            warn!(
                "Query with invalid signature received from {}",
                peer_id.to_string()
            );
            return;
        }
        let query_id = if let Some(query_id) = query.query_id.clone() {
            query_id
        } else {
            warn!(
                "Query without query_id received from {}",
                peer_id.to_string()
            );
            return;
        };
        let result = self.process_query(query).await;
        match result {
            Ok(values) => match values.try_into() {
                Ok(query_result) => {
                    self.send_query_result(query_id, peer_id, Ok(query_result))
                        .await
                }
                Err(e) => {
                    error!("Couldn't encode query result: {e:?}");
                    return;
                }
            },
            Err(e) => {
                warn!("Query {query_id} execution failed: {e:?}");
                self.send_query_result(query_id, peer_id, Err(e)).await
            }
        };
        // TODO: send logs
    }

    async fn process_query(&self, query: Query) -> Result<Vec<JsonValue>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        if let (Some(dataset), Some(query_str)) = (query.dataset, query.query) {
            match self.queries_tx.try_send(QueryTask {
                dataset,
                query: serde_json::from_str(&query_str)?,
                response_sender: resp_tx,
            }) {
                Err(mpsc::error::TrySendError::Full(_)) => {
                    bail!("Service overloaded");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    panic!("Query subscriber dropped");
                }
                _ => {}
            }
        } else {
            bail!("Some fields are missing in proto message");
        }
        resp_rx
            .await
            .expect("Query processor didn't produce a result")
            .map_err(From::from)
    }

    async fn send_query_result(
        &self,
        query_id: String,
        peer_id: PeerId,
        result: Result<QueryResult>,
    ) {
        use subsquid_messages::query_result;
        let query_result = match result {
            Ok(result) => query_result::Result::Ok(subsquid_messages::OkResult {
                data: result.compressed_data,
                exec_plan: None,
            }),
            // TODO: use separate error types for bad request and internal error
            Err(e) => query_result::Result::BadRequest(e.to_string()),
        };
        let envelope = subsquid_messages::Envelope {
            msg: Some(Msg::QueryResult(subsquid_messages::QueryResult {
                query_id,
                result: Some(query_result),
            })),
        };
        if let Err(e) = self
            .transport_handle
            .send_direct_msg(envelope.encode_to_vec(), peer_id)
            .await
        {
            error!("Couldn't send query result: {e:?}");
            // TODO: add retries
        }
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.worker_id
    }
}

impl super::Transport for P2PTransport {
    async fn send_ping(&self, state: super::State) -> Result<()> {
        let mut ping = Ping {
            stored_ranges: state
                .datasets
                .into_iter()
                .map(|(dataset, ranges)| DatasetRanges {
                    url: dataset,
                    ranges: ranges.ranges,
                })
                .collect(),
            worker_id: Some(self.worker_id.to_string()),
            version: Some("0.2.1".to_owned()),
            ..Default::default()
        };
        ping.sign(&self.keypair)?;
        let envelope = subsquid_messages::Envelope {
            msg: Some(Msg::Ping(ping)),
        };
        self.transport_handle
            .send_direct_msg(envelope.encode_to_vec(), self.scheduler_id)
            .await?;
        Ok(())
    }

    fn stream_assignments(&self) -> impl futures::Stream<Item = Ranges> + 'static {
        let rx = self.assignments_rx.take().unwrap();
        WatchStream::from_changes(rx)
    }

    fn stream_queries(&self) -> impl futures::Stream<Item = super::QueryTask> + 'static {
        let rx = self.queries_rx.take().unwrap();
        ReceiverStream::new(rx)
    }

    async fn run(&self, cancellation_token: CancellationToken) {
        self.run(cancellation_token).await
    }
}

struct QueryResult {
    compressed_data: Vec<u8>,
    data_size: usize,
    data_sha3_256: Option<Vec<u8>>,
}

impl TryFrom<Vec<JsonValue>> for QueryResult {
    type Error = anyhow::Error;

    fn try_from(value: Vec<JsonValue>) -> Result<Self> {
        use flate2::write::GzEncoder;
        use sha3::{Digest, Sha3_256};

        let data = serde_json::to_vec(&value)?;
        let data_size = data.len();

        let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&data)?;
        let compressed_data = encoder.finish()?;

        let mut hasher = Sha3_256::new();
        hasher.update(data);
        let hash = hasher.finalize();

        Ok(Self {
            compressed_data,
            data_size,
            data_sha3_256: Some(hash.to_vec()),
        })
    }
}
