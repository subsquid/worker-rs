use std::{collections::HashMap, str::FromStr, sync::Arc};

use reqwest::Url;
use sqd_network_transport::{Keypair, PeerId};
use tracing::error;

use crate::types::{
    dataset::Dataset,
    state::{ChunkRef, ChunkSet},
};

use super::layout::DataChunk;

pub struct DatasetsIndex {
    assignment: sqd_assignments::Assignment,
    assignment_id: String,
    peer_id: PeerId,
    http_headers: reqwest::header::HeaderMap,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub url: Url,
    pub name: String,
}

impl DatasetsIndex {
    pub fn list_files(&self, dataset: &Dataset, chunk: &DataChunk) -> Option<Vec<RemoteFile>> {
        let chunk = self
            .assignment
            .find_chunk(dataset, *chunk.first_block)
            .ok()?;
        let base_url = Url::from_str(&chunk.dataset_base_url())
            .inspect_err(|e| {
                tracing::warn!(
                    "Can't parse dataset base url '{}': {e}",
                    chunk.dataset_base_url()
                )
            })
            .ok()?;
        let base_url = base_url
            .join(&format!("{}/", chunk.base_url()))
            .inspect_err(|e| {
                tracing::warn!("Can't parse chunk base url '{}': {e}", chunk.base_url())
            })
            .ok()?;
        let mut result = Vec::with_capacity(chunk.files().len());
        for file in chunk.files() {
            result.push(RemoteFile {
                name: file.filename().to_owned(),
                url: base_url
                    .join(file.url())
                    .inspect_err(|e| tracing::warn!("Can't parse file url '{}': {e}", file.url()))
                    .ok()?,
            });
        }

        Some(result)
    }
    pub fn new(
        assignment: sqd_assignments::Assignment,
        id: impl Into<String>,
        key: &Keypair,
    ) -> anyhow::Result<Self> {
        let peer_id = key.public().to_peer_id();
        let Some(worker) = assignment.get_worker(peer_id) else {
            anyhow::bail!("No assignment for this worker");
        };
        let headers = worker.decrypt_headers(key)?;
        let http_headers = headers
            .into_iter()
            .filter_map(|(k, v)| {
                let key = reqwest::header::HeaderName::from_str(&k)
                    .inspect_err(|err| error!("Couldn't parse header name: {}: {err:?}", k))
                    .ok()?;
                let val = reqwest::header::HeaderValue::from_str(&v)
                    .inspect_err(|err| error!("Couldn't parse header value: {}: {err:?}", k))
                    .ok()?;
                Some((key, val))
            })
            .collect();

        Ok(Self {
            assignment,
            assignment_id: id.into(),
            peer_id,
            http_headers,
        })
    }

    pub fn create_chunks_set(&self) -> ChunkSet {
        let mut chunk_set = ChunkSet::new();
        let Some(worker) = self.assignment.get_worker(self.peer_id) else {
            return chunk_set;
        };
        let mut pool = StringPool::default();
        for chunk in worker.chunks() {
            match DataChunk::from_str(chunk.id()) {
                Ok(id) => {
                    let chunk = ChunkRef {
                        dataset: pool.get(chunk.dataset_id()),
                        chunk: id,
                    };
                    if let Some(last) = chunk_set.last() {
                        debug_assert!(
                            last < &chunk,
                            "Assigned chunks are not sorted: {last} >= {chunk}"
                        );
                    }
                    chunk_set.insert(chunk);
                }
                Err(e) => tracing::warn!("Couldn't parse chunk id {}: {e}", chunk.id()),
            }
        }
        chunk_set
    }

    pub fn get_headers(&self) -> &reqwest::header::HeaderMap {
        &self.http_headers
    }

    pub fn assignment_id(&self) -> &str {
        &self.assignment_id
    }
}

#[derive(Default)]
struct StringPool {
    map: HashMap<String, Arc<String>>,
}

impl StringPool {
    fn get(&mut self, s: &str) -> Arc<String> {
        match self.map.get(s) {
            Some(s) => s.clone(),
            None => {
                let key = s.to_owned();
                let value = Arc::new(s.to_owned());
                self.map.insert(key, value.clone());
                value
            }
        }
    }
}

#[test]
fn test_url_joining() {
    let base_url = Url::from_str("https://eclipse-testnet-2.sqd-datasets.io/").unwrap();
    let url = base_url
        .join(&format!("{}/", "0086800000/0089600001-0089800000-cg1JNYDM"))
        .unwrap()
        .join("blocks.parquet")
        .unwrap();
    assert_eq!(url.as_str(), "https://eclipse-testnet-2.sqd-datasets.io/0086800000/0089600001-0089800000-cg1JNYDM/blocks.parquet");
}
