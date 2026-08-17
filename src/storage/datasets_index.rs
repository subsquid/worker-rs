use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use reqwest::Url;
use sqd_network_transport::Keypair;
use tracing::error;

use crate::types::schema::SchemaId;
use crate::types::state::ChunkRef;
use sqd_assignments::ChunkRef as ChunkAssignmentRef;

pub enum AssignmentBlob {
    Legacy(sqd_assignments::Assignment),
    Worker(sqd_assignments::WorkerAssignment),
}

pub struct DatasetsIndex {
    assignment: AssignmentBlob,
    assignment_id: String,
    status: sqd_assignments::WorkerStatus,
    http_headers: reqwest::header::HeaderMap,
    // chunks assigned to this worker
    chunks: HashMap<ChunkRef, ChunkAssignmentRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkSchema {
    Pinned(SchemaId),
    Unpinned,
    Unassigned,
    NoAssignment,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub url: Url,
    pub name: String,
}

impl DatasetsIndex {
    /// Returns the remote files (URL + filename) associated with the given
    /// chunk, or `None` if the chunk is not in the assignment or any URL
    /// fails to parse.
    pub fn list_files(&self, chunk: &ChunkRef) -> Option<Vec<RemoteFile>> {
        let chunk_ref = self.chunks.get(chunk)?;
        match &self.assignment {
            AssignmentBlob::Legacy(assignment) => {
                let chunk = assignment.get_chunk(*chunk_ref)?;
                let base_url = chunk_base_url(chunk.dataset_base_url(), chunk.base_url())?;
                let mut result = Vec::with_capacity(chunk.files().len());
                for file in chunk.files() {
                    result.push(RemoteFile {
                        name: file.filename().to_owned(),
                        url: base_url
                            .join(file.url())
                            .inspect_err(|e| {
                                tracing::warn!("Can't parse file url '{}': {e}", file.url())
                            })
                            .ok()?,
                    });
                }
                Some(result)
            }
            AssignmentBlob::Worker(assignment) => {
                let chunk = assignment.get_chunk(*chunk_ref)?;
                // The chunk composes its own url: the dataset's base, the prefix of the
                // generation its `version` names, then the chunk id.
                let base_url = directory_url(&chunk.url()?)?;
                let tables = assignment.chunk_tables(chunk)?;
                let mut result = Vec::new();
                for table in tables {
                    let name = format!("{table}.parquet");
                    result.push(RemoteFile {
                        url: base_url
                            .join(&name)
                            .inspect_err(|e| tracing::warn!("Can't parse file url '{name}': {e}"))
                            .ok()?,
                        name,
                    });
                }
                Some(result)
            }
        }
    }

    pub fn new(
        assignment: AssignmentBlob,
        id: impl Into<String>,
        key: &Keypair,
        schema_available: impl Fn(SchemaId) -> bool,
    ) -> anyhow::Result<Self> {
        let peer_id = key.public().to_peer_id();
        let mut pool = StringPool::default();
        let mut checked = HashSet::new();

        let (status, headers, chunks) = match &assignment {
            AssignmentBlob::Legacy(assignment) => {
                let Some(worker) = assignment.get_worker(&peer_id) else {
                    anyhow::bail!("no assignment for this worker");
                };
                let chunks = worker
                    .iter_chunks_with_ref()
                    .map(|(chunk_ref, chunk)| {
                        (
                            // Legacy assignments have no versions: every chunk is the ingested
                            // copy, stored where it has always been.
                            pool.chunk_ref(chunk.dataset_id(), chunk.id(), 0),
                            chunk_ref,
                        )
                    })
                    .collect();
                (worker.status(), worker.decrypt_headers(key)?, chunks)
            }
            AssignmentBlob::Worker(assignment) => {
                let Some(worker) = assignment.get_worker(&peer_id) else {
                    anyhow::bail!("no assignment for this worker");
                };
                let mut chunks = HashMap::new();
                for (chunk_ref, chunk) in worker.iter_chunks_with_ref() {
                    // Rebuilt from the chunk's columns, so an id that won't assemble is a
                    // malformed document rather than a missing field.
                    let Some(id) = chunk.id() else {
                        anyhow::bail!(
                            "chunk {} of dataset '{}' has a hash that isn't UTF-8",
                            chunk.index(),
                            chunk.dataset().id()
                        );
                    };
                    if assignment.chunk_tables(chunk).is_none() {
                        anyhow::bail!(
                            "chunk '{id}' references write schema {} which has no roster in the assignment",
                            chunk.write_schema_id()
                        );
                    }
                    let schema_id = SchemaId::from(chunk.write_schema_id());
                    if checked.insert(schema_id) && !schema_available(schema_id) {
                        crate::metrics::SCHEMA_BUNDLE_MISMATCHES.inc();
                        anyhow::bail!(
                            "chunk '{id}' references write schema {schema_id}, which its schema bundle doesn't carry",
                        );
                    }
                    chunks.insert(
                        pool.chunk_ref(chunk.dataset().id(), &id, chunk.version()),
                        chunk_ref,
                    );
                }
                (worker.status(), worker.decrypt_headers(key)?, chunks)
            }
        };

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
            status,
            assignment,
            assignment_id: id.into(),
            http_headers,
            chunks,
        })
    }

    pub fn chunk_schema(&self, chunk: &ChunkRef) -> ChunkSchema {
        let Some(chunk_ref) = self.chunks.get(chunk) else {
            return ChunkSchema::Unassigned;
        };
        match &self.assignment {
            AssignmentBlob::Legacy(_) => ChunkSchema::Unpinned,
            AssignmentBlob::Worker(assignment) => assignment
                .get_chunk(*chunk_ref)
                .map(|c| ChunkSchema::Pinned(SchemaId::from(c.write_schema_id())))
                .unwrap_or(ChunkSchema::Unassigned),
        }
    }

    pub fn status(&self) -> sqd_assignments::WorkerStatus {
        self.status
    }

    pub fn get_headers(&self) -> &reqwest::header::HeaderMap {
        &self.http_headers
    }

    pub fn assignment_id(&self) -> &str {
        &self.assignment_id
    }

    pub fn schemas_covered_by(&self, covers: impl Fn(SchemaId) -> bool) -> bool {
        match &self.assignment {
            AssignmentBlob::Legacy(_) => true,
            AssignmentBlob::Worker(assignment) => self.chunks.values().all(|chunk_ref| {
                assignment
                    .get_chunk(*chunk_ref)
                    .is_some_and(|chunk| covers(SchemaId::from(chunk.write_schema_id())))
            }),
        }
    }

    pub fn chunks(&self) -> &HashMap<ChunkRef, ChunkAssignmentRef> {
        &self.chunks
    }
}

fn chunk_base_url(dataset_base_url: &str, chunk_prefix: &str) -> Option<Url> {
    Url::from_str(dataset_base_url)
        .inspect_err(|e| tracing::warn!("Can't parse dataset base url '{dataset_base_url}': {e}"))
        .ok()?
        .join(&format!("{chunk_prefix}/"))
        .inspect_err(|e| tracing::warn!("Can't parse chunk base url '{chunk_prefix}': {e}"))
        .ok()
}

/// Parses a url naming a directory, so joining a file name onto it extends the path instead of
/// replacing its last segment.
fn directory_url(url: &str) -> Option<Url> {
    Url::from_str(&format!("{url}/"))
        .inspect_err(|e| tracing::warn!("Can't parse chunk base url '{url}': {e}"))
        .ok()
}

#[derive(Default)]
struct StringPool {
    map: HashMap<String, Arc<String>>,
}

impl StringPool {
    fn chunk_ref(&mut self, dataset: &str, chunk: &str, version: u32) -> ChunkRef {
        ChunkRef {
            dataset: self.get(dataset),
            chunk: Arc::from(chunk),
            version,
        }
    }

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
