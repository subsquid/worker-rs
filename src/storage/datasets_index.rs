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

/// How a query resolves the schema for a chunk the worker holds.
///
/// The store decides what can be answered — the layout scan recovers every chunk and its version,
/// so a locked chunk has bytes on disk whatever the assignment says. This only decides how to read
/// them, which is why there is no "not ours to serve" state: an assignment describes what the
/// worker should hold, not what it may answer for (INV-2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkSchema {
    /// The assignment names the schema the chunk was written with.
    Pinned(SchemaId),
    /// Nothing names one, so the query's dataset type does. Sound wherever the type registry is
    /// loaded at all: only the legacy CDN manifest fills it, and that carries one schema per
    /// type. A bundle installs by id alone, so this can never reach for a bundle's schema and
    /// pick the wrong version of a type.
    ByType,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub url: Url,
    pub name: String,
}

/// Why the assignment can't say where a chunk's files live.
///
/// The two are told apart because they are answers to different questions: one says the caller
/// asked about a chunk this assignment never mentioned, the other that the document itself is
/// unusable for a chunk it does mention (FM-11).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum UnresolvedChunk {
    /// The ref didn't come from this assignment — the caller holds state this index never
    /// produced.
    #[error("chunk is not in this assignment")]
    NotAssigned,
    /// The document mentions the chunk but carries no usable address for it: a base url that
    /// won't parse, a version whose dataset registers no generation, a hash that isn't UTF-8.
    #[error("{0}")]
    NoAddress(String),
}

impl DatasetsIndex {
    /// The remote files (URL + filename) that make up `chunk`.
    ///
    /// # Errors
    ///
    /// If the chunk is not in this assignment, or the assignment carries no address the worker
    /// can fetch it from.
    pub fn list_files(&self, chunk: &ChunkRef) -> Result<Vec<RemoteFile>, UnresolvedChunk> {
        let chunk_ref = self.chunks.get(chunk).ok_or(UnresolvedChunk::NotAssigned)?;
        match &self.assignment {
            AssignmentBlob::Legacy(assignment) => {
                let chunk = assignment
                    .get_chunk(*chunk_ref)
                    .ok_or(UnresolvedChunk::NotAssigned)?;
                let base_url = chunk_base_url(chunk.dataset_base_url(), chunk.base_url())?;
                let mut result = Vec::with_capacity(chunk.files().len());
                for file in chunk.files() {
                    result.push(RemoteFile {
                        name: file.filename().to_owned(),
                        url: join_file(&base_url, file.url())?,
                    });
                }
                Ok(result)
            }
            AssignmentBlob::Worker(assignment) => {
                let chunk = assignment
                    .get_chunk(*chunk_ref)
                    .ok_or(UnresolvedChunk::NotAssigned)?;
                // The chunk composes its own url: the dataset's base, the prefix of the
                // generation its `version` names, then the chunk id.
                let chunk_url = chunk.url().ok_or_else(|| {
                    UnresolvedChunk::NoAddress(format!(
                        "chunk {} of dataset '{}' is at version {}, which the assignment gives no \
                         address for",
                        chunk.index(),
                        chunk.dataset().id(),
                        chunk.version()
                    ))
                })?;
                let base_url = directory_url(&chunk_url)?;
                let tables = assignment.chunk_tables(chunk).ok_or_else(|| {
                    UnresolvedChunk::NoAddress(format!(
                        "write schema {} has no roster in the assignment",
                        chunk.write_schema_id()
                    ))
                })?;
                let mut result = Vec::new();
                for table in tables {
                    let name = format!("{table}.parquet");
                    result.push(RemoteFile {
                        url: join_file(&base_url, &name)?,
                        name,
                    });
                }
                Ok(result)
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
            return ChunkSchema::ByType;
        };
        match &self.assignment {
            AssignmentBlob::Legacy(_) => ChunkSchema::ByType,
            AssignmentBlob::Worker(assignment) => assignment
                .get_chunk(*chunk_ref)
                .map(|c| ChunkSchema::Pinned(SchemaId::from(c.write_schema_id())))
                .unwrap_or(ChunkSchema::ByType),
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

    pub fn chunks(&self) -> &HashMap<ChunkRef, ChunkAssignmentRef> {
        &self.chunks
    }
}

fn chunk_base_url(dataset_base_url: &str, chunk_prefix: &str) -> Result<Url, UnresolvedChunk> {
    let base = Url::from_str(dataset_base_url).map_err(|e| {
        UnresolvedChunk::NoAddress(format!(
            "dataset base url '{dataset_base_url}' doesn't parse: {e}"
        ))
    })?;
    base.join(&format!("{chunk_prefix}/")).map_err(|e| {
        UnresolvedChunk::NoAddress(format!(
            "chunk base url '{chunk_prefix}' doesn't parse: {e}"
        ))
    })
}

/// Parses a url naming a directory, so joining a file name onto it extends the path instead of
/// replacing its last segment.
fn directory_url(url: &str) -> Result<Url, UnresolvedChunk> {
    Url::from_str(&format!("{url}/"))
        .map_err(|e| UnresolvedChunk::NoAddress(format!("chunk url '{url}' doesn't parse: {e}")))
}

fn join_file(base_url: &Url, file: &str) -> Result<Url, UnresolvedChunk> {
    base_url
        .join(file)
        .map_err(|e| UnresolvedChunk::NoAddress(format!("file url '{file}' doesn't parse: {e}")))
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
