use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use camino::{Utf8Component, Utf8Path};
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

/// How a query resolves the schema for a stored chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkSchema {
    Pinned(SchemaId),
    /// Resolve through the legacy type registry.
    ByType,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub url: Url,
    pub name: String,
}

/// Why an assignment cannot locate a chunk's files.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum UnresolvedChunk {
    #[error("chunk is not in this assignment")]
    NotAssigned,
    /// The assigned chunk has no usable address. Dataset-level causes — a base url that won't
    /// parse, a version with no generation — are refused at admission, so this is a per-chunk
    /// remainder: a file address that will not build.
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
                let base_url = chunk_base_url(
                    chunk.dataset_id(),
                    chunk.dataset_base_url(),
                    chunk.base_url(),
                )?;
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
                // Admission refuses a document whose own roster table doesn't define a schema its
                // chunks name (FM-12), so no chunk in this index reaches here — the arm exists
                // because the reader's signature allows it, not because it is a live verdict.
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
                let mut chunks = HashMap::new();
                let mut addressed = HashSet::new();
                for (chunk_ref, chunk) in worker.iter_chunks_with_ref() {
                    // Once per dataset: the base url is shared by every chunk of it, so one
                    // that won't parse leaves the whole dataset without an address — the
                    // document contradicting itself, which makes it inapplicable (FM-12).
                    if addressed.insert(chunk.dataset_id()) {
                        dataset_base(chunk.dataset_id(), chunk.dataset_base_url())?;
                    }
                    chunks.insert(
                        // Legacy assignments have no versions: every chunk is the ingested
                        // copy, stored where it has always been.
                        pool.chunk_ref(chunk.dataset_id(), chunk.id(), 0),
                        chunk_ref,
                    );
                }
                (worker.status(), worker.decrypt_headers(key)?, chunks)
            }
            AssignmentBlob::Worker(assignment) => {
                let Some(worker) = assignment.get_worker(&peer_id) else {
                    anyhow::bail!("no assignment for this worker");
                };
                let mut chunks = HashMap::new();
                let mut addressed = HashSet::new();
                let mut generations = HashSet::new();
                for (chunk_ref, chunk) in worker.iter_chunks_with_ref() {
                    let dataset = chunk.dataset();
                    let Some(id) = chunk.id() else {
                        anyhow::bail!(
                            "chunk {} of dataset '{}' has a hash that isn't UTF-8",
                            chunk.index(),
                            dataset.id()
                        );
                    };
                    // A document that contradicts itself is inapplicable whole (FM-12), and
                    // each of these is checked once per dataset, not per chunk: a base url that
                    // won't parse, or a version the dataset registers no generation for, leaves
                    // every chunk sharing it without an address.
                    if addressed.insert(dataset.id()) {
                        dataset_base(dataset.id(), dataset.base_url())?;
                    }
                    if chunk.version() != 0
                        && generations.insert((dataset.id(), chunk.version()))
                        && dataset.get_generation(chunk.version()).is_none()
                    {
                        anyhow::bail!(
                            "chunk '{id}' of dataset '{}' is at version {}, which the dataset registers no generation for",
                            dataset.id(),
                            chunk.version()
                        );
                    }
                    // A chunk naming a schema the document's own roster table doesn't define
                    // is the document disagreeing with itself in the same way.
                    if assignment.chunk_tables(chunk).is_none() {
                        anyhow::bail!(
                            "chunk '{id}' references write schema {} which has no roster in the assignment",
                            chunk.write_schema_id()
                        );
                    }
                    let schema_id = SchemaId::from(chunk.write_schema_id());
                    // Once per schema, not per chunk: both of these are properties of the roster.
                    if checked.insert(schema_id) {
                        if !schema_available(schema_id) {
                            crate::metrics::SCHEMA_BUNDLE_MISMATCHES.inc();
                            anyhow::bail!(
                                "chunk '{id}' references write schema {schema_id}, which its schema bundle doesn't carry",
                            );
                        }
                        if let Some(table) = assignment
                            .get_write_schema(chunk.write_schema_id())
                            .and_then(|roster| {
                                roster.tables().iter().find(|table| !is_file_name(table))
                            })
                        {
                            anyhow::bail!(
                                "write schema {schema_id} names a table '{table}' that is not a file name",
                            );
                        }
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

/// A table name becomes `<name>.parquet` inside the chunk's directory, so it has to be a file
/// name and not a path. `..` or a separator would write the file somewhere else while the chunk
/// still commits, leaving one the worker holds and reports while it is quietly missing a table —
/// which queries answer as empty rather than as an error.
fn is_file_name(name: &str) -> bool {
    let mut components = Utf8Path::new(name).components();
    matches!(components.next(), Some(Utf8Component::Normal(first)) if first == name)
        && components.next().is_none()
}

/// A dataset's base url, as admission and [`DatasetsIndex::list_files`] both read it, so the
/// two cannot disagree about what is addressable.
fn dataset_base(dataset: &str, base_url: &str) -> anyhow::Result<Url> {
    Url::from_str(base_url).map_err(|e| {
        anyhow::anyhow!("dataset '{dataset}' base url '{base_url}' doesn't parse: {e}")
    })
}

fn chunk_base_url(
    dataset: &str,
    dataset_base_url: &str,
    chunk_prefix: &str,
) -> Result<Url, UnresolvedChunk> {
    let base = dataset_base(dataset, dataset_base_url)
        .map_err(|e| UnresolvedChunk::NoAddress(e.to_string()))?;
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
