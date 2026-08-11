use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use reqwest::Url;
use sqd_network_transport::Keypair;
use tracing::error;

use crate::types::state::ChunkRef;
use sqd_assignments::ChunkRef as ChunkAssignmentRef;

/// A downloaded assignment in whichever format the network published it.
///
/// The two carry the same chunk assignments but describe a chunk's contents differently: the
/// legacy blob lists each file explicitly, while the worker blob names a write schema whose
/// inline roster the file list is derived from.
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
    /// Distinct write schemas this worker's chunks were written with. Empty under legacy
    /// assignments, which pin no schema. Kept so a schema bundle can be checked against what is
    /// still in use before it replaces the one in force.
    schema_ids: HashSet<u32>,
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
                // No per-chunk base_url in this format: the legacy field only ever restated `id`.
                let base_url = chunk_base_url(chunk.dataset_base_url(), chunk.id())?;
                // `new` rejects an assignment with an unresolvable chunk, so a roster is present.
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

    /// `schema_available` reports whether a write schema's content is loaded and usable. An
    /// assignment referencing a schema the worker doesn't have is refused rather than applied:
    /// its chunks would download fine and then fail — or worse, silently resolve to whichever
    /// schema shares the query's dataset type — at query time.
    pub fn new(
        assignment: AssignmentBlob,
        id: impl Into<String>,
        key: &Keypair,
        schema_available: impl Fn(u32) -> bool,
    ) -> anyhow::Result<Self> {
        let peer_id = key.public().to_peer_id();
        let mut pool = StringPool::default();
        let mut schema_ids = HashSet::new();

        let (status, headers, chunks) = match &assignment {
            AssignmentBlob::Legacy(assignment) => {
                let Some(worker) = assignment.get_worker(&peer_id) else {
                    anyhow::bail!("no assignment for this worker");
                };
                let chunks = worker
                    .iter_chunks_with_ref()
                    .map(|(chunk_ref, chunk)| {
                        (pool.chunk_ref(chunk.dataset_id(), chunk.id()), chunk_ref)
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
                    // A chunk whose write schema has no roster has no derivable file list. That
                    // can only be a malformed assignment, and applying it partially would leave
                    // the worker quietly short of the data the network believes it holds — so
                    // reject the whole thing and keep serving the previous assignment.
                    if assignment.chunk_tables(chunk).is_none() {
                        anyhow::bail!(
                            "chunk '{}' references write schema {} which has no roster in the assignment",
                            chunk.id(),
                            chunk.write_schema_id()
                        );
                    }
                    let schema_id = chunk.write_schema_id();
                    if schema_ids.insert(schema_id) && !schema_available(schema_id) {
                        anyhow::bail!(
                            "chunk '{}' references write schema {schema_id}, which is not in the loaded schema bundle",
                            chunk.id(),
                        );
                    }
                    chunks.insert(pool.chunk_ref(chunk.dataset_id(), chunk.id()), chunk_ref);
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
            schema_ids,
        })
    }

    /// The schema a chunk's data was written with, or `None` under a legacy assignment (which
    /// pins none) or for a chunk this assignment doesn't cover.
    ///
    /// Derived from the assignment rather than stored separately, so it cannot disagree with the
    /// chunk list it came from.
    pub fn write_schema_id(&self, chunk: &ChunkRef) -> Option<u32> {
        let chunk_ref = self.chunks.get(chunk)?;
        match &self.assignment {
            AssignmentBlob::Legacy(_) => None,
            AssignmentBlob::Worker(assignment) => {
                Some(assignment.get_chunk(*chunk_ref)?.write_schema_id())
            }
        }
    }

    pub fn schema_ids(&self) -> &HashSet<u32> {
        &self.schema_ids
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

/// The directory a chunk's files live under: `<dataset_base_url>/<chunk prefix>/`.
fn chunk_base_url(dataset_base_url: &str, chunk_prefix: &str) -> Option<Url> {
    Url::from_str(dataset_base_url)
        .inspect_err(|e| tracing::warn!("Can't parse dataset base url '{dataset_base_url}': {e}"))
        .ok()?
        .join(&format!("{chunk_prefix}/"))
        .inspect_err(|e| tracing::warn!("Can't parse chunk base url '{chunk_prefix}': {e}"))
        .ok()
}

#[derive(Default)]
struct StringPool {
    map: HashMap<String, Arc<String>>,
}

impl StringPool {
    fn chunk_ref(&mut self, dataset: &str, chunk: &str) -> ChunkRef {
        ChunkRef {
            dataset: self.get(dataset),
            chunk: Arc::from(chunk),
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
