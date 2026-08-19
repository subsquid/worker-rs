//! Persistent query schemas supplied with worker assignments.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context};
use arc_swap::ArcSwap;
use camino::{Utf8Path, Utf8PathBuf};
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use sqd_query_engine::metadata::DatasetDescription;
use tempfile::TempDir;

use crate::metrics;
use crate::query::result::QueryError;
use crate::types::schema::SchemaId;

/// Maximum compressed and decompressed bundle size.
const MAX_BUNDLE_SIZE: usize = 64 * 1024 * 1024;

const TEMP_PREFIX: &str = "temp-";
const SCHEMA_SUFFIX: &str = ".yaml";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaBundle {
    pub hash: BundleHash,
    pub url: String,
}

/// Whether another attempt at the same pair could end differently.
///
/// The hash names the content, so the hash check is the dividing line. Up to it, what was
/// fetched is a property of the location the network announced, and a corrected url still
/// rescues the pair (IB-40b) — as does simply retrying a transport failure. Past it, the bytes
/// are the ones the hash vouched for, so a fault in them is a verdict on the pair itself
/// (FM-12) and no re-fetch from anywhere can change it. Local disk faults are the network's
/// fault least of all, so they retry too.
#[derive(Debug)]
pub enum BundleFault {
    /// Transport or local I/O: retry, and let a corrected location rescue the pair.
    Transient(anyhow::Error),
    /// A verdict on content the hash already vouched for: refuse and wait for a different pair.
    Permanent(anyhow::Error),
}

impl BundleFault {
    fn transient(error: impl Into<anyhow::Error>) -> Self {
        Self::Transient(error.into())
    }

    fn permanent(error: impl Into<anyhow::Error>) -> Self {
        Self::Permanent(error.into())
    }

    pub fn is_permanent(&self) -> bool {
        matches!(self, Self::Permanent(_))
    }

    pub fn into_error(self) -> anyhow::Error {
        match self {
            Self::Transient(error) | Self::Permanent(error) => error,
        }
    }

    fn error(&self) -> &anyhow::Error {
        match self {
            Self::Transient(error) | Self::Permanent(error) => error,
        }
    }

    /// Adds context without disturbing the verdict.
    fn context(self, context: impl std::fmt::Display + Send + Sync + 'static) -> Self {
        match self {
            Self::Transient(error) => Self::Transient(error.context(context)),
            Self::Permanent(error) => Self::Permanent(error.context(context)),
        }
    }
}

impl std::fmt::Display for BundleFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.error(), f)
    }
}

/// A blocking task's outcome. A panic in one of these comes from bytes the hash already vouched
/// for, so it is a verdict on the pair like any other parse failure — the applier draws the same
/// line for a document whose reader panics. A task that was cancelled instead is the runtime
/// going down, which is nothing to refuse a pair over.
fn joined<T>(
    outcome: Result<Result<T, BundleFault>, tokio::task::JoinError>,
    what: &str,
) -> Result<T, BundleFault> {
    match outcome {
        Ok(result) => result,
        Err(e) if e.is_panic() => Err(BundleFault::permanent(
            anyhow::Error::new(e).context(format!("{what} panicked")),
        )),
        Err(e) => Err(BundleFault::transient(
            anyhow::Error::new(e).context(format!("{what} didn't finish")),
        )),
    }
}

#[derive(Debug)]
pub struct PreparedBundle {
    hash: BundleHash,
    ids: Arc<HashSet<SchemaId>>,
    schemas: HashMap<SchemaId, Arc<DatasetDescription>>,
    files: PreparedFiles,
}

pub struct PreparedSchemaUpdate {
    registry: Arc<SchemaRegistry>,
    bundle: PreparedBundle,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl PreparedSchemaUpdate {
    pub fn contains(&self, id: SchemaId) -> bool {
        self.bundle.contains(id)
    }

    pub fn ids(&self) -> Arc<HashSet<SchemaId>> {
        self.bundle.ids()
    }

    /// Moves the staged schemas into the store and publishes them. The file moves run off the
    /// runtime like the rest of this store's I/O; the mutation lock is held until they land.
    pub async fn install(self) -> anyhow::Result<()> {
        let Self {
            registry,
            bundle,
            _guard,
        } = self;
        tokio::task::spawn_blocking(move || registry.activate_bundle(bundle))
            .await
            .context("schema bundle activation task panicked")?
    }
}

#[derive(Debug)]
enum PreparedFiles {
    Cached,
    Staged {
        dir: TempDir,
        missing: HashSet<SchemaId>,
    },
}

impl PreparedBundle {
    pub fn hash(&self) -> BundleHash {
        self.hash
    }

    pub fn contains(&self, id: SchemaId) -> bool {
        self.ids.contains(&id)
    }

    pub fn ids(&self) -> Arc<HashSet<SchemaId>> {
        Arc::clone(&self.ids)
    }
}

impl TryFrom<sqd_assignments::SchemaBundle> for SchemaBundle {
    type Error = anyhow::Error;

    fn try_from(bundle: sqd_assignments::SchemaBundle) -> anyhow::Result<Self> {
        Ok(Self {
            hash: bundle.hash.parse()?,
            url: bundle.url,
        })
    }
}

/// A validated SHA-256 hash. `Display` includes the `sha256:` prefix.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BundleHash([u8; 32]);

impl BundleHash {
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

impl std::str::FromStr for BundleHash {
    type Err = anyhow::Error;

    fn from_str(hash: &str) -> anyhow::Result<Self> {
        let (algorithm, hex) = hash.split_once(':').with_context(|| {
            format!("schema bundle hash '{hash}' is not in 'algorithm:hex' form")
        })?;
        if algorithm != "sha256" {
            bail!("unsupported schema bundle hash algorithm '{algorithm}', expected sha256");
        }
        if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            bail!("schema bundle hash '{hex}' is not 64 hex digits");
        }
        let mut bytes = [0u8; 32];
        for (byte, pair) in bytes.iter_mut().zip(hex.as_bytes().chunks_exact(2)) {
            *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
        }
        Ok(Self(bytes))
    }
}

impl std::fmt::Display for BundleHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sha256:{self:x}")
    }
}

impl std::fmt::Debug for BundleHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::LowerHex for BundleHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Additive persistent schema store.
#[derive(Default)]
pub(crate) struct SchemaSnapshot {
    pub(crate) legacy_loaded: bool,
    pub(crate) by_type: HashMap<String, Arc<DatasetDescription>>,
    pub(crate) by_id: HashMap<SchemaId, Arc<DatasetDescription>>,
    pub(crate) bundle_hash: Option<BundleHash>,
    pub(crate) bundle_ids: HashSet<SchemaId>,
}

pub struct SchemaRegistry {
    dir: Utf8PathBuf,
    snapshot: ArcSwap<SchemaSnapshot>,
    merge_lock: parking_lot::Mutex<()>,
    #[cfg(test)]
    activation_failures: std::sync::atomic::AtomicUsize,
}

pub struct SchemaManager {
    registry: Arc<SchemaRegistry>,
    mutation_lock: Arc<tokio::sync::Mutex<()>>,
}

impl SchemaManager {
    pub fn open(dir: impl Into<Utf8PathBuf>) -> Self {
        Self {
            registry: Arc::new(SchemaRegistry::open(dir)),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn registry(&self) -> Arc<SchemaRegistry> {
        Arc::clone(&self.registry)
    }

    pub async fn prepare(
        &self,
        bundle: &SchemaBundle,
        client: &reqwest::Client,
    ) -> Result<PreparedSchemaUpdate, BundleFault> {
        let guard = Arc::clone(&self.mutation_lock).lock_owned().await;
        let bundle = self.registry.prepare_bundle(bundle, client).await?;
        Ok(PreparedSchemaUpdate {
            registry: Arc::clone(&self.registry),
            bundle,
            _guard: guard,
        })
    }

    pub fn installed_hash(&self) -> Option<BundleHash> {
        self.registry.installed_hash()
    }

    #[cfg(test)]
    pub(crate) fn fail_next_install(&self) {
        self.registry
            .activation_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl SchemaRegistry {
    #[cfg(test)]
    pub fn memory() -> Self {
        Self {
            dir: Utf8PathBuf::new(),
            snapshot: ArcSwap::from_pointee(SchemaSnapshot::default()),
            merge_lock: parking_lot::Mutex::new(()),
            activation_failures: Default::default(),
        }
    }

    pub fn open(dir: impl Into<Utf8PathBuf>) -> Self {
        let dir = dir.into();
        let swept = sweep_staging(&dir);
        if swept > 0 {
            tracing::debug!(swept, "Removed staging files left by an earlier run");
        }
        let snapshot = match read_store(&dir) {
            Ok(by_id) if !by_id.is_empty() => {
                tracing::info!(schemas = by_id.len(), "Adopted stored query schemas");
                SchemaSnapshot {
                    by_id,
                    ..Default::default()
                }
            }
            Ok(_) => SchemaSnapshot::default(),
            Err(e) => {
                tracing::warn!(%dir, error = ?e, "Couldn't read the schema store");
                SchemaSnapshot::default()
            }
        };
        Self {
            dir,
            snapshot: ArcSwap::from_pointee(snapshot),
            merge_lock: parking_lot::Mutex::new(()),
            #[cfg(test)]
            activation_failures: Default::default(),
        }
    }

    pub(crate) fn snapshot(&self) -> Arc<SchemaSnapshot> {
        self.snapshot.load_full()
    }

    /// Missing type schemas are server errors because worker-mode bundles populate only IDs.
    pub fn get_by_type(&self, dataset_type: &str) -> Result<Arc<DatasetDescription>, QueryError> {
        let snapshot = self.snapshot();
        if !snapshot.legacy_loaded {
            return Err(QueryError::Other(
                "query schemas for the experimental engine have not been loaded yet".to_owned(),
            ));
        }
        snapshot.by_type.get(dataset_type).cloned().ok_or_else(|| {
            QueryError::BadRequest(format!(
                "dataset type '{dataset_type}' is not supported by the experimental engine"
            ))
        })
    }

    pub fn get_by_id(&self, schema_id: SchemaId) -> Result<Arc<DatasetDescription>, QueryError> {
        self.snapshot()
            .by_id
            .get(&schema_id)
            .cloned()
            .ok_or_else(|| {
                QueryError::Other(format!(
                    "schema {schema_id} is not in the loaded schema bundle"
                ))
            })
    }

    pub(crate) fn replace_legacy(&self, by_type: HashMap<String, Arc<DatasetDescription>>) {
        let _updating = self.merge_lock.lock();
        let current = self.snapshot.load_full();
        self.snapshot.store(Arc::new(SchemaSnapshot {
            legacy_loaded: true,
            by_type,
            by_id: current.by_id.clone(),
            bundle_hash: current.bundle_hash,
            bundle_ids: current.bundle_ids.clone(),
        }));
    }

    pub fn installed_hash(&self) -> Option<BundleHash> {
        self.snapshot.load().bundle_hash
    }

    pub fn bundle_ids(&self) -> HashSet<SchemaId> {
        self.snapshot.load().bundle_ids.clone()
    }

    #[cfg(test)]
    pub(crate) fn merge_bundle(
        &self,
        schemas: HashMap<SchemaId, Arc<DatasetDescription>>,
        hash: BundleHash,
    ) {
        let ids = Arc::new(schemas.keys().copied().collect());
        self.activate_bundle(PreparedBundle {
            hash,
            ids,
            schemas,
            files: PreparedFiles::Cached,
        })
        .unwrap();
    }

    async fn prepare_bundle(
        &self,
        bundle: &SchemaBundle,
        client: &reqwest::Client,
    ) -> Result<PreparedBundle, BundleFault> {
        match self.merge(bundle, client).await {
            Ok(bundle) => Ok(bundle),
            Err(e) => {
                metrics::SCHEMA_BUNDLE_FAILURES.inc();
                Err(e)
            }
        }
    }

    #[cfg(test)]
    async fn ensure(&self, bundle: &SchemaBundle, client: &reqwest::Client) -> anyhow::Result<()> {
        let prepared = self
            .prepare_bundle(bundle, client)
            .await
            .map_err(BundleFault::into_error)?;
        self.activate_bundle(prepared)
    }

    async fn merge(
        &self,
        bundle: &SchemaBundle,
        client: &reqwest::Client,
    ) -> Result<PreparedBundle, BundleFault> {
        if self.installed_hash() == Some(bundle.hash) {
            tracing::debug!(hash = %bundle.hash, "Schema bundle unchanged");
            return Ok(PreparedBundle {
                hash: bundle.hash,
                ids: Arc::new(self.bundle_ids()),
                schemas: HashMap::new(),
                files: PreparedFiles::Cached,
            });
        }

        let bytes = download(&bundle.url, client)
            .await
            .with_context(|| format!("couldn't download schema bundle from {}", bundle.url))
            .map_err(BundleFault::transient)?;
        let actual = BundleHash::of(&bytes);
        if actual != bundle.hash {
            // Transient on purpose: the hash names the content, so a mismatch says this
            // location served the wrong bytes — which is exactly what a corrected url fixes.
            // Refusing here would key the refusal on the pair and drop that correction.
            return Err(BundleFault::transient(anyhow::anyhow!(
                "schema bundle hash mismatch: expected {}, got {actual}",
                bundle.hash
            )));
        }
        let staged = unpack(bytes, self.dir.clone())
            .await
            .map_err(|e| e.context("couldn't unpack schema bundle"))?;
        let staged_path = Utf8Path::from_path(staged.path())
            .ok_or_else(|| {
                BundleFault::transient(anyhow::anyhow!("staging directory path is not UTF-8"))
            })?
            .to_owned();
        let schemas = load_dir(staged_path.clone())
            .await
            .map_err(|e| e.context("couldn't load the staged schema bundle"))?;

        let ids = Arc::new(schemas.keys().copied().collect());
        let schemas_to_install = classify_cached(self.dir.clone(), staged_path, &ids).await?;

        tracing::info!(hash = %bundle.hash, new_schemas = schemas_to_install.len(), "Merging schema bundle");
        Ok(PreparedBundle {
            hash: bundle.hash,
            ids,
            schemas,
            files: PreparedFiles::Staged {
                dir: staged,
                missing: schemas_to_install,
            },
        })
    }

    fn activate_bundle(&self, bundle: PreparedBundle) -> anyhow::Result<()> {
        self.activate_bundle_with(bundle, &metrics::SCHEMA_BUNDLE_LOADED)
    }

    fn activate_bundle_with(
        &self,
        bundle: PreparedBundle,
        loaded: &prometheus_client::metrics::gauge::Gauge,
    ) -> anyhow::Result<()> {
        #[cfg(test)]
        if self
            .activation_failures
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
        {
            anyhow::bail!("injected schema bundle activation failure");
        }
        let _updating = self.merge_lock.lock();
        match bundle.files {
            PreparedFiles::Cached => {}
            PreparedFiles::Staged { dir, missing } => {
                std::fs::create_dir_all(&self.dir)?;
                let staged = Utf8Path::from_path(dir.path())
                    .context("staging directory path is not UTF-8")?;
                for id in missing {
                    let name = format!("{id}{SCHEMA_SUFFIX}");
                    std::fs::rename(staged.join(&name), self.dir.join(&name))
                        .with_context(|| format!("couldn't move schema {id} into the store"))?;
                }
            }
        }
        let current = self.snapshot.load_full();
        let mut by_id = current.by_id.clone();
        for (id, description) in bundle.schemas {
            by_id.insert(id, description);
        }
        self.snapshot.store(Arc::new(SchemaSnapshot {
            legacy_loaded: current.legacy_loaded,
            by_type: current.by_type.clone(),
            by_id,
            bundle_hash: Some(bundle.hash),
            bundle_ids: (*bundle.ids).clone(),
        }));
        loaded.set(1);
        Ok(())
    }
}

fn sweep_staging(dir: &Utf8Path) -> usize {
    let mut removed = 0usize;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return removed;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.starts_with(TEMP_PREFIX) {
            continue;
        }
        let outcome = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        match outcome {
            Ok(()) => removed += 1,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "Couldn't remove a staging leftover")
            }
        }
    }
    removed
}

fn read_store(dir: &Utf8Path) -> anyhow::Result<HashMap<SchemaId, Arc<DatasetDescription>>> {
    if !dir.exists() {
        return Ok(HashMap::new());
    }
    let mut schemas = HashMap::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(id) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(schema_id)
        else {
            continue;
        };
        match std::fs::read_to_string(&path)
            .map_err(anyhow::Error::from)
            .and_then(|yaml| {
                sqd_query_engine::metadata::parse_dataset_description(&yaml)
                    .map_err(|e| anyhow::anyhow!("{e:?}"))
            }) {
            Ok(description) => {
                schemas.insert(id, Arc::new(description));
            }
            // Remove unreadable files so a later bundle can reinstall the ID.
            Err(e) => {
                tracing::warn!(path = %path.display(), error = ?e, "Removing an unreadable stored schema");
                if let Err(e) = std::fs::remove_file(&path) {
                    tracing::warn!(path = %path.display(), error = %e, "Couldn't remove the unreadable stored schema");
                }
            }
        }
    }
    Ok(schemas)
}

async fn download(url: &str, client: &reqwest::Client) -> anyhow::Result<Vec<u8>> {
    let response = client.get(url).send().await?.error_for_status()?;
    let mut buf = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or(0)
            .min(MAX_BUNDLE_SIZE as u64) as usize,
    );
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.try_next().await? {
        if buf.len() + chunk.len() > MAX_BUNDLE_SIZE {
            bail!("schema bundle exceeds {MAX_BUNDLE_SIZE} bytes");
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

async fn unpack(bytes: Vec<u8>, parent: Utf8PathBuf) -> Result<TempDir, BundleFault> {
    joined(
        tokio::task::spawn_blocking(move || {
            // Making somewhere to stage is this worker's business, not the bundle's.
            std::fs::create_dir_all(&parent).map_err(BundleFault::transient)?;
            let temp = tempfile::Builder::new()
                .prefix(TEMP_PREFIX)
                .tempdir_in(&parent)
                .map_err(BundleFault::transient)?;
            let path = Utf8Path::from_path(temp.path()).ok_or_else(|| {
                BundleFault::transient(anyhow::anyhow!("staging directory path is not UTF-8"))
            })?;

            extract_schemas(&bytes, path, MAX_BUNDLE_SIZE)?;
            Ok(temp)
        })
        .await,
        "unpacking the schema bundle",
    )
}

/// Limits all decompressed bytes, including ignored tar entries.
struct Bounded<R> {
    inner: R,
    limit: usize,
    read: usize,
}

impl<R: std::io::Read> std::io::Read for Bounded<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.read = self.read.saturating_add(read);
        if self.read > self.limit {
            return Err(std::io::Error::other(format!(
                "unpacked schema bundle exceeds {} bytes",
                self.limit
            )));
        }
        Ok(read)
    }
}

/// Extracts root-level `<id>.yaml` files and ignores other entries.
fn extract_schemas(bytes: &[u8], dest: &Utf8Path, limit: usize) -> Result<(), BundleFault> {
    use std::io::Read;

    let mut archive = tar::Archive::new(Bounded {
        inner: flate2::read::GzDecoder::new(bytes),
        limit,
        read: 0,
    });
    let mut written = 0usize;

    // Walking the archive only ever reads the bytes in hand — the gzip framing, the tar
    // structure, and the decompressed-size cap are all properties of what the hash vouched for.
    for entry in archive.entries().map_err(BundleFault::permanent)? {
        let mut entry = entry.map_err(BundleFault::permanent)?;
        let path = entry.path().map_err(BundleFault::permanent)?;
        let at_root = path
            .parent()
            .is_none_or(|p| p.as_os_str().is_empty() || p == std::path::Path::new("."));
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let Some(id) = schema_id(name).filter(|_| at_root && entry.header().entry_type().is_file())
        else {
            tracing::debug!(entry = %path.display(), "Ignoring unrecognized schema bundle entry");
            continue;
        };
        let name = format!("{id}.yaml");

        // Read and write as separate steps rather than one `io::copy`: reading is the archive
        // (and the cap), writing is this worker's disk, and a single call could not say which
        // of the two failed.
        let mut yaml = Vec::new();
        entry.read_to_end(&mut yaml).map_err(|e| {
            BundleFault::permanent(
                anyhow::Error::new(e)
                    .context(format!("couldn't read schema '{name}' out of the bundle")),
            )
        })?;
        std::fs::write(dest.join(&name), &yaml).map_err(|e| {
            BundleFault::transient(
                anyhow::Error::new(e).context(format!("couldn't write schema '{name}'")),
            )
        })?;
        written += 1;
    }

    if written == 0 {
        return Err(BundleFault::permanent(anyhow::anyhow!(
            "schema bundle contains no <id>.yaml entries"
        )));
    }
    Ok(())
}

/// Rejects alternate spellings that could define one id twice.
fn schema_id(file_name: &str) -> Option<SchemaId> {
    let stem = file_name.strip_suffix(".yaml")?;
    if stem.is_empty() || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if stem.starts_with('0') && stem.len() > 1 {
        return None;
    }
    stem.parse().ok().map(SchemaId::new)
}

async fn load_dir(
    dir: Utf8PathBuf,
) -> Result<HashMap<SchemaId, Arc<DatasetDescription>>, BundleFault> {
    joined(
        tokio::task::spawn_blocking(move || {
            let mut schemas = HashMap::new();
            // Reading back what was just staged is disk; what the yaml says is the bundle.
            for entry in std::fs::read_dir(&dir).map_err(BundleFault::transient)? {
                let path = entry.map_err(BundleFault::transient)?.path();
                let Some(id) = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(schema_id)
                else {
                    continue;
                };
                let yaml = std::fs::read_to_string(&path)
                    .with_context(|| format!("couldn't read schema {}", path.display()))
                    .map_err(BundleFault::transient)?;
                let description = sqd_query_engine::metadata::parse_dataset_description(&yaml)
                    .map_err(|e| {
                        BundleFault::permanent(anyhow::anyhow!(
                            "couldn't parse schema {}: {e:?}",
                            path.display()
                        ))
                    })?;
                schemas.insert(id, Arc::new(description));
            }
            if schemas.is_empty() {
                return Err(BundleFault::permanent(anyhow::anyhow!(
                    "no schemas found in {dir}"
                )));
            }
            Ok(schemas)
        })
        .await,
        "loading the staged schema bundle",
    )
}

async fn classify_cached(
    store: Utf8PathBuf,
    staged: Utf8PathBuf,
    ids: &HashSet<SchemaId>,
) -> Result<HashSet<SchemaId>, BundleFault> {
    let ids = ids.clone();
    joined(
        tokio::task::spawn_blocking(move || {
            let mut missing = HashSet::new();
            for id in ids {
                let name = format!("{id}{SCHEMA_SUFFIX}");
                let stored = store.join(&name);
                if !stored.exists() {
                    missing.insert(id);
                    continue;
                }
                let cached = std::fs::read(&stored).map_err(BundleFault::transient)?;
                let fresh = std::fs::read(staged.join(&name)).map_err(BundleFault::transient)?;
                if cached != fresh {
                    // An id names its contents for all time, so no re-fetch of this bundle can
                    // produce anything but the bytes that already disagree with the store.
                    return Err(BundleFault::permanent(anyhow::anyhow!(
                        "schema {id} was republished with different contents"
                    )));
                }
            }
            Ok(missing)
        })
        .await,
        "comparing the staged schemas with the store",
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    pub(crate) const SCHEMA: &str = r#"
name: evm
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number:
        type: uint64
"#;

    pub(crate) fn targz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::default(),
        ));
        for (name, body) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(body.len() as u64);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            let name_field = &mut header.as_gnu_mut().unwrap().name;
            name_field[..name.len()].copy_from_slice(name.as_bytes());
            header.set_cksum();
            builder.append(&header, *body).unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{targz, SCHEMA};
    use super::*;
    use crate::controller::test_support::TestServer;

    fn store(dir: &tempfile::TempDir) -> Arc<SchemaRegistry> {
        Arc::new(SchemaRegistry::open(
            Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap(),
        ))
    }

    async fn served_bundle(archive: Vec<u8>) -> SchemaBundle {
        SchemaBundle {
            hash: BundleHash::of(&archive),
            url: TestServer::serve_once(archive).await,
        }
    }

    #[test]
    fn parses_only_sha256_hashes() {
        let hex = "a".repeat(64);
        let parsed: BundleHash = format!("sha256:{hex}").parse().unwrap();
        assert_eq!(parsed.to_string(), format!("sha256:{hex}"), "round-trips");
        assert_eq!(
            format!("{parsed:x}"),
            hex,
            "bare hex names the unpacked dir"
        );

        let bad = |s: String| s.parse::<BundleHash>().is_err();
        assert!(bad(format!("md5:{hex}")));
        assert!(bad(hex.clone()), "bare hex has no algorithm");
        assert!(bad("sha256:abc".to_owned()), "wrong length");
        assert!(bad(format!("sha256:{}", "z".repeat(64))));
    }

    #[test]
    fn schema_ids_come_from_digits_only() {
        assert_eq!(schema_id("7.yaml"), Some(SchemaId::new(7)));
        assert_eq!(schema_id("140000.yaml"), Some(SchemaId::new(140_000)));
        // `str::parse` would accept these as 7, letting one bundle define the same id twice.
        assert_eq!(schema_id("+7.yaml"), None);
        assert_eq!(schema_id("007.yaml"), None);
        assert_eq!(schema_id("evm.yaml"), None);
        assert_eq!(schema_id("7.yml"), None);
        assert_eq!(schema_id(".yaml"), None);
    }

    #[test]
    fn an_ignored_entry_counts_against_the_unpacked_cap() {
        let dir = tempfile::tempdir().unwrap();
        let dest = Utf8Path::from_path(dir.path()).unwrap();
        let archive = targz(&[
            ("7.yaml", SCHEMA.as_bytes()),
            ("junk.bin", &vec![0u8; 64 * 1024]),
        ]);

        // Room for the schema and its tar framing, not for the member that is thrown away.
        let error = extract_schemas(&archive, dest, 8 * 1024)
            .expect_err("an ignored entry is decompressed like any other");
        assert!(format!("{error:#}").contains("exceeds"), "{error:#}");

        // The same archive is fine under the real cap, and still yields only the schema.
        extract_schemas(&archive, dest, MAX_BUNDLE_SIZE).unwrap();
        assert_eq!(stored(&dir), vec!["7.yaml"]);
    }

    #[tokio::test]
    async fn preparing_a_bundle_does_not_make_it_active() {
        let dir = tempfile::tempdir().unwrap();
        let registry = store(&dir);
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let bundle = served_bundle(archive).await;

        let prepared = registry
            .prepare_bundle(&bundle, &reqwest::Client::new())
            .await
            .unwrap();

        assert!(registry.get_by_id(SchemaId::new(7)).is_err());
        assert_eq!(registry.installed_hash(), None);
        assert!(registry.bundle_ids().is_empty());
        assert!(stored(&dir).is_empty());

        let loaded = prometheus_client::metrics::gauge::Gauge::default();
        assert_eq!(loaded.get(), 0);
        registry.activate_bundle_with(prepared, &loaded).unwrap();
        assert_eq!(loaded.get(), 1);
        assert_eq!(registry.installed_hash(), Some(bundle.hash));
        assert_eq!(registry.bundle_ids(), HashSet::from([SchemaId::new(7)]));
        assert_eq!(stored(&dir), vec!["7.yaml"]);
    }

    #[tokio::test]
    async fn a_later_bundle_adds_to_the_store_rather_than_replacing_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let registry = Arc::clone(&store);

        for (name, body) in [
            ("7.yaml", SCHEMA.to_owned()),
            ("12.yaml", SCHEMA.replace("name: evm", "name: solana")),
        ] {
            let archive = targz(&[(name, body.as_bytes())]);
            let bundle = served_bundle(archive).await;
            store
                .ensure(&bundle, &reqwest::Client::new())
                .await
                .unwrap();
        }

        assert_eq!(
            registry.get_by_id(SchemaId::new(7)).unwrap().name,
            "evm",
            "the first bundle's schema survives the second"
        );
        assert_eq!(
            registry.get_by_id(SchemaId::new(12)).unwrap().name,
            "solana"
        );
        assert_eq!(stored(&dir), vec!["12.yaml", "7.yaml"]);

        assert_eq!(store.bundle_ids(), HashSet::from([SchemaId::new(12)]));
    }

    #[tokio::test]
    async fn retry_publishes_an_identical_schema_cached_by_a_partial_install() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        std::fs::write(dir.path().join("7.yaml"), SCHEMA).unwrap();
        assert!(store.get_by_id(SchemaId::new(7)).is_err());

        let archive = targz(&[("7.yaml", SCHEMA.as_bytes()), ("9.yaml", SCHEMA.as_bytes())]);
        let bundle = served_bundle(archive).await;
        store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .unwrap();

        assert!(store.get_by_id(SchemaId::new(7)).is_ok());
        assert!(store.get_by_id(SchemaId::new(9)).is_ok());
        assert_eq!(stored(&dir), vec!["7.yaml", "9.yaml"]);
    }

    /// Which staging faults another attempt could end differently, and which could not. The hash
    /// names the content, so the hash check is the dividing line: up to it — a url that does not
    /// serve, bytes that do not match the hash (the signature of a stale or wrong url, the one
    /// fault a corrected location is guaranteed to fix; a refusal is keyed on the pair and would
    /// drop exactly that correction) — the fault is transient. Past it the bytes are the ones the
    /// network vouched for: no `<id>.yaml` entry, a schema that will not parse, or an id
    /// republished with different contents than the store holds — whether it was installed by a
    /// bundle or adopted from disk — are verdicts on the pair, and asking again would only burn
    /// the download every retry period. Whatever the fault, nothing is installed, the store is
    /// as it was, and no staging directory survives the attempt that made it.
    #[tokio::test]
    async fn staging_faults_split_into_transient_and_permanent() {
        #[derive(Clone, Copy)]
        enum Seed {
            Nothing,
            Installed,
            OnDisk,
        }
        let client = reqwest::Client::new();
        let changed = || {
            targz(&[(
                "7.yaml",
                SCHEMA.replace("name: evm", "name: evm2").as_bytes(),
            )])
        };
        let cases: [(&str, Seed, bool, &str); 6] = [
            (
                "url that does not serve",
                Seed::Nothing,
                false,
                "couldn't download",
            ),
            (
                "bytes that do not match the hash",
                Seed::Nothing,
                false,
                "hash mismatch",
            ),
            (
                "no <id>.yaml entries",
                Seed::Nothing,
                true,
                "no <id>.yaml entries",
            ),
            (
                "schema that will not parse",
                Seed::Nothing,
                true,
                "couldn't load",
            ),
            (
                "id republished against an installed schema",
                Seed::Installed,
                true,
                "republished with different contents",
            ),
            (
                "id republished against a schema adopted from disk",
                Seed::OnDisk,
                true,
                "republished with different contents",
            ),
        ];

        for (case, seed, permanent, fragment) in cases {
            let dir = tempfile::tempdir().unwrap();
            if let Seed::OnDisk = seed {
                std::fs::write(dir.path().join("7.yaml"), SCHEMA).unwrap();
            }
            let store = store(&dir);
            let mut installed = None;
            if let Seed::Installed = seed {
                let original = served_bundle(targz(&[("7.yaml", SCHEMA.as_bytes())])).await;
                store.ensure(&original, &client).await.unwrap();
                installed = Some(original.hash);
            }
            let bundle = match case {
                "url that does not serve" => SchemaBundle {
                    hash: BundleHash::of(b"whatever"),
                    url: TestServer::start().await.url("/never-served.tar.gz"),
                },
                "bytes that do not match the hash" => SchemaBundle {
                    hash: BundleHash::of(b"something else"),
                    url: TestServer::serve_once(targz(&[("7.yaml", SCHEMA.as_bytes())])).await,
                },
                "no <id>.yaml entries" => {
                    served_bundle(targz(&[("readme.txt", b"no schemas here")])).await
                }
                "schema that will not parse" => {
                    served_bundle(targz(&[("7.yaml", b"this is not a dataset description")])).await
                }
                _ => served_bundle(changed()).await,
            };

            let fault = store.prepare_bundle(&bundle, &client).await.unwrap_err();

            assert_eq!(fault.is_permanent(), permanent, "{case}: {fault:#}");
            assert!(format!("{fault:#}").contains(fragment), "{case}: {fault:#}");
            assert_eq!(
                store.installed_hash(),
                installed,
                "{case}: nothing was installed"
            );
            match seed {
                Seed::Nothing => {
                    assert!(store.get_by_id(SchemaId::new(7)).is_err(), "{case}");
                    assert!(stored(&dir).is_empty(), "{case}: {:?}", stored(&dir));
                }
                Seed::Installed | Seed::OnDisk => {
                    assert_eq!(
                        store.get_by_id(SchemaId::new(7)).unwrap().name,
                        "evm",
                        "{case}"
                    );
                    assert_eq!(stored(&dir), vec!["7.yaml"], "{case}");
                    assert_eq!(
                        std::fs::read_to_string(dir.path().join("7.yaml")).unwrap(),
                        SCHEMA,
                        "{case}: the store is as it was"
                    );
                }
            }
            assert!(
                stored_all(&dir)
                    .iter()
                    .all(|name| !name.starts_with(TEMP_PREFIX)),
                "{case}: a staged bundle must not survive the attempt that made it: {:?}",
                stored_all(&dir)
            );
        }
    }

    #[tokio::test]
    async fn ignores_entries_that_are_not_root_level_id_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let registry = Arc::clone(&store);
        let archive = targz(&[
            ("7.yaml", SCHEMA.as_bytes()),
            ("nested/9.yaml", SCHEMA.as_bytes()),
            ("../escape.yaml", b"nope"),
            ("manifest.json", b"{}"),
        ]);
        let bundle = served_bundle(archive).await;

        store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .unwrap();

        assert!(registry.get_by_id(SchemaId::new(7)).is_ok());
        assert!(
            registry.get_by_id(SchemaId::new(9)).is_err(),
            "nested entries are not unpacked"
        );
        assert!(
            !dir.path().parent().unwrap().join("escape.yaml").exists(),
            "traversal entries must not escape the store directory"
        );
        assert_eq!(stored(&dir), vec!["7.yaml"]);
    }

    /// Adopted schemas answer queries but belong to no bundle this process merged, so they admit
    /// no assignment on their own (ADR-21) and say nothing about the legacy type registry.
    #[tokio::test]
    async fn a_restart_adopts_stored_schemas_without_a_bundle_or_legacy_set() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = store(&dir);
            let bundle = served_bundle(targz(&[("7.yaml", SCHEMA.as_bytes())])).await;
            store
                .ensure(&bundle, &reqwest::Client::new())
                .await
                .unwrap();
        }

        let store = store(&dir);

        assert_eq!(store.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
        assert_eq!(
            store.installed_hash(),
            None,
            "adopted schemas belong to no bundle this process merged"
        );
        assert!(
            store.bundle_ids().is_empty(),
            "so they cannot admit an assignment on their own (ADR-21)"
        );
        assert!(
            matches!(store.get_by_type("evm"), Err(QueryError::Other(_))),
            "and they fill no type index"
        );
    }

    /// A stored file that will not read is removed at startup rather than skipped: in place it
    /// would read as a republished id and refuse every bundle carrying it, so its id could never
    /// be reinstalled. Its readable neighbours are adopted regardless.
    #[tokio::test]
    async fn a_bundle_reinstalls_a_schema_the_store_could_not_read() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("7.yaml"), b"").unwrap();
        std::fs::write(dir.path().join("12.yaml"), SCHEMA).unwrap();
        let store = store(&dir);

        assert!(
            store.get_by_id(SchemaId::new(12)).is_ok(),
            "the readable neighbour is adopted"
        );
        assert!(store.get_by_id(SchemaId::new(7)).is_err());
        assert_eq!(stored(&dir), vec!["12.yaml"], "the unreadable one is gone");

        let archive = targz(&[("7.yaml", SCHEMA.as_bytes()), ("9.yaml", SCHEMA.as_bytes())]);
        let bundle = served_bundle(archive).await;
        store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .unwrap();

        assert_eq!(store.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
        assert_eq!(
            store.get_by_id(SchemaId::new(9)).unwrap().name,
            "evm",
            "and the rest of the bundle no longer goes down with it"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("7.yaml")).unwrap(),
            SCHEMA
        );
    }

    #[test]
    fn construction_sweeps_staging_but_keeps_stored_schemas() {
        let dir = tempfile::tempdir().unwrap();
        let root = Utf8Path::from_path(dir.path()).unwrap();
        std::fs::create_dir_all(root.join(format!("{TEMP_PREFIX}abcd"))).unwrap();
        std::fs::write(root.join(format!("{TEMP_PREFIX}stray")), b"x").unwrap();
        std::fs::write(root.join("7.yaml"), SCHEMA).unwrap();
        std::fs::write(root.join("unrelated"), b"keep me").unwrap();

        let _store = store(&dir);

        let mut left = stored_all(&dir);
        left.sort();
        assert_eq!(left, vec!["7.yaml", "unrelated"]);
    }

    fn stored(dir: &tempfile::TempDir) -> Vec<String> {
        let mut names: Vec<String> = stored_all(dir)
            .into_iter()
            .filter(|n| schema_id(n).is_some())
            .collect();
        names.sort();
        names
    }

    fn stored_all(dir: &tempfile::TempDir) -> Vec<String> {
        std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    }
}
