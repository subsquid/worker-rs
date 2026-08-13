//! The network's schema bundle: a gzipped tar of `<schema_id>.yaml` query-engine schemas,
//! published alongside the assignments that reference those ids, and the store they are merged
//! into. In worker-assignment mode it replaces the CDN manifest as the source of query schemas.
//!
//! A worker assignment and bundle are validated together: the bundle must name every schema the
//! assignment uses. Schema ids are immutable, and installing a bundle only adds ids that are not
//! already stored.

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

/// Caps both the download and the unpacked total: the only bound on a corrupt or hostile archive
/// before the hash is verified. Published bundles are tens of kilobytes.
const MAX_BUNDLE_SIZE: usize = 64 * 1024 * 1024;

/// Staging entries are never read back and are swept at startup.
const TEMP_PREFIX: &str = "temp-";
const SCHEMA_SUFFIX: &str = ".yaml";

/// A schema bundle published by the network.
#[derive(Clone, Debug)]
pub struct SchemaBundle {
    pub hash: BundleHash,
    pub url: String,
}

#[derive(Debug)]
pub struct PreparedBundle {
    hash: BundleHash,
    ids: HashSet<SchemaId>,
    /// Every validated schema in the bundle, including ones already cached on disk.
    schemas: HashMap<SchemaId, Arc<DatasetDescription>>,
    /// Schemas whose files are not cached yet and must move out of `staged`.
    schemas_to_install: HashSet<SchemaId>,
    staged: Option<TempDir>,
}

impl PreparedBundle {
    pub fn hash(&self) -> BundleHash {
        self.hash
    }

    pub fn contains(&self, id: SchemaId) -> bool {
        self.ids.contains(&id)
    }

    pub fn ids(&self) -> HashSet<SchemaId> {
        self.ids.clone()
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

/// A validated SHA-256 schema bundle hash.
///
/// `Display` renders the wire form `sha256:<hex>`; `LowerHex` the bare hex it is built from.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BundleHash([u8; 32]);

impl BundleHash {
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

impl std::str::FromStr for BundleHash {
    type Err = anyhow::Error;

    /// Only sha256. An unknown algorithm fails rather than skipping verification.
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

/// The worker's schema store: every `<id>.yaml` it has ever merged, under `dir`, and the same
/// set loaded into `registry`.
///
/// A bundle is merged into the store rather than replacing it. The network publishes only its
/// current bundle and offers no way to fetch an older one, so a schema dropped here is gone —
/// and any chunk still on disk that was written with it becomes unreadable.
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
    /// Guards the merge: writing the files and publishing them to the registry are one step.
    /// Nothing under it awaits.
    merge_lock: parking_lot::Mutex<()>,
}

impl SchemaRegistry {
    #[cfg(test)]
    pub fn memory() -> Self {
        Self {
            dir: Utf8PathBuf::new(),
            snapshot: ArcSwap::from_pointee(SchemaSnapshot::default()),
            merge_lock: parking_lot::Mutex::new(()),
        }
    }

    /// Adopts whatever an earlier run left in `dir` and sweeps anything half-written. Adopted
    /// schemas answer queries immediately; admitting an assignment still waits for a bundle.
    pub fn open(dir: impl Into<Utf8PathBuf>) -> Self {
        let dir = dir.into();
        // Blocking, but it's startup and the files are few and small (as in `StateManager::new`).
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
        }
    }

    pub(crate) fn snapshot(&self) -> Arc<SchemaSnapshot> {
        self.snapshot.load_full()
    }

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

    /// Hash of the last bundle merged. Set only on success, so a failure leaves the previous
    /// value and the next poll re-offers the bundle rather than deduplicating it away.
    pub fn installed_hash(&self) -> Option<BundleHash> {
        self.snapshot.load().bundle_hash
    }

    #[cfg(test)]
    pub(crate) fn bundle_hash(&self) -> Option<BundleHash> {
        self.installed_hash()
    }

    /// Schema ids carried by the bundle in force.
    pub fn bundle_ids(&self) -> HashSet<SchemaId> {
        self.snapshot.load().bundle_ids.clone()
    }

    pub fn loaded_ids(&self) -> HashSet<SchemaId> {
        self.snapshot.load().by_id.keys().copied().collect()
    }

    #[cfg(test)]
    pub(crate) fn merge_bundle(
        &self,
        schemas: HashMap<SchemaId, Arc<DatasetDescription>>,
        hash: BundleHash,
    ) {
        let ids = schemas.keys().copied().collect();
        self.activate_bundle(PreparedBundle {
            hash,
            ids,
            schemas_to_install: schemas.keys().copied().collect(),
            schemas,
            staged: None,
        })
        .unwrap();
    }

    /// Downloads `bundle` unless it is already the one in force, and merges it into the store.
    /// The caller must serialize preparation and activation. Production does this in the
    /// assignment loop; this is public only for the integration harness.
    #[doc(hidden)]
    pub async fn prepare_bundle(
        &self,
        bundle: &SchemaBundle,
        client: &reqwest::Client,
    ) -> anyhow::Result<PreparedBundle> {
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
        let prepared = self.prepare_bundle(bundle, client).await?;
        self.activate_bundle(prepared)
    }

    async fn merge(
        &self,
        bundle: &SchemaBundle,
        client: &reqwest::Client,
    ) -> anyhow::Result<PreparedBundle> {
        if self.installed_hash() == Some(bundle.hash) {
            tracing::debug!(hash = %bundle.hash, "Schema bundle unchanged");
            return Ok(PreparedBundle {
                hash: bundle.hash,
                ids: self.bundle_ids(),
                schemas: HashMap::new(),
                schemas_to_install: HashSet::new(),
                staged: None,
            });
        }

        // Fetch, unpack and parse hold no lock: they write only into a directory of this
        // attempt's own, which nothing else can name.
        let bytes = download(&bundle.url, client)
            .await
            .with_context(|| format!("couldn't download schema bundle from {}", bundle.url))?;
        let actual = BundleHash::of(&bytes);
        if actual != bundle.hash {
            bail!(
                "schema bundle hash mismatch: expected {}, got {actual}",
                bundle.hash
            );
        }
        let staged = unpack(bytes, self.dir.clone())
            .await
            .context("couldn't unpack schema bundle")?;
        let staged_path = Utf8Path::from_path(staged.path())
            .context("staging directory path is not UTF-8")?
            .to_owned();
        let schemas = load_dir(staged_path.clone())
            .await
            .context("couldn't load the staged schema bundle")?;

        let ids = schemas.keys().copied().collect();
        let mut schemas_to_install = HashSet::new();
        // Schema ids are immutable. Exact files are already cached and need no write; a different
        // file under an existing id would change the meaning of chunks already on disk.
        for id in schemas.keys().copied().collect::<Vec<_>>() {
            let name = format!("{id}{SCHEMA_SUFFIX}");
            let stored = self.dir.join(&name);
            if !stored.exists() {
                schemas_to_install.insert(id);
                continue;
            }
            let staged_file = staged_path.join(&name);
            if std::fs::read(&stored)? != std::fs::read(&staged_file)? {
                bail!("schema {id} was republished with different contents");
            }
        }

        tracing::info!(hash = %bundle.hash, new_schemas = schemas_to_install.len(), "Merging schema bundle");
        Ok(PreparedBundle {
            hash: bundle.hash,
            ids,
            schemas,
            schemas_to_install,
            staged: Some(staged),
        })
    }

    #[doc(hidden)]
    pub fn activate_bundle(&self, bundle: PreparedBundle) -> anyhow::Result<()> {
        self.activate_bundle_with(bundle, &metrics::SCHEMA_BUNDLE_LOADED)
    }

    fn activate_bundle_with(
        &self,
        mut bundle: PreparedBundle,
        loaded: &prometheus_client::metrics::gauge::Gauge,
    ) -> anyhow::Result<()> {
        let _updating = self.merge_lock.lock();
        if let Some(staged) = bundle.staged.take() {
            std::fs::create_dir_all(&self.dir)?;
            let staged = Utf8Path::from_path(staged.path())
                .context("staging directory path is not UTF-8")?;
            for id in &bundle.schemas_to_install {
                let name = format!("{id}{SCHEMA_SUFFIX}");
                std::fs::rename(staged.join(&name), self.dir.join(&name))
                    .with_context(|| format!("couldn't move schema {id} into the store"))?;
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
            bundle_ids: bundle.ids,
        }));
        loaded.set(1);
        Ok(())
    }
}

/// Removes anything left in the staging namespace. Best-effort: a leftover is inert, since
/// nothing reads a `temp-` name back.
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

/// Reads the store as it stands. A schema that won't parse is skipped rather than fatal, so the
/// rest can still answer queries.
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
            Err(e) => {
                tracing::warn!(path = %path.display(), error = ?e, "Skipping an unreadable stored schema")
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

/// Extracts into a staging directory, then renames it onto `dest`, so a crash mid-unpack can't
/// leave a partial bundle under a hash that claims to be complete.
async fn unpack(bytes: Vec<u8>, parent: Utf8PathBuf) -> anyhow::Result<TempDir> {
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&parent)?;
        // Named, not adopted, so the directory is exclusively this attempt's, and dropped on
        // every failure path, so only a success or a crash leaves one behind. The prefix is what
        // the startup sweep looks for, since a crash skips the drop.
        let temp = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempdir_in(&parent)?;
        let path = Utf8Path::from_path(temp.path())
            .ok_or_else(|| anyhow::anyhow!("staging directory path is not UTF-8"))?;

        extract_schemas(&bytes, path)?;
        Ok(temp)
    })
    .await
    .context("unpack task panicked")?
}

/// Writes out every `<id>.yaml` at the archive root. Anything else — nested paths, a later
/// manifest, and by construction any `..` or absolute path — is skipped rather than rejected, so
/// a bundle that grows new entries still loads on an older worker.
fn extract_schemas(bytes: &[u8], dest: &Utf8Path) -> anyhow::Result<()> {
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes));
    let mut total = 0usize;
    let mut written = 0usize;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
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
        // Rebuilt from the parsed id: no archive-supplied path reaches the filesystem.
        let name = format!("{id}.yaml");

        let size = usize::try_from(entry.header().size()?).unwrap_or(usize::MAX);
        total = total.saturating_add(size);
        if total > MAX_BUNDLE_SIZE {
            bail!("unpacked schema bundle exceeds {MAX_BUNDLE_SIZE} bytes");
        }

        let mut file = std::fs::File::create(dest.join(&name))?;
        std::io::copy(&mut entry, &mut file)
            .with_context(|| format!("couldn't write schema '{name}'"))?;
        written += 1;
    }

    if written == 0 {
        bail!("schema bundle contains no <id>.yaml entries");
    }
    Ok(())
}

/// Parses `<id>.yaml` into its id. Digits only, no leading zeros — `+7.yaml` (which `str::parse`
/// accepts) and `007.yaml` would let one bundle define id 7 twice, resolved by directory order.
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

async fn load_dir(dir: Utf8PathBuf) -> anyhow::Result<HashMap<SchemaId, Arc<DatasetDescription>>> {
    tokio::task::spawn_blocking(move || {
        let mut schemas = HashMap::new();
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            let Some(id) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(schema_id)
            else {
                continue;
            };
            let yaml = std::fs::read_to_string(&path)
                .with_context(|| format!("couldn't read schema {}", path.display()))?;
            let description = sqd_query_engine::metadata::parse_dataset_description(&yaml)
                .map_err(|e| anyhow::anyhow!("couldn't parse schema {}: {e:?}", path.display()))?;
            schemas.insert(id, Arc::new(description));
        }
        if schemas.is_empty() {
            bail!("no schemas found in {dir}");
        }
        Ok(schemas)
    })
    .await
    .context("schema load task panicked")?
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"
name: evm
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number:
        type: uint64
"#;

    /// Writes names straight into the header: `Builder::append_data` refuses to produce a `..`.
    fn targz(entries: &[(&str, &[u8])]) -> Vec<u8> {
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

    async fn serve_once(body: Vec<u8>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let mut response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .into_bytes();
            response.extend_from_slice(&body);
            let _ = socket.write_all(&response).await;
        });
        url
    }

    fn store(dir: &tempfile::TempDir) -> Arc<SchemaRegistry> {
        Arc::new(SchemaRegistry::open(
            Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap(),
        ))
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

    #[tokio::test]
    async fn merges_a_verified_bundle_into_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let registry = Arc::clone(&store);
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let bundle = SchemaBundle {
            hash: BundleHash::of(&archive),
            url: serve_once(archive).await,
        };

        store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .unwrap();

        assert_eq!(registry.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
        assert_eq!(store.installed_hash(), Some(bundle.hash));
        assert_eq!(store.bundle_ids(), HashSet::from([SchemaId::new(7)]));
        assert_eq!(stored(&dir), vec!["7.yaml"]);
    }

    #[tokio::test]
    async fn preparing_a_bundle_does_not_make_it_active() {
        let dir = tempfile::tempdir().unwrap();
        let registry = store(&dir);
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let bundle = SchemaBundle {
            hash: BundleHash::of(&archive),
            url: serve_once(archive).await,
        };

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
    async fn dropping_a_prepared_bundle_does_not_publish_new_schemas() {
        let dir = tempfile::tempdir().unwrap();
        let registry = store(&dir);
        let original = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let original_bundle = SchemaBundle {
            hash: BundleHash::of(&original),
            url: serve_once(original).await,
        };
        registry
            .ensure(&original_bundle, &reqwest::Client::new())
            .await
            .unwrap();

        let replacement = targz(&[("9.yaml", SCHEMA.as_bytes())]);
        let replacement_bundle = SchemaBundle {
            hash: BundleHash::of(&replacement),
            url: serve_once(replacement).await,
        };
        let prepared = registry
            .prepare_bundle(&replacement_bundle, &reqwest::Client::new())
            .await
            .unwrap();

        assert_eq!(registry.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
        assert!(registry.get_by_id(SchemaId::new(9)).is_err());
        drop(prepared);
        drop(registry);

        let restarted = store(&dir);
        assert_eq!(restarted.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
        assert!(restarted.get_by_id(SchemaId::new(9)).is_err());
    }

    /// Bundles accumulate. The network publishes only its current bundle and offers no way back
    /// to an older one, so a schema dropped here would strand every chunk written with it.
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
            let bundle = SchemaBundle {
                hash: BundleHash::of(&archive),
                url: serve_once(archive).await,
            };
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

        // Coverage is judged against the bundle in force, never the accumulated set (ADR-21).
        assert_eq!(store.bundle_ids(), HashSet::from([SchemaId::new(12)]));
    }

    /// Schema ids are immutable because stored chunks retain only the id that describes them.
    #[tokio::test]
    async fn a_republished_id_with_different_contents_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let registry = Arc::clone(&store);

        let original = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        store
            .ensure(
                &SchemaBundle {
                    hash: BundleHash::of(&original),
                    url: serve_once(original).await,
                },
                &reqwest::Client::new(),
            )
            .await
            .unwrap();

        let changed = targz(&[(
            "7.yaml",
            SCHEMA.replace("name: evm", "name: evm2").as_bytes(),
        )]);
        let err = store
            .ensure(
                &SchemaBundle {
                    hash: BundleHash::of(&changed),
                    url: serve_once(changed).await,
                },
                &reqwest::Client::new(),
            )
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("republished with different contents"));
        assert_eq!(registry.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
        assert_eq!(stored(&dir), vec!["7.yaml"]);
    }

    #[tokio::test]
    async fn an_identical_cached_schema_is_not_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let first = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        store
            .ensure(
                &SchemaBundle {
                    hash: BundleHash::of(&first),
                    url: serve_once(first).await,
                },
                &reqwest::Client::new(),
            )
            .await
            .unwrap();
        let path = dir.path().join("7.yaml");
        let modified = std::fs::metadata(&path).unwrap().modified().unwrap();

        let second = targz(&[("7.yaml", SCHEMA.as_bytes()), ("9.yaml", SCHEMA.as_bytes())]);
        store
            .ensure(
                &SchemaBundle {
                    hash: BundleHash::of(&second),
                    url: serve_once(second).await,
                },
                &reqwest::Client::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            std::fs::metadata(path).unwrap().modified().unwrap(),
            modified
        );
        assert_eq!(stored(&dir), vec!["7.yaml", "9.yaml"]);
    }

    /// Models a retry after activation copied one file and failed before publishing the registry.
    /// The cached file needs no second write, but must still enter the in-memory snapshot.
    #[tokio::test]
    async fn retry_publishes_an_identical_schema_cached_by_a_partial_install() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        std::fs::write(dir.path().join("7.yaml"), SCHEMA).unwrap();
        assert!(store.get_by_id(SchemaId::new(7)).is_err());

        let archive = targz(&[("7.yaml", SCHEMA.as_bytes()), ("9.yaml", SCHEMA.as_bytes())]);
        let bundle = SchemaBundle {
            hash: BundleHash::of(&archive),
            url: serve_once(archive).await,
        };
        store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .unwrap();

        assert!(store.get_by_id(SchemaId::new(7)).is_ok());
        assert!(store.get_by_id(SchemaId::new(9)).is_ok());
        assert_eq!(stored(&dir), vec!["7.yaml", "9.yaml"]);
    }

    #[tokio::test]
    async fn rejects_a_bundle_whose_hash_does_not_match() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let registry = Arc::clone(&store);
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let bundle = SchemaBundle {
            hash: BundleHash::of(b"something else"),
            url: serve_once(archive).await,
        };

        let err = store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("hash mismatch"), "{err:#}");
        // Nothing merged, so the next poll offers the same bundle again.
        assert!(store.installed_hash().is_none());
        assert!(registry.get_by_id(SchemaId::new(7)).is_err());
        assert!(stored(&dir).is_empty());
    }

    #[tokio::test]
    async fn a_bundle_that_will_not_parse_leaves_nothing_behind() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let registry = Arc::clone(&store);
        let archive = targz(&[("7.yaml", b"this is not a dataset description")]);
        let bundle = SchemaBundle {
            hash: BundleHash::of(&archive),
            url: serve_once(archive).await,
        };

        let err = store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("couldn't load"), "{err:#}");
        assert!(store.installed_hash().is_none());
        assert!(registry.get_by_id(SchemaId::new(7)).is_err());
        assert!(
            stored(&dir).is_empty(),
            "a staged bundle must not survive the attempt that made it: {:?}",
            stored(&dir)
        );
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
        let bundle = SchemaBundle {
            hash: BundleHash::of(&archive),
            url: serve_once(archive).await,
        };

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

    /// The store outlives the process, so a restart answers queries for chunks already on disk
    /// without waiting for a download.
    #[tokio::test]
    async fn a_restart_adopts_the_stored_schemas() {
        let dir = tempfile::tempdir().unwrap();
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let hash = BundleHash::of(&archive);
        {
            let store = store(&dir);
            let bundle = SchemaBundle {
                hash,
                url: serve_once(archive).await,
            };
            store
                .ensure(&bundle, &reqwest::Client::new())
                .await
                .unwrap();
        }

        // The one-shot server is spent, so nothing below can have come over the network.
        let store = store(&dir);
        let registry = Arc::clone(&store);
        assert_eq!(registry.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
        assert_eq!(
            store.installed_hash(),
            None,
            "adopted schemas belong to no bundle this process merged"
        );
        assert!(
            store.bundle_ids().is_empty(),
            "so they cannot admit an assignment on their own (ADR-21)"
        );
    }

    /// A stored schema that won't parse is skipped rather than fatal — the rest still answer,
    /// and an operator can remove it before a valid bundle restores it.
    #[test]
    fn an_unreadable_stored_schema_is_skipped_at_startup() {
        let dir = tempfile::tempdir().unwrap();
        let root = Utf8Path::from_path(dir.path()).unwrap();
        std::fs::write(root.join("7.yaml"), SCHEMA).unwrap();
        std::fs::write(root.join("9.yaml"), b"not a schema").unwrap();

        let registry = store(&dir);

        assert!(registry.get_by_id(SchemaId::new(7)).is_ok());
        assert!(registry.get_by_id(SchemaId::new(9)).is_err());
    }

    #[test]
    fn restored_bundle_schemas_do_not_mark_legacy_schemas_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let root = Utf8Path::from_path(dir.path()).unwrap();
        std::fs::write(root.join("7.yaml"), SCHEMA).unwrap();

        let registry = store(&dir);

        assert!(matches!(
            registry.get_by_type("evm"),
            Err(QueryError::Other(_))
        ));
        assert!(registry.get_by_id(SchemaId::new(7)).is_ok());
    }

    #[test]
    fn construction_sweeps_staging_but_keeps_stored_schemas() {
        let dir = tempfile::tempdir().unwrap();
        let root = Utf8Path::from_path(dir.path()).unwrap();
        std::fs::create_dir_all(root.join(format!("{TEMP_PREFIX}abcd"))).unwrap();
        std::fs::write(root.join(format!("{TEMP_PREFIX}stray")), b"x").unwrap();
        std::fs::write(root.join("7.yaml"), SCHEMA).unwrap();
        // Anything the store didn't write is left alone.
        std::fs::write(root.join("unrelated"), b"keep me").unwrap();

        let _store = store(&dir);

        let mut left = stored_all(&dir);
        left.sort();
        assert_eq!(left, vec!["7.yaml", "unrelated"]);
    }

    /// Names in the store directory, `<id>.yaml` only.
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
