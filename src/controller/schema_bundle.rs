//! The network's schema bundle: a gzipped tar of `<schema_id>.yaml` query-engine schemas,
//! published alongside the assignments that reference those ids. In worker-assignment mode it
//! replaces the CDN manifest as the source of query schemas; only query execution reads it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use sqd_query_engine::metadata::DatasetDescription;
use tempfile::TempDir;

use super::experimental_engine::QuerySchemaRegistry;
use crate::metrics;
use crate::types::schema::SchemaId;

/// Caps both the download and the unpacked total: the only bound on a corrupt or hostile archive
/// before the hash is verified. Published bundles are tens of kilobytes.
const MAX_BUNDLE_SIZE: usize = 64 * 1024 * 1024;

/// Staging prefix for a file or directory in flight. Nothing under it is ever read back, so a
/// crash leaves only garbage the next startup sweeps.
const TEMP_PREFIX: &str = "temp-";
/// What a stored schema is called. The store is keyed by schema id, not by bundle: bundles are
/// merged into it and the ids accumulate.
const SCHEMA_SUFFIX: &str = ".yaml";

/// A schema bundle the network published: hash parsed, address left to the fetch to judge.
#[derive(Clone, Debug)]
pub struct SchemaBundle {
    pub hash: BundleHash,
    pub url: String,
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

/// The sha256 of a schema bundle, parsed once where it enters from the network so that every
/// comparison downstream is 32 bytes rather than a string that may or may not be well-formed.
///
/// `Display` renders the wire form `sha256:<hex>`; `LowerHex` renders the bare hex the unpacked
/// directory is named after.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BundleHash([u8; 32]);

impl BundleHash {
    /// The hash of `bytes`, for checking a download against what the network advertised.
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

/// Same as `Display`: a hash has one readable form, and logs carry it either way.
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
pub struct SchemaBundleStore {
    dir: Utf8PathBuf,
    registry: Arc<QuerySchemaRegistry>,
    /// Guards the merge: writing the files and publishing them to the registry are one step.
    /// Nothing under it awaits.
    merge_lock: parking_lot::Mutex<()>,
    /// Ids that were loaded when an assignment settled but that the assignment does not
    /// reference. Safe to reclaim: a settled assignment has completed its removals, so no chunk
    /// on disk was written with them.
    ///
    /// FIXME: nothing acts on this yet — the store only grows. Reclaiming must key on this set
    /// and never on the current assignment alone, since anything deleted cannot be re-fetched.
    unused: parking_lot::Mutex<HashSet<SchemaId>>,
}

impl SchemaBundleStore {
    /// Adopts whatever an earlier run left in `dir` and sweeps anything half-written.
    ///
    /// Adopted schemas can answer queries immediately, which matters for the chunks already on
    /// disk, but they belong to no bundle this process has merged — so they cannot admit an
    /// assignment until a bundle arrives (ADR-21).
    pub fn new(dir: impl Into<Utf8PathBuf>, registry: Arc<QuerySchemaRegistry>) -> Self {
        let dir = dir.into();
        // Blocking, but it's startup and the files are few and small (as in `StateManager::new`).
        let swept = sweep_staging(&dir);
        if swept > 0 {
            tracing::debug!(swept, "Removed staging files left by an earlier run");
        }
        match read_store(&dir) {
            Ok(schemas) if !schemas.is_empty() => {
                tracing::info!(schemas = schemas.len(), "Adopted stored query schemas");
                registry.adopt_local(schemas);
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(%dir, error = ?e, "Couldn't read the schema store"),
        }
        Self {
            dir,
            registry,
            merge_lock: parking_lot::Mutex::new(()),
            unused: parking_lot::Mutex::new(HashSet::new()),
        }
    }

    /// Hash of the last bundle merged. Set only on success, so a failure leaves the previous
    /// value and the next poll re-offers the bundle rather than deduplicating it away.
    pub fn installed_hash(&self) -> Option<BundleHash> {
        self.registry.bundle_hash()
    }

    /// Ids the bundle in force carried — what an assignment's coverage is judged against.
    pub fn bundle_ids(&self) -> HashSet<SchemaId> {
        self.registry.bundle_ids()
    }

    /// Records schemas the settled assignment doesn't reference as reclaimable. See
    /// [`Self::unused`]; nothing deletes them yet.
    pub fn mark_unused_after_settle(&self, in_use: &HashSet<SchemaId>) {
        let loaded = self.registry.loaded_ids();
        let mut unused = self.unused.lock();
        unused.extend(loaded.difference(in_use).copied());
        unused.retain(|id| !in_use.contains(id));
    }

    /// Downloads `bundle` unless it is already the one in force, and merges it into the store.
    pub async fn ensure(
        &self,
        bundle: &SchemaBundle,
        client: &reqwest::Client,
    ) -> anyhow::Result<()> {
        match self.merge(bundle, client).await {
            Ok(()) => Ok(()),
            Err(e) => {
                metrics::SCHEMA_BUNDLE_FAILURES.inc();
                Err(e)
            }
        }
    }

    async fn merge(&self, bundle: &SchemaBundle, client: &reqwest::Client) -> anyhow::Result<()> {
        if self.installed_hash() == Some(bundle.hash) {
            tracing::debug!(hash = %bundle.hash, "Schema bundle unchanged");
            return Ok(());
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

        tracing::info!(hash = %bundle.hash, schemas = schemas.len(), "Merging schema bundle");
        self.install(&staged_path, schemas, bundle.hash)
    }

    /// Moves each staged schema into the store and publishes the bundle to the registry.
    ///
    /// Per file rather than per bundle, because the store is a union: a crash partway leaves a
    /// smaller union, which the next merge completes. There is no state in which the store
    /// claims to hold something it doesn't.
    fn install(
        &self,
        staged: &Utf8Path,
        schemas: HashMap<SchemaId, Arc<DatasetDescription>>,
        hash: BundleHash,
    ) -> anyhow::Result<()> {
        let _merging = self.merge_lock.lock();
        std::fs::create_dir_all(&self.dir)?;
        for id in schemas.keys() {
            let name = format!("{id}{SCHEMA_SUFFIX}");
            std::fs::rename(staged.join(&name), self.dir.join(&name))
                .with_context(|| format!("couldn't move schema {id} into the store"))?;
        }
        self.registry.merge_bundle(schemas, hash);
        metrics::SCHEMA_BUNDLE_LOADED.set(1);
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

/// Reads the store as it stands. A schema that won't parse is skipped rather than fatal: the
/// rest still answer, and a bundle carrying that id will overwrite it.
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

/// Buffers in memory: nothing can be trusted until the whole thing is hashed.
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

    fn store(dir: &tempfile::TempDir) -> (SchemaBundleStore, Arc<QuerySchemaRegistry>) {
        let registry = Arc::new(QuerySchemaRegistry::default());
        let store = SchemaBundleStore::new(
            Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap(),
            registry.clone(),
        );
        (store, registry)
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
        let (store, registry) = store(&dir);
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

    /// Bundles accumulate. The network publishes only its current bundle and offers no way back
    /// to an older one, so a schema dropped here would strand every chunk written with it.
    #[tokio::test]
    async fn a_later_bundle_adds_to_the_store_rather_than_replacing_it() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);

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

    /// Republishing an id is the sanctioned way to correct a schema in place. It changes the
    /// bundle hash, so it is not deduplicated away.
    #[tokio::test]
    async fn a_republished_id_overwrites_the_stored_copy() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);

        for body in [SCHEMA.to_owned(), SCHEMA.replace("name: evm", "name: evm2")] {
            let archive = targz(&[("7.yaml", body.as_bytes())]);
            let bundle = SchemaBundle {
                hash: BundleHash::of(&archive),
                url: serve_once(archive).await,
            };
            store
                .ensure(&bundle, &reqwest::Client::new())
                .await
                .unwrap();
        }

        assert_eq!(registry.get_by_id(SchemaId::new(7)).unwrap().name, "evm2");
        assert_eq!(stored(&dir), vec!["7.yaml"]);
    }

    #[tokio::test]
    async fn rejects_a_bundle_whose_hash_does_not_match() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);
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
        let (store, registry) = store(&dir);
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
        let (store, registry) = store(&dir);
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
            let (store, _) = store(&dir);
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
        let (store, registry) = store(&dir);
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
    /// and a bundle carrying that id overwrites it.
    #[test]
    fn an_unreadable_stored_schema_is_skipped_at_startup() {
        let dir = tempfile::tempdir().unwrap();
        let root = Utf8Path::from_path(dir.path()).unwrap();
        std::fs::write(root.join("7.yaml"), SCHEMA).unwrap();
        std::fs::write(root.join("9.yaml"), b"not a schema").unwrap();

        let (_store, registry) = store(&dir);

        assert!(registry.get_by_id(SchemaId::new(7)).is_ok());
        assert!(registry.get_by_id(SchemaId::new(9)).is_err());
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

        let _ = store(&dir);

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
