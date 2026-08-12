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

use super::experimental_engine::QuerySchemaRegistry;
use crate::metrics;
use crate::types::schema::SchemaId;

/// Caps both the download and the unpacked total: the only bound on a corrupt or hostile archive
/// before the hash is verified. Published bundles are tens of kilobytes.
const MAX_BUNDLE_SIZE: usize = 64 * 1024 * 1024;

/// Directory holding a bundle's unpacked schemas, named after its content hash.
const UNPACKED_PREFIX: &str = "sha256-";
/// Staging directory for an unpack in progress, renamed onto its final name once complete.
const TEMP_PREFIX: &str = "temp-";

/// A schema bundle the network published: hash parsed, address left to the fetch to judge.
#[derive(Clone, Debug)]
pub struct Bundle {
    pub hash: BundleHash,
    pub url: String,
}

impl TryFrom<sqd_assignments::SchemaBundle> for Bundle {
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

/// Keeps the current bundle unpacked under `dir` and its schemas installed in `registry`.
pub struct SchemaBundleStore {
    dir: Utf8PathBuf,
    registry: Arc<QuerySchemaRegistry>,
    /// Held for the whole of [`Self::install`], which is a check-then-act (skip if this hash is
    /// already installed) wrapping a read-modify-write (the carry-over in `store_bundle`).
    /// Uncontended today — the assignments loop is the only caller — so this costs nothing and
    /// stops a second caller from being a silent lost update rather than a compile error.
    install_lock: tokio::sync::Mutex<()>,
}

impl SchemaBundleStore {
    /// Sweeps staging directories a crashed unpack left behind; nothing else does, since
    /// [`Self::prune`] runs only after a successful install (legacy assignments never install one).
    /// Unpacked `sha256-*` dirs stay — they make a restart cheap; prune drops superseded ones.
    pub fn new(dir: impl Into<Utf8PathBuf>, registry: Arc<QuerySchemaRegistry>) -> Self {
        let dir = dir.into();
        // Blocking, but it's startup and the files are few and small (as in `StateManager::new`).
        let swept = remove_bundle_dirs(&dir, &[TEMP_PREFIX], None);
        if swept > 0 {
            tracing::debug!(swept, "Removed staging directories left by an earlier run");
        }
        Self {
            dir,
            registry,
            install_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Hash of the installed bundle, straight from the registry it describes. Set only by a
    /// successful install, so a failure leaves the previous value and the next poll re-offers
    /// the bundle rather than deduplicating it away.
    pub fn installed_hash(&self) -> Option<BundleHash> {
        self.registry.bundle_hash()
    }

    /// Installs `bundle`'s schemas into the registry, downloading and unpacking only if the
    /// hash-named directory isn't already on disk. `still_in_use` names write schemas the current
    /// assignment relies on; they survive the replacement even if the new bundle drops them — see
    /// [`QuerySchemaRegistry::store_bundle`].
    pub async fn ensure(
        &self,
        bundle: &Bundle,
        client: &reqwest::Client,
        still_in_use: &HashSet<SchemaId>,
    ) -> anyhow::Result<()> {
        match self.install(bundle, client, still_in_use).await {
            Ok(()) => Ok(()),
            Err(e) => {
                metrics::SCHEMA_BUNDLE_FAILURES.inc();
                Err(e)
            }
        }
    }

    async fn install(
        &self,
        bundle: &Bundle,
        client: &reqwest::Client,
        still_in_use: &HashSet<SchemaId>,
    ) -> anyhow::Result<()> {
        let _installing = self.install_lock.lock().await;

        if self.installed_hash() == Some(bundle.hash) {
            tracing::debug!(hash = %bundle.hash, "Schema bundle unchanged");
            return Ok(());
        }

        let dir = self.dir.join(format!("{UNPACKED_PREFIX}{:x}", bundle.hash));
        let schemas = match self.load_unpacked(&dir).await {
            Some(schemas) => schemas,
            None => {
                let bytes = download(&bundle.url, client).await.with_context(|| {
                    format!("couldn't download schema bundle from {}", bundle.url)
                })?;
                let actual = BundleHash::of(&bytes);
                if actual != bundle.hash {
                    bail!(
                        "schema bundle hash mismatch: expected {}, got {actual}",
                        bundle.hash
                    );
                }
                unpack(bytes, self.dir.clone(), dir.clone())
                    .await
                    .context("couldn't unpack schema bundle")?;
                load_dir(dir.clone())
                    .await
                    .with_context(|| format!("couldn't load schema bundle from {dir}"))?
            }
        };

        tracing::info!(hash = %bundle.hash, schemas = schemas.len(), "Loaded schema bundle");
        self.registry
            .store_bundle(schemas, still_in_use, bundle.hash);
        metrics::SCHEMA_BUNDLE_LOADED.set(1);

        self.prune(dir).await;
        Ok(())
    }

    /// Reads back a bundle an earlier run unpacked, or `None` if there isn't a usable one. One
    /// that exists but won't load is discarded rather than fatal: it would otherwise suppress the
    /// re-download that fixes it, and a bundle that never installs blocks every assignment.
    async fn load_unpacked(
        &self,
        dir: &Utf8Path,
    ) -> Option<HashMap<SchemaId, Arc<DatasetDescription>>> {
        if !dir.exists() {
            return None;
        }
        match load_dir(dir.to_owned()).await {
            Ok(schemas) => Some(schemas),
            Err(e) => {
                tracing::warn!(
                    %dir,
                    error = ?e,
                    "Unpacked schema bundle is unusable; discarding it and fetching again"
                );
                if let Err(e) = tokio::fs::remove_dir_all(dir).await {
                    tracing::warn!(%dir, error = %e, "Couldn't remove the unusable schema bundle");
                }
                None
            }
        }
    }

    /// Removes every unpacked or half-unpacked bundle other than `keep`. Best-effort.
    async fn prune(&self, keep: Utf8PathBuf) {
        let dir = self.dir.clone();
        let removed = tokio::task::spawn_blocking(move || {
            remove_bundle_dirs(&dir, &[UNPACKED_PREFIX, TEMP_PREFIX], Some(&keep))
        })
        .await
        .unwrap_or(0);
        if removed > 0 {
            tracing::debug!(removed, "Pruned stale schema bundles");
        }
    }
}

/// Removes directories under `dir` whose name starts with one of `prefixes`, except `keep`,
/// returning how many went. Failures are logged, not propagated: the next install sweeps them.
fn remove_bundle_dirs(dir: &Utf8Path, prefixes: &[&str], keep: Option<&Utf8Path>) -> usize {
    let mut removed = 0usize;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return removed;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !prefixes.iter().any(|prefix| name.starts_with(prefix)) {
            continue;
        }
        if keep.is_some_and(|keep| path == keep.as_std_path()) {
            continue;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(()) => removed += 1,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "Couldn't remove stale schema bundle")
            }
        }
    }
    removed
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
async fn unpack(bytes: Vec<u8>, parent: Utf8PathBuf, dest: Utf8PathBuf) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        let name = dest.file_name().unwrap_or("bundle");
        let temp = parent.join(format!("{TEMP_PREFIX}{name}"));
        std::fs::create_dir_all(&parent)?;
        if temp.exists() {
            std::fs::remove_dir_all(&temp)?;
        }
        std::fs::create_dir(&temp)?;

        let result = extract_schemas(&bytes, &temp);
        if result.is_err() {
            let _ = std::fs::remove_dir_all(&temp);
        }
        result?;

        std::fs::rename(&temp, &dest)
            .with_context(|| format!("couldn't move unpacked bundle into {dest}"))?;
        Ok(())
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
    async fn downloads_verifies_and_caches_a_bundle() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let bundle = Bundle {
            hash: BundleHash::of(&archive),
            url: serve_once(archive).await,
        };

        store
            .ensure(&bundle, &reqwest::Client::new(), &HashSet::new())
            .await
            .unwrap();

        assert_eq!(registry.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
        assert_eq!(registry.get("evm").unwrap().name, "evm");
        assert!(registry.get_by_id(SchemaId::new(8)).is_err());
        assert_eq!(store.installed_hash(), Some(bundle.hash));
    }

    #[tokio::test]
    async fn rejects_a_bundle_whose_hash_does_not_match() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let bundle = Bundle {
            hash: BundleHash::of(b"something else"),
            url: serve_once(archive).await,
        };

        let err = store
            .ensure(&bundle, &reqwest::Client::new(), &HashSet::new())
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("hash mismatch"), "{err:#}");
        // Nothing installed, so the next poll offers the same bundle again.
        assert!(store.installed_hash().is_none());
        assert!(registry.get("evm").is_err());
        // Nothing is left behind for a later run to mistake for a verified bundle.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn ignores_entries_that_are_not_root_level_id_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);
        let archive = targz(&[
            ("7.yaml", SCHEMA.as_bytes()),
            ("manifest.json", b"{}"),
            ("nested/9.yaml", SCHEMA.as_bytes()),
            ("../escape.yaml", b"nope"),
        ]);
        let bundle = Bundle {
            hash: BundleHash::of(&archive),
            url: serve_once(archive).await,
        };

        store
            .ensure(&bundle, &reqwest::Client::new(), &HashSet::new())
            .await
            .unwrap();

        assert!(
            registry.get_by_id(SchemaId::new(7)).is_ok(),
            "the root-level <id>.yaml is loaded"
        );
        assert!(
            registry.get_by_id(SchemaId::new(9)).is_err(),
            "nested entries are not unpacked"
        );
        assert!(
            !dir.path().parent().unwrap().join("escape.yaml").exists(),
            "traversal entries must not escape the store directory"
        );
    }

    #[tokio::test]
    async fn reuses_the_unpacked_copy_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let hash = BundleHash::of(&archive);
        let url = serve_once(archive).await;

        store(&dir)
            .0
            .ensure(
                &Bundle {
                    hash: hash.clone(),
                    url: url.clone(),
                },
                &reqwest::Client::new(),
                &HashSet::new(),
            )
            .await
            .unwrap();

        // The one-shot server is spent, so a second fetch could only succeed from disk.
        let (restarted, registry) = store(&dir);
        restarted
            .ensure(
                &Bundle { hash, url },
                &reqwest::Client::new(),
                &HashSet::new(),
            )
            .await
            .unwrap();
        assert_eq!(registry.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
    }

    /// A damaged hash-named directory must not suppress the re-download that fixes it, or a
    /// worker that can't install a bundle wedges with no way back.
    #[tokio::test]
    async fn recovers_from_an_unusable_unpacked_bundle() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let hash = BundleHash::of(&archive);

        // An empty directory under the hash this bundle resolves to: exists, but won't load.
        let damaged = dir.path().join(format!("{UNPACKED_PREFIX}{hash:x}"));
        std::fs::create_dir_all(&damaged).unwrap();

        let bundle = Bundle {
            hash,
            url: serve_once(archive).await,
        };
        store
            .ensure(&bundle, &reqwest::Client::new(), &HashSet::new())
            .await
            .expect("the damaged copy is discarded and the bundle fetched again");

        assert_eq!(registry.get_by_id(SchemaId::new(7)).unwrap().name, "evm");
        assert_eq!(store.installed_hash(), Some(bundle.hash));
    }

    /// Construction is the only thing that sweeps a crashed unpack's staging directory (`prune`
    /// runs only after an install, which may never come), and it must spare unpacked bundles.
    #[test]
    fn construction_sweeps_staging_directories_but_keeps_unpacked_bundles() {
        let dir = tempfile::tempdir().unwrap();
        let hex = "a".repeat(64);
        let unpacked = format!("{UNPACKED_PREFIX}{hex}");
        std::fs::create_dir_all(dir.path().join(format!("{TEMP_PREFIX}{unpacked}"))).unwrap();
        std::fs::create_dir_all(dir.path().join(&unpacked)).unwrap();
        std::fs::write(dir.path().join("unrelated"), b"keep me").unwrap();

        let _ = store(&dir);

        let mut left: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left, [unpacked, "unrelated".to_owned()]);
    }

    #[tokio::test]
    async fn prunes_the_previous_bundle() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);

        for body in [SCHEMA, &SCHEMA.replace("name: evm", "name: solana")] {
            let archive = targz(&[("7.yaml", body.as_bytes())]);
            let bundle = Bundle {
                hash: BundleHash::of(&archive),
                url: serve_once(archive).await,
            };
            store
                .ensure(&bundle, &reqwest::Client::new(), &HashSet::new())
                .await
                .unwrap();
        }

        assert_eq!(
            registry.get_by_id(SchemaId::new(7)).unwrap().name,
            "solana",
            "the newer bundle replaced the older one in the registry"
        );
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            1,
            "the superseded bundle's directory is removed"
        );
    }
}
