//! The network's schema bundle: a gzipped tar of `<schema_id>.yaml` query-engine schemas,
//! published alongside the assignments that reference those ids.
//!
//! In worker-assignment mode this replaces the CDN manifest as the source of query schemas: an
//! installed bundle is pushed into the [`QuerySchemaRegistry`] the experimental engine reads.
//! Chunk downloads don't consult it — a worker assignment carries its own table rosters — so the
//! bundle's only consumer is query execution.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context};
use arc_swap::ArcSwapOption;
use camino::{Utf8Path, Utf8PathBuf};
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use sqd_assignments::SchemaBundle;
use sqd_query_engine::metadata::DatasetDescription;

use super::experimental_engine::QuerySchemaRegistry;
use crate::metrics;

/// Caps both the download and the unpacked total. Published bundles are tens of kilobytes of
/// YAML; this exists only so a corrupt or hostile archive can't fill the data directory. The
/// hash is verified after the download, so the cap is the only thing bounding it before that.
const MAX_BUNDLE_SIZE: usize = 64 * 1024 * 1024;

/// Directory holding a bundle's unpacked schemas, named after its content hash.
const UNPACKED_PREFIX: &str = "sha256-";
/// Staging directory for an unpack in progress, renamed onto its final name once complete.
const TEMP_PREFIX: &str = "temp-";

/// Keeps the current bundle unpacked under `dir` and its schemas installed in `registry`.
pub struct SchemaBundleStore {
    dir: Utf8PathBuf,
    registry: Arc<QuerySchemaRegistry>,
    /// Hash of the bundle currently in `registry`. Only set once schemas are actually installed,
    /// so a failed install leaves this on the previous value and the next poll retries.
    installed_hash: ArcSwapOption<String>,
}

impl SchemaBundleStore {
    /// Sweeps staging directories a crashed unpack left behind. Nothing else would:
    /// [`Self::prune`] only runs after a successful install, which may never come — a worker
    /// running legacy assignments never installs a bundle at all.
    ///
    /// Only `temp-*` goes. An unpacked `sha256-*` is not garbage: it is very likely the bundle
    /// this run is about to reuse, and removing it here would turn every restart into a
    /// re-download. Superseded ones are pruned by the first successful install instead.
    pub fn new(dir: impl Into<Utf8PathBuf>, registry: Arc<QuerySchemaRegistry>) -> Self {
        let dir = dir.into();
        // Blocking, but this is startup and the directory holds a handful of small files — the
        // same trade-off `StateManager::new` makes with `remove_temps`.
        let swept = remove_bundle_dirs(&dir, &[TEMP_PREFIX], None);
        if swept > 0 {
            tracing::debug!(swept, "Removed staging directories left by an earlier run");
        }
        Self {
            dir,
            registry,
            installed_hash: ArcSwapOption::empty(),
        }
    }

    pub fn installed_hash(&self) -> Option<String> {
        self.installed_hash.load_full().map(|h| h.to_string())
    }

    /// Installs `bundle`'s schemas into the registry, downloading and unpacking only if needed.
    ///
    /// A bundle already installed is a no-op, and one already unpacked on disk (from an earlier
    /// run) is read back rather than re-downloaded — the hash names the directory, so a match
    /// means the content matches. Directories for other bundles are pruned once the new one is
    /// live.
    pub async fn ensure(
        &self,
        bundle: &SchemaBundle,
        client: &reqwest::Client,
    ) -> anyhow::Result<()> {
        match self.install(bundle, client).await {
            Ok(()) => Ok(()),
            Err(e) => {
                metrics::SCHEMA_BUNDLE_FAILURES.inc();
                Err(e)
            }
        }
    }

    async fn install(&self, bundle: &SchemaBundle, client: &reqwest::Client) -> anyhow::Result<()> {
        let hex = parse_sha256(&bundle.hash)?;

        if self.installed_hash().as_deref() == Some(bundle.hash.as_str()) {
            tracing::debug!(hash = %bundle.hash, "Schema bundle unchanged");
            return Ok(());
        }

        let dir = self.dir.join(format!("{UNPACKED_PREFIX}{hex}"));
        let schemas = match self.load_unpacked(&dir).await {
            Some(schemas) => schemas,
            None => {
                let bytes = download(&bundle.url, client).await.with_context(|| {
                    format!("couldn't download schema bundle from {}", bundle.url)
                })?;
                verify_sha256(&bytes, hex)?;
                unpack(bytes, self.dir.clone(), dir.clone())
                    .await
                    .context("couldn't unpack schema bundle")?;
                load_dir(dir.clone())
                    .await
                    .with_context(|| format!("couldn't load schema bundle from {dir}"))?
            }
        };

        tracing::info!(hash = %bundle.hash, schemas = schemas.len(), "Loaded schema bundle");
        self.registry.store_bundle(schemas);
        self.installed_hash
            .store(Some(Arc::new(bundle.hash.clone())));
        metrics::SCHEMA_BUNDLE_LOADED.set(1);

        self.prune(dir).await;
        Ok(())
    }

    /// Reads back a bundle an earlier run unpacked, or `None` if there isn't a usable one.
    ///
    /// A directory that exists but won't load is discarded rather than treated as fatal. The
    /// unpack is atomic, so this means damage after the fact — a [`Self::prune`] that failed
    /// partway, say. Without this the hash-named directory would suppress the re-download that
    /// would fix it, and in worker-assignment mode a bundle that never installs blocks every
    /// assignment: the worker would wedge permanently on a state it can't recover from.
    async fn load_unpacked(&self, dir: &Utf8Path) -> Option<HashMap<u32, Arc<DatasetDescription>>> {
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

    /// Removes every unpacked or half-unpacked bundle other than `keep`. Best-effort: a stale
    /// directory wastes a few kilobytes, which isn't worth failing an otherwise-good bundle over.
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

/// Removes every directory under `dir` whose name starts with one of `prefixes`, except `keep`,
/// returning how many went. Failures are logged rather than propagated: a leftover directory
/// costs a few kilobytes and the next successful install sweeps it.
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

/// Splits `algorithm:hex`, accepting only sha256. Unknown algorithms are rejected rather than
/// skipped — silently not checking an integrity hash is worse than refusing the bundle.
fn parse_sha256(hash: &str) -> anyhow::Result<&str> {
    let (algorithm, hex) = hash
        .split_once(':')
        .with_context(|| format!("schema bundle hash '{hash}' is not in 'algorithm:hex' form"))?;
    if algorithm != "sha256" {
        bail!("unsupported schema bundle hash algorithm '{algorithm}', expected sha256");
    }
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("schema bundle hash '{hex}' is not 64 hex digits");
    }
    Ok(hex)
}

fn verify_sha256(bytes: &[u8], expected_hex: &str) -> anyhow::Result<()> {
    let actual = hex_encode(&Sha256::digest(bytes));
    if !actual.eq_ignore_ascii_case(expected_hex) {
        bail!("schema bundle hash mismatch: expected {expected_hex}, got {actual}");
    }
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// Buffers the bundle in memory: it must be hashed as a whole before any of it is trusted, and
/// the published bundles are far smaller than [`MAX_BUNDLE_SIZE`].
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

/// Extracts into a staging directory under `parent`, then renames it onto `dest` so a crash
/// mid-unpack can never leave a partial bundle under a hash that claims to be complete.
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

/// Writes out every `<id>.yaml` at the archive root.
///
/// Anything else — nested paths, a manifest added later, and by construction any `..` or absolute
/// path — is skipped rather than rejected, so a bundle that grows new entries still loads on an
/// older worker.
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
        if !at_root || !entry.header().entry_type().is_file() || schema_id(name).is_none() {
            tracing::debug!(entry = %path.display(), "Ignoring unrecognized schema bundle entry");
            continue;
        }
        // `name` is known to be `<digits>.yaml`, so it can't escape `dest`.
        let name = name.to_owned();

        total = total.saturating_add(entry.header().size()? as usize);
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

/// Parses `<id>.yaml` into its id.
///
/// Exactly one spelling per id: digits only (rejecting `+7.yaml`, which `str::parse` would
/// accept) and no leading zeros (rejecting `007.yaml`). Otherwise one bundle could carry both
/// `7.yaml` and `007.yaml` for id 7, and which one won would come down to directory order.
fn schema_id(file_name: &str) -> Option<u32> {
    let stem = file_name.strip_suffix(".yaml")?;
    if stem.is_empty() || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if stem.starts_with('0') && stem.len() > 1 {
        return None;
    }
    stem.parse().ok()
}

async fn load_dir(dir: Utf8PathBuf) -> anyhow::Result<HashMap<u32, Arc<DatasetDescription>>> {
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

    /// Writes the entry name straight into the header rather than going through
    /// `Builder::append_data`, which refuses to *produce* a `..` path. A hostile bundle is not
    /// built by this crate's tar writer, so the test data can't be either.
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

    fn sha256_of(bytes: &[u8]) -> String {
        format!("sha256:{}", hex_encode(&Sha256::digest(bytes)))
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

    /// The store plus the registry it installs into — assertions go through the registry, since
    /// that is what query execution actually reads.
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
        assert_eq!(parse_sha256(&format!("sha256:{hex}")).unwrap(), hex);
        // An unknown algorithm must fail rather than skip verification.
        assert!(parse_sha256(&format!("md5:{hex}")).is_err());
        assert!(parse_sha256(&hex).is_err(), "bare hex has no algorithm");
        assert!(parse_sha256("sha256:abc").is_err(), "wrong length");
        assert!(parse_sha256(&format!("sha256:{}", "z".repeat(64))).is_err());
    }

    #[test]
    fn schema_ids_come_from_digits_only() {
        assert_eq!(schema_id("7.yaml"), Some(7));
        assert_eq!(schema_id("140000.yaml"), Some(140000));
        // `str::parse` would accept these as 7; treating them as schema 7 would let one bundle
        // define the same id twice, with the winner decided by directory order.
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
        let bundle = SchemaBundle {
            hash: sha256_of(&archive),
            url: serve_once(archive).await,
        };

        store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .unwrap();

        // Installed where the experimental engine looks, reachable both ways.
        assert_eq!(registry.get_by_id(7).unwrap().name, "evm");
        assert_eq!(registry.get("evm").unwrap().name, "evm");
        assert!(registry.get_by_id(8).is_err());
        assert_eq!(
            store.installed_hash().as_deref(),
            Some(bundle.hash.as_str())
        );
    }

    #[tokio::test]
    async fn rejects_a_bundle_whose_hash_does_not_match() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let bundle = SchemaBundle {
            hash: sha256_of(b"something else"),
            url: serve_once(archive).await,
        };

        let err = store
            .ensure(&bundle, &reqwest::Client::new())
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
        let bundle = SchemaBundle {
            hash: sha256_of(&archive),
            url: serve_once(archive).await,
        };

        store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .unwrap();

        assert!(
            registry.get_by_id(7).is_ok(),
            "the root-level <id>.yaml is loaded"
        );
        assert!(
            registry.get_by_id(9).is_err(),
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
        let hash = sha256_of(&archive);
        let url = serve_once(archive).await;

        store(&dir)
            .0
            .ensure(
                &SchemaBundle {
                    hash: hash.clone(),
                    url: url.clone(),
                },
                &reqwest::Client::new(),
            )
            .await
            .unwrap();

        // The one-shot server is spent, so a second fetch could only succeed from disk.
        let (restarted, registry) = store(&dir);
        restarted
            .ensure(&SchemaBundle { hash, url }, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(registry.get_by_id(7).unwrap().name, "evm");
    }

    /// A hash-named directory is normally taken as proof the content is good, which is what
    /// makes restarts cheap. If it has been damaged since (a prune that failed partway), that
    /// shortcut would otherwise suppress the very download that fixes it — and because a bundle
    /// that never installs blocks every assignment, the worker would wedge with no way back.
    #[tokio::test]
    async fn recovers_from_an_unusable_unpacked_bundle() {
        let dir = tempfile::tempdir().unwrap();
        let (store, registry) = store(&dir);
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let hash = sha256_of(&archive);

        // Leave behind an empty directory under the hash this bundle will resolve to.
        let hex = hash.strip_prefix("sha256:").unwrap();
        let damaged = dir.path().join(format!("{UNPACKED_PREFIX}{hex}"));
        std::fs::create_dir_all(&damaged).unwrap();

        let bundle = SchemaBundle {
            hash,
            url: serve_once(archive).await,
        };
        store
            .ensure(&bundle, &reqwest::Client::new())
            .await
            .expect("the damaged copy is discarded and the bundle fetched again");

        assert_eq!(registry.get_by_id(7).unwrap().name, "evm");
        assert_eq!(
            store.installed_hash().as_deref(),
            Some(bundle.hash.as_str())
        );
    }

    /// Nothing else removes a staging directory a crashed unpack left behind: `prune` only runs
    /// after a successful install, which may never come (legacy mode never installs a bundle).
    ///
    /// An unpacked bundle must survive though — it is what makes a restart cheap.
    #[test]
    fn construction_sweeps_staging_directories_but_keeps_unpacked_bundles() {
        let dir = tempfile::tempdir().unwrap();
        let hex = "a".repeat(64);
        let unpacked = format!("{UNPACKED_PREFIX}{hex}");
        std::fs::create_dir_all(dir.path().join(format!("{TEMP_PREFIX}{unpacked}"))).unwrap();
        std::fs::create_dir_all(dir.path().join(&unpacked)).unwrap();
        // Anything the store didn't write is left alone.
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
            let bundle = SchemaBundle {
                hash: sha256_of(&archive),
                url: serve_once(archive).await,
            };
            store
                .ensure(&bundle, &reqwest::Client::new())
                .await
                .unwrap();
        }

        assert_eq!(
            registry.get_by_id(7).unwrap().name,
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
