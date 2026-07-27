use itertools::Itertools;
use std::collections::HashMap;
use tracing::{info, instrument, warn};

use crate::{
    metrics,
    types::state::{ChunkId, ChunkRef, ChunkSet, DatasetId},
};

/// How many times a chunk download may fail before the worker gives up on it
/// until the next assignment. Attempts are spaced by the downloader's global
/// backoff, so the budget is not burned in a tight loop.
pub const MAX_DOWNLOAD_ATTEMPTS: u32 = 5;

#[derive(Debug, Default)]
pub struct State {
    available: ChunkSet,
    downloading: ChunkSet, // available and downloading don't intersect
    desired: ChunkSet,
    to_download: ChunkSet, // always equal to desired.diff(available).diff(downloading).diff(failed_downloads)
    locks: HashMap<ChunkRef, u8>, // stores ref count for each chunk
    // Undesired chunks still query-locked when the removal pass ran, keyed by
    // remaining lock count: invisible to new queries and downloads, kept on
    // disk until the last query finishes.
    condemned: HashMap<ChunkRef, u8>,
    // Both reset on every new assignment — each assignment gets a fresh download budget
    download_attempts: HashMap<ChunkRef, u32>, // failed attempts per desired chunk
    failed_downloads: ChunkSet, // desired chunks that exhausted their download attempts
}

#[derive(Debug)]
pub enum UpdateStatus {
    Unchanged,
    Updated,
}

pub struct Status {
    pub available: ChunkSet,
    pub desired: ChunkSet,
}

impl State {
    pub fn new(available: ChunkSet) -> Self {
        Self {
            available: available.clone(),
            desired: available,
            ..Default::default()
        }
    }

    #[instrument(skip_all)]
    pub fn set_desired_chunks(&mut self, desired: ChunkSet) -> UpdateStatus {
        let status = if self.desired == desired {
            UpdateStatus::Unchanged
        } else {
            UpdateStatus::Updated
        };

        self.desired = desired;
        // Fresh download budget: a new assignment may carry fixed URLs, so
        // previously given-up chunks are retried.
        self.download_attempts.clear();
        self.failed_downloads.clear();
        self.to_download = self
            .desired
            .iter()
            .filter(|chunk| !self.available.contains(chunk) && !self.downloading.contains(chunk))
            .cloned()
            .collect();

        status
    }

    // make desired = available + downloading
    pub fn _stop_downloads(&mut self) -> UpdateStatus {
        if self.to_download.is_empty() {
            return UpdateStatus::Unchanged;
        };
        self.desired
            .retain(|chunk| !self.to_download.contains(chunk));
        self.to_download.clear();
        UpdateStatus::Updated
    }

    pub fn take_next_download(&mut self) -> Option<ChunkRef> {
        // Deletion before download: while any undesired chunk is still available
        // or its in-flight download hasn't been reaped yet, don't start new
        // downloads. Otherwise old and new data would coexist and the worker
        // could exceed its storage commitment.
        if self.has_pending_removals() {
            return None;
        }
        let chunk_ref = {
            // TODO: use priority queue if it's slow
            let (_dataset, chunks) = self
                .to_download
                .iter()
                // A re-desired condemned chunk can't start downloading yet: its
                // stale copy still occupies the destination path until the last
                // query holding it finishes.
                .filter(|chunk| !self.condemned.contains_key(*chunk))
                .into_group_map_by(|chunk| chunk.dataset.clone())
                .into_iter()
                .min_by_key(|(_ds, chunks)| chunks.len())?;
            (*chunks.first()?).clone()
        };
        self.to_download.remove(&chunk_ref);
        self.downloading.insert(chunk_ref.clone());
        Some(chunk_ref)
    }

    // Undesired chunks that are still available or still being downloaded.
    // While any exist, new downloads are held back.
    pub fn has_pending_removals(&self) -> bool {
        self.available.difference(&self.desired).next().is_some()
            || self.downloading.difference(&self.desired).next().is_some()
    }

    /// Undesired chunks ready for physical deletion. Locked undesired chunks
    /// are condemned instead and handed out here once their last query
    /// finishes.
    pub fn take_removals(&mut self) -> Vec<ChunkRef> {
        let mut result = Vec::new();
        self.available.retain(|chunk| {
            if self.desired.contains(chunk) {
                return true;
            }
            if let Some(lock_count) = self.locks.remove(chunk) {
                self.condemned.insert(chunk.clone(), lock_count);
            } else {
                result.push(chunk.clone());
            }
            false
        });
        // Condemned chunks whose last query has finished (see `unlock_chunk`)
        self.condemned.retain(|chunk, lock_count| {
            if *lock_count == 0 {
                result.push(chunk.clone());
                false
            } else {
                true
            }
        });
        result
    }

    // Only works as a hint to speed up things.
    // Cancelled downloads still have to be reported with a `complete_download` call
    pub fn get_stale_downloads(&self) -> Vec<ChunkRef> {
        self.downloading
            .difference(&self.desired)
            .cloned()
            .collect()
    }

    pub fn complete_download(&mut self, chunk: &ChunkRef, success: bool) {
        let chunk = self
            .downloading
            .take(chunk)
            .unwrap_or_else(|| panic!("Completing download of unknown chunk: {chunk}"));
        if success {
            self.download_attempts.remove(&chunk);
            self.failed_downloads.remove(&chunk);
            self.available.insert(chunk);
        } else if self.desired.contains(&chunk) {
            let attempts = self.download_attempts.entry(chunk.clone()).or_insert(0);
            *attempts += 1;
            if *attempts >= MAX_DOWNLOAD_ATTEMPTS {
                // The chunk keeps being reported as missing; once no download
                // work is left, the assignment counts as stalled.
                warn!("Giving up on chunk {chunk} after {MAX_DOWNLOAD_ATTEMPTS} download attempts");
                self.download_attempts.remove(&chunk);
                self.failed_downloads.insert(chunk);
            } else {
                self.to_download.insert(chunk);
            }
        } else {
            self.download_attempts.remove(&chunk);
        }
    }

    pub fn is_fully_applied(&self) -> bool {
        // A stale in-flight download for a chunk no longer desired does not block
        // applying the current assignment. It will either complete as extra
        // available data or be ignored when cancellation is reported.
        self.to_download.is_empty()
            && self
                .desired
                .iter()
                .all(|chunk| self.available.contains(chunk))
    }

    /// No download work is left, but some desired chunks were given up on after
    /// exhausting their attempts — this assignment can never become fully
    /// applied. Terminal until the next assignment resets the budget.
    pub fn is_stalled(&self) -> bool {
        !self.failed_downloads.is_empty()
            && self.to_download.is_empty()
            && self.downloading.iter().all(|c| !self.desired.contains(c))
    }

    pub fn get_and_lock_chunk(&mut self, dataset: DatasetId, chunk: ChunkId) -> Option<ChunkRef> {
        let chunk_ref = self.available.get(&ChunkRef { dataset, chunk }).cloned();

        if let Some(chunk_ref) = chunk_ref.as_ref() {
            self.lock_chunk(chunk_ref);
        }

        chunk_ref
    }

    #[instrument(skip_all)]
    pub fn status(&self) -> Status {
        Status {
            available: self.available.clone(),
            desired: self.desired.clone(),
        }
    }

    /// Returns `true` if this was the last lock on a chunk that awaits removal
    /// (undesired and still available, or condemned), so the caller can wake
    /// the state loop to process the deletion promptly.
    pub fn unlock_chunk(&mut self, chunk: &ChunkRef) -> bool {
        if let Some(count) = self.locks.get_mut(chunk) {
            *count -= 1;
            if *count > 0 {
                return false;
            }
            self.locks.remove(chunk);
            return self.available.contains(chunk) && !self.desired.contains(chunk);
        }
        if let Some(count) = self.condemned.get_mut(chunk) {
            *count -= 1;
            // The zero-count entry stays for `take_removals` to extract
            return *count == 0;
        }
        false
    }

    fn lock_chunk(&mut self, chunk: &ChunkRef) {
        assert!(
            self.available.contains(chunk),
            "Trying to lock unknown chunk: {chunk}"
        );
        *self.locks.entry(chunk.clone()).or_insert(0) += 1;
    }

    pub fn report_status(&self) {
        info!(
            "Chunks available: {}, downloading: {}, pending downloads: {}, given up: {}, condemned: {}",
            self.available.len(),
            self.downloading.len(),
            self.to_download.len(),
            self.failed_downloads.len(),
            self.condemned.len()
        );
        metrics::CHUNKS_AVAILABLE.set(self.available.len() as i64);
        metrics::CHUNKS_DOWNLOADING.set(self.downloading.len() as i64);
        metrics::CHUNKS_PENDING.set(self.to_download.len() as i64);
    }
}

/// Test-only introspection for the property-based tests in
/// [`super::state_pbt`], kept out of the production API surface.
#[cfg(test)]
impl State {
    /// Structural invariants that must hold after every operation.
    pub(super) fn assert_invariants(&self) {
        assert!(
            self.available.is_disjoint(&self.downloading),
            "available and downloading must not intersect"
        );
        let expected_to_download: ChunkSet = self
            .desired
            .iter()
            .filter(|chunk| {
                !self.available.contains(*chunk)
                    && !self.downloading.contains(*chunk)
                    && !self.failed_downloads.contains(*chunk)
            })
            .cloned()
            .collect();
        assert_eq!(
            self.to_download, expected_to_download,
            "to_download must equal desired − available − downloading − failed_downloads"
        );
        assert!(
            self.failed_downloads
                .iter()
                .all(|chunk| self.desired.contains(chunk) && !self.available.contains(chunk)),
            "given-up chunks must be desired and missing"
        );
        assert!(
            self.locks
                .keys()
                .all(|chunk| self.available.contains(chunk)),
            "locks must only be held on available chunks"
        );
        assert!(
            self.condemned.keys().all(|chunk| {
                !self.available.contains(chunk)
                    && !self.downloading.contains(chunk)
                    && !self.locks.contains_key(chunk)
            }),
            "condemned chunks must not be available, downloading, or freshly locked"
        );
    }

    pub(super) fn available(&self) -> &ChunkSet {
        &self.available
    }

    pub(super) fn downloading(&self) -> &ChunkSet {
        &self.downloading
    }

    pub(super) fn desired(&self) -> &ChunkSet {
        &self.desired
    }

    pub(super) fn has_queued_downloads(&self) -> bool {
        !self.to_download.is_empty()
    }

    pub(super) fn queued_downloads(&self) -> &ChunkSet {
        &self.to_download
    }

    pub(super) fn is_condemned(&self, chunk: &ChunkRef) -> bool {
        self.condemned.contains_key(chunk)
    }

    pub(super) fn has_condemned(&self) -> bool {
        !self.condemned.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::types::state::{ChunkRef, ChunkSet};

    use super::{State, MAX_DOWNLOAD_ATTEMPTS};

    #[test]
    fn test_state() {
        let ds = Arc::new("ds".to_owned());
        let chunk_ref = |x| ChunkRef {
            dataset: ds.clone(),
            chunk: Arc::from(format!(
                "0000000000/000000000{}-000000000{}-00000000",
                x,
                x + 1
            )),
        };
        let a = chunk_ref(0);
        let b = chunk_ref(1);
        let c = chunk_ref(2);
        let d = chunk_ref(3);

        let mut state = State::new([a.clone(), b.clone()].into_iter().collect());
        state.set_desired_chunks([a.clone(), b.clone(), c.clone()].into_iter().collect());
        assert_eq!(state.take_next_download(), Some(c.clone()));
        assert_eq!(state.take_next_download(), None);

        state.set_desired_chunks([b.clone(), d.clone()].into_iter().collect());
        assert_eq!(state.get_stale_downloads(), &[c.clone()]);
        // No new downloads until `a` is removed and the stale download `c` is reaped
        assert_eq!(state.take_next_download(), None);
        assert_eq!(state.take_removals(), &[a.clone()]);
        assert_eq!(state.take_removals(), &[]);
        assert_eq!(state.get_stale_downloads(), &[c.clone()]);
        assert_eq!(state.take_next_download(), None);
        state.complete_download(&c, false);

        assert_eq!(state.take_next_download(), Some(d.clone()));
        assert_eq!(state.take_next_download(), None);
        state.complete_download(&d, true);

        assert_eq!(
            state.status().available.into_iter().collect_vec(),
            &[b.clone(), d.clone()]
        );
        assert_eq!(
            state.status().desired.into_iter().collect_vec(),
            &[b.clone(), d.clone()]
        );
        assert!(state.is_fully_applied());
    }

    #[test]
    fn fully_applied_requires_desired_chunks_to_be_available() {
        let ds = Arc::new("ds".to_owned());
        let chunk_ref = |x| ChunkRef {
            dataset: ds.clone(),
            chunk: Arc::from(format!(
                "0000000000/000000000{}-000000000{}-00000000",
                x,
                x + 1
            )),
        };
        let a = chunk_ref(0);
        let b = chunk_ref(1);
        let c = chunk_ref(2);

        let mut state = State::new([a.clone(), b.clone()].into_iter().collect());
        assert!(state.is_fully_applied());

        state.set_desired_chunks([a.clone(), b.clone(), c.clone()].into_iter().collect());
        assert!(!state.is_fully_applied());

        assert_eq!(state.take_next_download(), Some(c.clone()));
        assert!(!state.is_fully_applied());

        state.complete_download(&c, true);
        assert!(state.is_fully_applied());

        state.set_desired_chunks([b.clone(), c.clone()].into_iter().collect());
        assert!(state.is_fully_applied());

        assert_eq!(state.take_removals(), &[a]);
        assert!(state.is_fully_applied());
    }

    #[test]
    fn locked_undesired_chunk_is_condemned_without_blocking_downloads() {
        let ds = Arc::new("ds".to_owned());
        let chunk_ref = |x| ChunkRef {
            dataset: ds.clone(),
            chunk: Arc::from(format!(
                "0000000000/000000000{}-000000000{}-00000000",
                x,
                x + 1
            )),
        };
        let a = chunk_ref(0);
        let b = chunk_ref(1);
        let c = chunk_ref(2);

        let mut state = State::new([a.clone(), b.clone()].into_iter().collect());
        // A query is using `a` when the new assignment drops it and adds `c`
        assert!(state
            .get_and_lock_chunk(ds.clone(), a.chunk.clone())
            .is_some());
        state.set_desired_chunks([b.clone(), c.clone()].into_iter().collect());

        // Until the removal pass runs, downloads are gated as usual
        assert!(state.has_pending_removals());
        assert_eq!(state.take_next_download(), None);

        // The removal pass condemns the locked chunk: nothing to delete yet,
        // but downloads proceed immediately
        assert_eq!(state.take_removals(), &[]);
        assert!(!state.has_pending_removals());
        assert_eq!(state.take_next_download(), Some(c));

        // The condemned chunk refuses new query locks
        assert!(state
            .get_and_lock_chunk(ds.clone(), a.chunk.clone())
            .is_none());

        // Releasing the last lock makes `a` physically deletable
        assert!(state.unlock_chunk(&a));
        assert_eq!(state.take_removals(), &[a]);
    }

    // A condemned chunk that the next assignment wants again is not resurrected:
    // its stale copy still occupies the destination path, so the re-download
    // waits until the last query releases it and the copy is deleted.
    #[test]
    fn redesired_condemned_chunk_is_redownloaded_after_deletion() {
        let ds = Arc::new("ds".to_owned());
        let a = ChunkRef {
            dataset: ds.clone(),
            chunk: Arc::from("0000000000/0000000000-0000000001-00000000"),
        };

        let mut state = State::new([a.clone()].into_iter().collect());
        assert!(state
            .get_and_lock_chunk(ds.clone(), a.chunk.clone())
            .is_some());
        state.set_desired_chunks(ChunkSet::new());
        assert_eq!(state.take_removals(), &[]); // `a` is condemned

        // The next assignment wants `a` again — it is queued for download, but
        // can't start while the stale copy is still on disk
        state.set_desired_chunks([a.clone()].into_iter().collect());
        assert!(state.has_queued_downloads());
        assert_eq!(state.take_next_download(), None);
        assert!(!state.is_fully_applied());

        // Once the query finishes and the stale copy is deleted, the download
        // proceeds
        assert!(state.unlock_chunk(&a));
        assert_eq!(state.take_removals(), &[a.clone()]);
        assert_eq!(state.take_next_download(), Some(a.clone()));
        state.complete_download(&a, true);
        assert!(state.is_fully_applied());
    }

    #[test]
    fn unlock_signals_only_the_last_lock_on_an_undesired_chunk() {
        let ds = Arc::new("ds".to_owned());
        let a = ChunkRef {
            dataset: ds.clone(),
            chunk: Arc::from("0000000000/0000000000-0000000001-00000000"),
        };

        let mut state = State::new([a.clone()].into_iter().collect());
        assert!(state
            .get_and_lock_chunk(ds.clone(), a.chunk.clone())
            .is_some());
        // Unlocking a still-desired chunk doesn't require a removal pass
        assert!(!state.unlock_chunk(&a));

        assert!(state
            .get_and_lock_chunk(ds.clone(), a.chunk.clone())
            .is_some());
        assert!(state.get_and_lock_chunk(ds, a.chunk.clone()).is_some());
        state.set_desired_chunks(ChunkSet::new());
        assert!(!state.unlock_chunk(&a), "one lock is still held");
        assert!(state.unlock_chunk(&a), "the last lock was released");
    }

    // A permanently failing download — e.g. a chunk deleted from the bucket —
    // is given up on after MAX_DOWNLOAD_ATTEMPTS and the assignment becomes
    // stalled. The budget is per assignment: the next one retries the chunk.
    #[test]
    fn failing_download_is_given_up_after_attempt_cap() {
        let ds = Arc::new("ds".to_owned());
        let a = ChunkRef {
            dataset: ds,
            chunk: Arc::from("0000000000/0000000000-0000000001-00000000"),
        };

        let mut state = State::new(ChunkSet::new());
        state.set_desired_chunks([a.clone()].into_iter().collect());

        for _ in 0..MAX_DOWNLOAD_ATTEMPTS {
            assert_eq!(state.take_next_download(), Some(a.clone()));
            assert!(!state.is_stalled(), "still work in progress");
            state.complete_download(&a, false);
        }

        // Given up: no more retries; not applied, but stalled
        assert_eq!(state.take_next_download(), None);
        assert!(!state.is_fully_applied());
        assert!(state.is_stalled());

        // A new assignment resets the budget and retries the chunk
        state.set_desired_chunks([a.clone()].into_iter().collect());
        assert!(!state.is_stalled());
        assert_eq!(state.take_next_download(), Some(a.clone()));

        // A success clears the failure bookkeeping entirely
        state.complete_download(&a, true);
        assert!(state.is_fully_applied());
        assert!(!state.is_stalled());
    }

    // After the removal pass condemns an undesired chunk, new locks are
    // refused, so a steady stream of queries can't defer its removal forever.
    #[test]
    fn condemnation_stops_undesired_chunk_relocking() {
        let ds = Arc::new("ds".to_owned());
        let chunk_ref = |x| ChunkRef {
            dataset: ds.clone(),
            chunk: Arc::from(format!(
                "0000000000/000000000{}-000000000{}-00000000",
                x,
                x + 1
            )),
        };
        let a = chunk_ref(0);
        let b = chunk_ref(1);

        let mut state = State::new([a.clone()].into_iter().collect());
        // The new assignment drops `a` and adds `b`
        state.set_desired_chunks([b.clone()].into_iter().collect());

        // A query arriving *after* the assignment switch can still lock `a` —
        // the removal pass hasn't run yet
        assert!(state
            .get_and_lock_chunk(ds.clone(), a.chunk.clone())
            .is_some());

        // The removal pass condemns `a`: new locks are refused from here on,
        // and downloads are not held back
        assert_eq!(state.take_removals(), &[]);
        assert!(state.get_and_lock_chunk(ds, a.chunk.clone()).is_none());
        assert_eq!(state.take_next_download(), Some(b));

        // The last unlock releases `a` for deletion; it stays unlockable
        assert!(state.unlock_chunk(&a));
        assert_eq!(state.take_removals(), &[a]);
    }

    #[test]
    fn fully_applied_ignores_stale_downloads() {
        let ds = Arc::new("ds".to_owned());
        let chunk_ref = |x| ChunkRef {
            dataset: ds.clone(),
            chunk: Arc::from(format!(
                "0000000000/000000000{}-000000000{}-00000000",
                x,
                x + 1
            )),
        };
        let a = chunk_ref(0);
        let b = chunk_ref(1);
        let c = chunk_ref(2);

        let mut state = State::new([a.clone(), b.clone()].into_iter().collect());
        state.set_desired_chunks([a.clone(), b.clone(), c.clone()].into_iter().collect());

        assert_eq!(state.take_next_download(), Some(c.clone()));
        assert!(!state.is_fully_applied());

        state.set_desired_chunks([a, b].into_iter().collect());
        assert!(state.get_stale_downloads().contains(&c));
        assert!(state.is_fully_applied());
    }
}
