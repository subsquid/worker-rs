use itertools::Itertools;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{info, instrument, warn};

use crate::{
    metrics,
    types::state::{ChunkId, ChunkRef, ChunkSet, DatasetId},
};

#[derive(Debug, Default)]
pub struct State {
    available: ChunkSet,
    downloading: ChunkSet, // available and downloading don't intersect
    desired: ChunkSet,
    to_download: ChunkSet, // to_download is always equal to desired.diff(available).diff(downloading)
    locks: HashMap<ChunkRef, u8>, // stores ref count for each chunk
    // assigned chunks whose download addresses don't resolve (FM-11)
    unresolvable: ChunkSet,
    deletion_floor: DeletionFloor,
    deletion_hold: Option<DeletionHold>,
}

/// P-DEL-FLOOR / P-DEL-HOLD-MAX (REQ-25, ADR-17): the share of the stored chunks one
/// assignment may evict, and how long a larger batch waits before it goes through anyway.
///
/// The batch is held whole rather than trimmed to fit — trimming still wipes the store, just
/// over several passes. The wait separates the two cases by their own nature: a scheduler
/// glitch is corrected by the next publication, a real shrink keeps being republished.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeletionFloor {
    fraction: f64,
    hold_max: Duration,
}

/// What the floor is doing about one eviction batch. The window is evidence that the network
/// means this batch, so it belongs to the batch: a different one earns its own.
#[derive(Debug)]
enum DeletionHold {
    /// Withheld since `since`; the same batch has to stand for the whole window.
    Holding { batch: ChunkSet, since: Instant },
    /// The window lapsed. Members still pinned then evict as their pins release, rather than
    /// facing a fresh window each time one does.
    Authorized { batch: ChunkSet },
}

impl Default for DeletionFloor {
    fn default() -> Self {
        Self::new(0.5, Duration::from_secs(3600))
    }
}

impl DeletionFloor {
    /// `fraction >= 1` disables the gate: ADR-17's operator override. Clamped to `0..=1`.
    pub fn new(fraction: f64, hold_max: Duration) -> Self {
        Self {
            fraction: if fraction.is_nan() {
                0.0
            } else {
                fraction.clamp(0.0, 1.0)
            },
            hold_max,
        }
    }

    fn permits(&self, evicting: usize, stored: usize) -> bool {
        self.fraction >= 1.0 || evicting as f64 <= self.fraction * stored as f64
    }
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
    pub fn new(available: ChunkSet, deletion_floor: DeletionFloor) -> Self {
        Self {
            available: available.clone(),
            desired: available,
            deletion_floor,
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
        let chunk_ref = {
            // TODO: use priority queue if it's slow
            let (_dataset, chunks) = self
                .to_download
                .iter()
                .into_group_map_by(|chunk| chunk.dataset.clone())
                .into_iter()
                .min_by_key(|(_ds, chunks)| chunks.len())?;
            (*chunks.first()?).clone()
        };
        self.to_download.remove(&chunk_ref);
        self.downloading.insert(chunk_ref.clone());
        Some(chunk_ref)
    }

    pub fn take_removals(&mut self, now: Instant) -> Vec<ChunkRef> {
        // Everything the assignment dropped, pinned or not. A pin lasts one query, so counting
        // only what is evictable right now would let a wipe through in instalments as pins
        // drain — the floor bounds the document's blast radius, not one pass's.
        let batch: ChunkSet = self
            .available
            .iter()
            .filter(|chunk| !self.desired.contains(*chunk))
            .cloned()
            .collect();

        if self
            .deletion_floor
            .permits(batch.len(), self.available.len())
        {
            if self.deletion_hold.take().is_some() {
                info!("Deletion hold released: the batch is back under the floor");
            }
        } else if !self.authorize(batch, now) {
            return Vec::new();
        }

        let mut result = Vec::new();
        self.available.retain(|chunk| {
            if self.desired.contains(chunk) || self.locks.contains_key(chunk) {
                true
            } else {
                result.push(chunk.clone());
                false
            }
        });
        result
    }

    /// Whether an over-the-floor `batch` may go through this pass (REQ-25).
    fn authorize(&mut self, batch: ChunkSet, now: Instant) -> bool {
        match self.deletion_hold.take() {
            // Already earned, and nothing new joined: let the remainder through.
            Some(DeletionHold::Authorized { batch: earned }) if batch.is_subset(&earned) => {
                self.deletion_hold = Some(DeletionHold::Authorized { batch: earned });
                true
            }
            Some(DeletionHold::Holding { batch: held, since }) if held == batch => {
                let held_for = now.duration_since(since);
                if held_for < self.deletion_floor.hold_max {
                    self.deletion_hold = Some(DeletionHold::Holding { batch: held, since });
                    return false;
                }
                // Stood the whole window, so it is the network's intent, not a glitch.
                warn!(
                    "Deletion hold stood for {held_for:?}; evicting {} chunks",
                    batch.len()
                );
                self.deletion_hold = Some(DeletionHold::Authorized { batch });
                true
            }
            _ => {
                warn!(
                    "Holding eviction of {} of {} chunks: one assignment may not delete more \
                     than the deletion floor (REQ-25). It goes through in {:?} unless an \
                     assignment brings the batch back under the floor.",
                    batch.len(),
                    self.available.len(),
                    self.deletion_floor.hold_max
                );
                self.deletion_hold = Some(DeletionHold::Holding { batch, since: now });
                false
            }
        }
    }

    /// How many evictions the P-DEL-FLOOR gate is currently withholding, if any.
    pub fn deletion_hold(&self) -> Option<usize> {
        match &self.deletion_hold {
            Some(DeletionHold::Holding { batch, .. }) => Some(batch.len()),
            _ => None,
        }
    }

    /// How long until the hold lapses. LIV-4 wants the eviction without a further input
    /// event, so the reconciliation loop wakes itself on this.
    pub fn deletion_hold_remaining(&self, now: Instant) -> Option<Duration> {
        match &self.deletion_hold {
            Some(DeletionHold::Holding { since, .. }) => Some(
                self.deletion_floor
                    .hold_max
                    .saturating_sub(now.duration_since(*since)),
            ),
            _ => None,
        }
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
            self.available.insert(chunk);
        } else if self.desired.contains(&chunk) {
            self.to_download.insert(chunk);
        }
    }

    /// A chunk whose download address didn't resolve: back to pending for a later retry, and
    /// recorded so the alarm can name the condition (FM-11).
    pub fn defer_unresolvable(&mut self, chunk: ChunkRef) {
        self.complete_download(&chunk, false);
        self.unresolvable.insert(chunk);
    }

    pub fn unresolvable_chunks(&self) -> usize {
        self.unresolvable.len()
    }

    /// Forgets which addresses didn't resolve, reporting whether there were any. The verdicts
    /// belong to the document that produced them.
    pub fn clear_unresolvable(&mut self) -> bool {
        !std::mem::take(&mut self.unresolvable).is_empty()
    }

    #[cfg(any(feature = "mvcc-chunks", test))]
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

    pub fn unlock_chunk(&mut self, chunk: &ChunkRef) {
        let remove = self
            .locks
            .get_mut(chunk)
            .map(|count| {
                *count -= 1;
                *count == 0
            })
            .unwrap_or(false);
        if remove {
            self.locks.remove(chunk);
        }
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
            "Chunks available: {}, downloading: {}, pending downloads: {}",
            self.available.len(),
            self.downloading.len(),
            self.to_download.len()
        );
        metrics::CHUNKS_AVAILABLE.set(self.available.len() as i64);
        metrics::CHUNKS_DOWNLOADING.set(self.downloading.len() as i64);
        metrics::CHUNKS_PENDING.set(self.to_download.len() as i64);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use std::time::{Duration, Instant};

    use crate::types::state::ChunkRef;

    use super::{DeletionFloor, State};

    /// Four chunks named in sort order, so a slice of the source vec is the expected batch.
    fn store(n: usize) -> (Vec<ChunkRef>, Arc<String>) {
        let ds = Arc::new("ds".to_owned());
        let chunks = (0..n)
            .map(|x| ChunkRef {
                dataset: ds.clone(),
                chunk: Arc::from(format!("000000000{x}")),
            })
            .collect();
        (chunks, ds)
    }

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

        let now = Instant::now();
        let mut state = State::new(
            [a.clone(), b.clone()].into_iter().collect(),
            DeletionFloor::default(),
        );
        state.set_desired_chunks([a.clone(), b.clone(), c.clone()].into_iter().collect());
        assert_eq!(state.take_next_download(), Some(c.clone()));
        assert_eq!(state.take_next_download(), None);

        state.set_desired_chunks([b.clone(), d.clone()].into_iter().collect());
        assert_eq!(state.get_stale_downloads(), &[c.clone()]);
        assert_eq!(state.take_removals(now), &[a.clone()]);
        assert_eq!(state.take_removals(now), &[]);
        assert_eq!(state.get_stale_downloads(), &[c.clone()]);

        assert_eq!(state.take_next_download(), Some(d.clone()));
        assert_eq!(state.take_next_download(), None);
        state.complete_download(&d, true);
        state.complete_download(&c, false);

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

        let now = Instant::now();
        let mut state = State::new(
            [a.clone(), b.clone()].into_iter().collect(),
            DeletionFloor::default(),
        );
        assert!(state.is_fully_applied());

        state.set_desired_chunks([a.clone(), b.clone(), c.clone()].into_iter().collect());
        assert!(!state.is_fully_applied());

        assert_eq!(state.take_next_download(), Some(c.clone()));
        assert!(!state.is_fully_applied());

        state.complete_download(&c, true);
        assert!(state.is_fully_applied());

        state.set_desired_chunks([b.clone(), c.clone()].into_iter().collect());
        assert!(state.is_fully_applied());

        assert_eq!(state.take_removals(now), &[a]);
        assert!(state.is_fully_applied());
    }

    /// REQ-25: an assignment that drops everything must leave the store intact, and a
    /// restoring one must resume normal eviction.
    #[test]
    fn deletion_floor_holds_a_wipe_and_releases_on_recovery() {
        let (chunks, _ds) = store(4);
        let now = Instant::now();
        let mut state = State::new(chunks.iter().cloned().collect(), DeletionFloor::default());

        // An empty slice would evict all four.
        state.set_desired_chunks(Default::default());
        assert_eq!(state.take_removals(now), &[]);
        assert_eq!(state.deletion_hold(), Some(4));

        // The hold is re-evaluated, not latched: it survives another pass...
        assert_eq!(state.take_removals(now), &[]);
        assert_eq!(state.deletion_hold(), Some(4));

        // ...and lifts as soon as an assignment brings the batch back under the floor.
        state.set_desired_chunks(chunks[..2].iter().cloned().collect());
        assert_eq!(state.take_removals(now), &chunks[2..]);
        assert_eq!(state.deletion_hold(), None);
        assert_eq!(state.status().available.len(), 2);
    }

    /// The hold is a delay, not a veto: a shrink the network keeps republishing is its
    /// intent, and obeying it late is what keeps RS-3 and LIV-4 bounded.
    #[test]
    fn deletion_hold_lapses_and_lets_a_persistent_shrink_through() {
        let (chunks, _ds) = store(4);
        let window = Duration::from_secs(3600);
        let t0 = Instant::now();
        let mut state = State::new(
            chunks.iter().cloned().collect(),
            DeletionFloor::new(0.5, window),
        );

        state.set_desired_chunks(Default::default());
        assert_eq!(state.take_removals(t0), &[]);
        assert_eq!(state.deletion_hold_remaining(t0), Some(window));

        // Still held one second short of the window — and the clock runs from the first
        // hold, not from the last pass.
        let almost = t0 + window - Duration::from_secs(1);
        assert_eq!(state.take_removals(almost), &[]);
        assert_eq!(
            state.deletion_hold_remaining(almost),
            Some(Duration::from_secs(1))
        );

        assert_eq!(state.take_removals(t0 + window), chunks);
        assert_eq!(state.deletion_hold(), None);
        assert_eq!(state.deletion_hold_remaining(t0 + window), None);
    }

    /// The window is evidence about one batch, so a different wipe cannot inherit it — else a
    /// second bad document arriving near expiry executes with no delay at all.
    #[test]
    fn a_different_batch_earns_its_own_window() {
        let (chunks, _ds) = store(4);
        let window = Duration::from_secs(3600);
        let t0 = Instant::now();
        let mut state = State::new(
            chunks.iter().cloned().collect(),
            DeletionFloor::new(0.5, window),
        );

        state.set_desired_chunks(Default::default());
        assert_eq!(state.take_removals(t0), &[]);

        // A second wipe-inducing assignment, one second before the first would have lapsed.
        let late = t0 + window - Duration::from_secs(1);
        state.set_desired_chunks(chunks[..1].iter().cloned().collect());
        assert_eq!(state.take_removals(late), &[]);
        assert_eq!(
            state.take_removals(t0 + window),
            &[],
            "the new batch must not inherit the old one's window"
        );
        assert_eq!(state.take_removals(late + window), &chunks[1..]);
    }

    /// A pinned member of an authorized batch was already covered by that batch's window.
    /// Making it wait another one would double LIV-4's bound.
    #[test]
    fn an_authorized_batch_does_not_earn_its_window_twice() {
        let (chunks, _ds) = store(4);
        let window = Duration::from_secs(3600);
        let t0 = Instant::now();
        let mut state = State::new(
            chunks.iter().cloned().collect(),
            DeletionFloor::new(0.5, window),
        );
        state.get_and_lock_chunk(chunks[3].dataset.clone(), chunks[3].chunk.clone());

        state.set_desired_chunks(Default::default());
        assert_eq!(state.take_removals(t0), &[]);
        assert_eq!(state.take_removals(t0 + window), &chunks[..3]);

        state.unlock_chunk(&chunks[3]);
        assert_eq!(state.take_removals(t0 + window), &chunks[3..]);
    }

    /// A pin must not buy a wipe an instalment plan: pinned chunks count toward the batch, or
    /// half the store goes now and the rest as each pin drops — 75 % of it under a 50 % floor.
    #[test]
    fn a_pin_does_not_let_a_wipe_past_the_floor() {
        let (chunks, _ds) = store(4);
        let now = Instant::now();
        let mut state = State::new(chunks.iter().cloned().collect(), DeletionFloor::default());
        for chunk in &chunks[..2] {
            state.get_and_lock_chunk(chunk.dataset.clone(), chunk.chunk.clone());
        }

        state.set_desired_chunks(Default::default());
        assert_eq!(state.take_removals(now), &[]);
        assert_eq!(state.deletion_hold(), Some(4));

        // Releasing a pin changes what is evictable, not what the assignment dropped.
        state.unlock_chunk(&chunks[0]);
        assert_eq!(state.take_removals(now), &[]);
        assert_eq!(state.deletion_hold(), Some(4));
        assert_eq!(state.status().available.len(), 4);
    }

    /// The pin still decides *when* a permitted removal runs (RS-2: pin > assignment).
    #[test]
    fn a_pin_defers_a_removal_the_floor_permits() {
        let (chunks, _ds) = store(4);
        let now = Instant::now();
        let mut state = State::new(chunks.iter().cloned().collect(), DeletionFloor::default());
        state.get_and_lock_chunk(chunks[3].dataset.clone(), chunks[3].chunk.clone());

        // One of four dropped is well under the floor, but it is pinned.
        state.set_desired_chunks(chunks[..3].iter().cloned().collect());
        assert_eq!(state.take_removals(now), &[]);
        assert_eq!(state.deletion_hold(), None, "permitted, merely deferred");

        state.unlock_chunk(&chunks[3]);
        assert_eq!(state.take_removals(now), &chunks[3..]);
    }

    /// The operator override (a floor of 1) turns the gate off entirely.
    #[test]
    fn deletion_floor_of_one_permits_a_full_wipe() {
        let (chunks, _ds) = store(4);
        let mut state = State::new(
            chunks.iter().cloned().collect(),
            DeletionFloor::new(1.0, Duration::from_secs(3600)),
        );
        state.set_desired_chunks(Default::default());
        assert_eq!(state.take_removals(Instant::now()).len(), 4);
        assert_eq!(state.deletion_hold(), None);
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

        let mut state = State::new(
            [a.clone(), b.clone()].into_iter().collect(),
            DeletionFloor::default(),
        );
        state.set_desired_chunks([a.clone(), b.clone(), c.clone()].into_iter().collect());

        assert_eq!(state.take_next_download(), Some(c.clone()));
        assert!(!state.is_fully_applied());

        state.set_desired_chunks([a, b].into_iter().collect());
        assert!(state.get_stale_downloads().contains(&c));
        assert!(state.is_fully_applied());
    }
}
