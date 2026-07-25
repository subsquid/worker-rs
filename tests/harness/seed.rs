//! HC-12 — deterministic seeding; the seed is reported on failure.
//!
//! One root seed per run (`SQD_CONFORMANCE_SEED`, else a fixed default) fans out into
//! independent labelled streams, so adding a stub does not shift another stub's draws.

use std::sync::atomic::{AtomicBool, Ordering};

use crypto_box::aead::rand_core::{CryptoRng, Error, RngCore};

const DEFAULT_SEED: u64 = 0x5011_C0FF_EE00_0001;

/// Root seed for one conformance run.
#[derive(Clone, Copy, Debug)]
pub struct Seed(u64);

impl Seed {
    /// Reads `SQD_CONFORMANCE_SEED` (decimal or `0x`-prefixed), else the fixed default.
    pub fn from_env() -> Self {
        let root = std::env::var("SQD_CONFORMANCE_SEED")
            .ok()
            .and_then(|s| {
                let s = s.trim();
                match s.strip_prefix("0x") {
                    Some(hex) => u64::from_str_radix(hex, 16).ok(),
                    None => s.parse().ok(),
                }
            })
            .unwrap_or(DEFAULT_SEED);
        Self(root)
    }

    pub fn value(self) -> u64 {
        self.0
    }

    /// An independent stream for one component. Labels keep streams stable as the
    /// harness grows: a new `stream("x")` doesn't perturb any existing one.
    pub fn stream(self, label: &str) -> SplitMix64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in label.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        SplitMix64::new(self.0 ^ h)
    }
}

/// Prints the run seed if the current thread is unwinding, so a failure is reproducible.
/// Hold one for the lifetime of a test.
pub struct SeedReporter {
    seed: Seed,
    armed: AtomicBool,
}

impl SeedReporter {
    pub fn new(seed: Seed) -> Self {
        Self {
            seed,
            armed: AtomicBool::new(true),
        }
    }

    /// Suppresses the report — for tests that fail deliberately.
    pub fn disarm(&self) {
        self.armed.store(false, Ordering::Relaxed);
    }
}

impl Drop for SeedReporter {
    fn drop(&mut self) {
        if std::thread::panicking() && self.armed.load(Ordering::Relaxed) {
            eprintln!(
                "\n  reproduce with: SQD_CONFORMANCE_SEED=0x{:x}\n",
                self.seed.value()
            );
        }
    }
}

/// SplitMix64, implementing the `rand_core` 0.6 traits `sqd-assignments`' builder needs and
/// the workspace's `rand` 0.9 doesn't. `CryptoRng` is a compatibility claim, not a security
/// one — it only seeds stub-side key material.
#[derive(Clone, Debug)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }
}

impl RngCore for SplitMix64 {
    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for chunk in dest.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}

impl CryptoRng for SplitMix64 {}
