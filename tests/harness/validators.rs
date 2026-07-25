//! HC-5 — structural validators (spec/13 §structural validators).
//!
//! Kind-agnostic: these hold for every response regardless of what was asked, so a test
//! that forgets to assert something still catches a violation. Checks Phase 0 can't do are
//! listed in `MISSING` rather than stubbed.

use sqd_messages::{query_result, Compression, Query, QueryLogs, QueryResult, WorkerStatus};
use sqd_network_transport::PeerId;

/// Validators not yet implementable, so nobody reads their absence as a pass.
pub const MISSING: &[&str] = &[
    "INV-1 gauge/set-algebra agreement (needs HC-6 observability scraper)",
    "INV-5 log-page gap-freeness across a restart (needs HC-7)",
];

/// Accumulated contract violations. Empty means the response is structurally sound.
#[derive(Debug, Default)]
pub struct Violations(Vec<String>);

impl Violations {
    fn check(&mut self, ok: bool, property: &str, detail: impl FnOnce() -> String) {
        if !ok {
            self.0.push(format!("{property}: {}", detail()));
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[track_caller]
    pub fn assert_none(&self, context: &str) {
        assert!(
            self.0.is_empty(),
            "structural violations in {context}:\n  - {}",
            self.0.join("\n  - ")
        );
    }
}

/// Validates a query response against the request that produced it.
///
/// `worker_id` is the responding worker, needed for INV-25's signature check.
pub fn query_response(response: &QueryResult, request: &Query, worker_id: PeerId) -> Violations {
    let mut v = Violations::default();

    v.check(response.query_id == request.query_id, "IB-11", || {
        format!(
            "response query_id {:?} != request {:?}",
            response.query_id, request.query_id
        )
    });
    v.check(response.verify_signature(worker_id), "INV-25", || {
        "response signature does not verify against the worker identity".to_owned()
    });

    let Some(result) = &response.result else {
        v.check(false, "IB-11", || {
            "response carries no result oneof".to_owned()
        });
        return v;
    };

    match result {
        query_result::Result::Ok(ok) => {
            // A missing range is itself the violation — `execute` owes a `bad_request` for it.
            let Some(range) = request.block_range else {
                v.check(false, "RP-10", || {
                    "success for a query with no block_range".to_owned()
                });
                return v;
            };
            v.check(ok.last_block <= range.end, "RP-11", || {
                format!("last_block {} > range.end {}", ok.last_block, range.end)
            });

            let data = match decompress(&ok.data, request.compression()) {
                Ok(data) => data,
                Err(e) => {
                    v.check(false, "RP-14", || {
                        format!("payload does not round-trip: {e}")
                    });
                    return v;
                }
            };

            // Arrow IPC ordering needs a decoder the harness doesn't carry yet.
            if request.output_format() == sqd_messages::OutputFormat::Jsonl {
                jsonl_body(&mut v, &data, range.begin, ok.last_block, range.end);
            }
        }
        query_result::Result::Err(err) => {
            v.check(err.err.is_some(), "RP-20", || {
                "error response carries no class".to_owned()
            });
            // INV-20 holds by construction: the error variant has no data field.
        }
    }

    v
}

/// RP-12 emission order and RP-11 block membership over a JSONL body.
fn jsonl_body(v: &mut Violations, data: &[u8], begin: u64, last_block: u64, end: u64) {
    let Ok(text) = std::str::from_utf8(data) else {
        v.check(false, "IB-12", || "JSONL body is not UTF-8".to_owned());
        return;
    };

    let mut previous: Option<u64> = None;
    let mut count = 0usize;
    for (line_no, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        count += 1;
        let parsed: serde_json::Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(e) => {
                v.check(false, "IB-12", || {
                    format!("line {line_no} is not JSON: {e}")
                });
                return;
            }
        };
        let Some(number) = parsed
            .get("header")
            .and_then(|h| h.get("number"))
            .and_then(serde_json::Value::as_u64)
        else {
            v.check(false, "IB-12", || {
                format!("line {line_no} has no header.number")
            });
            continue;
        };

        v.check(number >= begin && number <= last_block, "RP-11", || {
            format!("block {number} outside covered range [{begin}, {last_block}]")
        });
        if let Some(prev) = previous {
            v.check(number > prev, "RP-12", || {
                format!("block {number} follows {prev} — not ascending")
            });
        }
        previous = Some(number);
    }

    // RP-11 boundary emission. Only `last_block < end` is structurally decidable: the result
    // claims early stop, so it evaluated something and owes boundary records. At
    // `last_block == end` an empty body is legal (disjoint range) — separating that from a
    // missing boundary record needs HC-4 (GAP-32).
    v.check(count > 0 || last_block >= end, "RP-11", || {
        format!(
            "result stopped early at {last_block} (range end {end}) \
             yet emitted no records from [{begin}, {last_block}]"
        )
    });
}

/// RP-22 / INV-5: one log page against the cursor that requested it.
///
/// `cursor` is the ⟨timestamp_ms, query_id⟩ the client resumed from; `now_ms` and `lag_ms`
/// bound how recent a served record may be.
pub fn logs_page(
    page: &QueryLogs,
    cursor: Option<(u64, &str)>,
    now_ms: u64,
    lag_ms: u64,
    max_bytes: usize,
) -> Violations {
    use sqd_messages::ProstMsg;

    let mut v = Violations::default();
    v.check(page.encoded_len() <= max_bytes, "RP-22", || {
        format!("page is {} bytes, budget {max_bytes}", page.encoded_len())
    });

    let mut previous: Option<(u64, String)> = None;
    for record in &page.queries_executed {
        let Some(query) = &record.query else {
            v.check(false, "IB-21", || {
                "log record carries no embedded query".to_owned()
            });
            continue;
        };
        let key = (record.timestamp_ms, query.query_id.clone());

        if let Some((cursor_ts, cursor_id)) = cursor {
            v.check(key > (cursor_ts, cursor_id.to_owned()), "RP-22", || {
                format!("record {key:?} is not strictly after cursor ({cursor_ts}, {cursor_id})")
            });
        }
        v.check(record.timestamp_ms + lag_ms <= now_ms, "RP-22", || {
            format!(
                "record at {} is newer than the {lag_ms} ms serving lag (now {now_ms})",
                record.timestamp_ms
            )
        });
        if let Some(prev) = &previous {
            v.check(&key > prev, "RP-22", || {
                format!("record {key:?} out of cursor order after {prev:?}")
            });
        }
        v.check(record.result.is_some(), "INV-32", || {
            format!("log record {} has no outcome", query.query_id)
        });
        previous = Some(key);
    }

    v
}

/// INV-30 / IB-22: the status report is internally coherent.
///
/// `assigned_chunks` is the size of this worker's assignment slice — the DEF-13 map must
/// have exactly one bit per assigned chunk.
pub fn status(report: &WorkerStatus, assigned_chunks: usize) -> Violations {
    let mut v = Violations::default();

    let Some(map) = &report.missing_chunks else {
        v.check(false, "IB-22", || {
            "status carries no availability map".to_owned()
        });
        return v;
    };

    match map.to_vec() {
        Ok(bits) => {
            v.check(bits.len() == assigned_chunks, "INV-30", || {
                format!(
                    "availability map has {} bits, assignment slice has {assigned_chunks}",
                    bits.len()
                )
            });
            let ones = bits.iter().filter(|b| **b).count() as u64;
            v.check(map.ones() == ones, "INV-30", || {
                format!("declared ones-count {} != actual {ones}", map.ones())
            });
        }
        Err(e) => v.check(false, "IB-22", || {
            format!("availability map does not decode: {e}")
        }),
    }

    v.check(!report.version.is_empty(), "IB-22", || {
        "status carries no version".to_owned()
    });

    v
}

fn decompress(data: &[u8], compression: Compression) -> anyhow::Result<Vec<u8>> {
    use std::io::Read;
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Gzip => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(data).read_to_end(&mut out)?;
            Ok(out)
        }
        Compression::Zstd => Ok(zstd::decode_all(data)?),
    }
}
