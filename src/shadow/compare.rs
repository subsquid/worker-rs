use serde_json::{Number, Value};

const EXCERPT_CHARS: usize = 300;

/// Where two JSON-lines responses first disagree.
#[derive(Debug, PartialEq)]
pub struct Difference {
    /// Zero-based line, which is one block.
    pub line: usize,
    pub block: Option<u64>,
    /// The JSON path inside the block, empty when a whole block is missing.
    pub path: String,
    pub legacy: String,
    pub new: String,
}

/// The first value two responses disagree on. The engines differ by design in
/// field order, string escaping and float spelling (the new engine's
/// `spec/GAPS.md`), so equal bytes are only a shortcut and unequal lines are
/// compared as values.
pub fn first_difference(legacy: &[u8], new: &[u8]) -> Option<Difference> {
    let legacy_lines: Vec<&[u8]> = lines(legacy).collect();
    let new_lines: Vec<&[u8]> = lines(new).collect();

    for line in 0..legacy_lines.len().max(new_lines.len()) {
        let (l, n) = match (legacy_lines.get(line), new_lines.get(line)) {
            (Some(l), Some(n)) if l == n => continue,
            (Some(l), Some(n)) => (*l, *n),
            (Some(l), None) => {
                let block = parse(l).ok();
                return Some(Difference {
                    line,
                    block: block.as_ref().and_then(block_number),
                    path: String::new(),
                    legacy: excerpt_bytes(l),
                    new: "<no block>".to_string(),
                });
            }
            (None, Some(n)) => {
                let block = parse(n).ok();
                return Some(Difference {
                    line,
                    block: block.as_ref().and_then(block_number),
                    path: String::new(),
                    legacy: "<no block>".to_string(),
                    new: excerpt_bytes(n),
                });
            }
            (None, None) => unreachable!("the loop stays within the longer response"),
        };

        let (legacy_block, new_block) = match (parse(l), parse(n)) {
            (Ok(l), Ok(n)) => (l, n),
            (legacy_block, new_block) => {
                return Some(Difference {
                    line,
                    block: None,
                    path: "<not JSON>".to_string(),
                    legacy: legacy_block.map_or_else(|e| e.to_string(), |_| excerpt_bytes(l)),
                    new: new_block.map_or_else(|e| e.to_string(), |_| excerpt_bytes(n)),
                });
            }
        };

        let mut path = String::new();
        if let Some((legacy_value, new_value)) =
            value_difference(&legacy_block, &new_block, &mut path)
        {
            return Some(Difference {
                line,
                block: block_number(&legacy_block).or_else(|| block_number(&new_block)),
                path,
                legacy: legacy_value,
                new: new_value,
            });
        }
    }

    None
}

pub fn count_lines(response: &[u8]) -> usize {
    lines(response).count()
}

fn lines(response: &[u8]) -> impl Iterator<Item = &[u8]> {
    response
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
}

fn parse(line: &[u8]) -> serde_json::Result<Value> {
    serde_json::from_slice(line)
}

fn block_number(block: &Value) -> Option<u64> {
    block.get("header")?.get("number")?.as_u64()
}

/// Excerpts of the first pair of values that differ, with `path` left pointing
/// at them.
fn value_difference(legacy: &Value, new: &Value, path: &mut String) -> Option<(String, String)> {
    match (legacy, new) {
        (Value::Object(l), Value::Object(n)) => {
            let mut keys: Vec<&String> = l.keys().chain(n.keys()).collect();
            keys.sort();
            keys.dedup();
            for key in keys {
                let len = path.len();
                if !path.is_empty() {
                    path.push('.');
                }
                path.push_str(key);
                match (l.get(key), n.get(key)) {
                    (Some(l), Some(n)) => {
                        if let Some(found) = value_difference(l, n, path) {
                            return Some(found);
                        }
                    }
                    (l, n) => return Some((excerpt_or_absent(l), excerpt_or_absent(n))),
                }
                path.truncate(len);
            }
            None
        }
        (Value::Array(l), Value::Array(n)) => {
            for index in 0..l.len().max(n.len()) {
                let len = path.len();
                path.push_str(&format!("[{index}]"));
                match (l.get(index), n.get(index)) {
                    (Some(l), Some(n)) => {
                        if let Some(found) = value_difference(l, n, path) {
                            return Some(found);
                        }
                    }
                    (l, n) => return Some((excerpt_or_absent(l), excerpt_or_absent(n))),
                }
                path.truncate(len);
            }
            None
        }
        (Value::Number(l), Value::Number(n)) => {
            // `1.0` and `1` are the same number spelled two ways.
            let same = if l.is_f64() || n.is_f64() {
                exact_f64(l).is_some_and(|l| Some(l) == exact_f64(n))
            } else {
                l == n
            };
            (!same).then(|| (excerpt(legacy), excerpt(new)))
        }
        (l, n) => (l != n).then(|| (excerpt(l), excerpt(n))),
    }
}

/// The number as the f64 that holds it exactly, if one does: an integer
/// above 2^53 would round into equality with a different float.
fn exact_f64(number: &Number) -> Option<f64> {
    let float = number.as_f64()?;
    if number.is_f64() {
        return Some(float);
    }

    let integer = number
        .as_u64()
        .map(i128::from)
        .or_else(|| number.as_i64().map(i128::from))?;
    // Back through i128, not u64: `as u64` saturates 2^64 into u64::MAX.
    (float as i128 == integer).then_some(float)
}

fn excerpt_or_absent(value: Option<&Value>) -> String {
    value.map_or_else(|| "<absent>".to_string(), excerpt)
}

fn excerpt(value: &Value) -> String {
    truncate(value.to_string())
}

fn excerpt_bytes(line: &[u8]) -> String {
    truncate(String::from_utf8_lossy(line).into_owned())
}

fn truncate(mut text: String) -> String {
    if let Some((cut, _)) = text.char_indices().nth(EXCERPT_CHARS) {
        text.truncate(cut);
        text.push('…');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_order_escaping_and_float_spelling_are_not_differences() {
        let legacy =
            br#"{"header":{"number":5,"hash":"0x01"},"logs":[{"data":"\u00e9","price":1.634916549089593e11,"n":1.0}]}
"#;
        let new = "{\"header\":{\"hash\":\"0x01\",\"number\":5},\"logs\":[{\"price\":163491654908.9593,\"n\":1,\"data\":\"é\"}]}\n";

        assert_eq!(first_difference(legacy, new.as_bytes()), None);
    }

    #[test]
    fn two_spellings_of_one_float_are_the_same_value() {
        // Best-effort float parsing rounds these two apart.
        let legacy = b"{\"header\":{\"number\":5},\"logs\":[{\"value\":8.322682722556836e15}]}\n";
        let new = b"{\"header\":{\"number\":5},\"logs\":[{\"value\":8322682722556836.0}]}\n";

        assert_eq!(first_difference(legacy, new), None);
    }

    #[test]
    fn an_integer_that_no_float_holds_differs_from_every_float() {
        for (l, n) in [
            ("9007199254740993", "9007199254740992.0"),
            ("9007199254740992.0", "9007199254740993"),
            ("18446744073709551615", "18446744073709551616.0"),
        ] {
            let legacy = format!("{{\"header\":{{\"number\":5}},\"logs\":[{{\"value\":{l}}}]}}\n");
            let new = format!("{{\"header\":{{\"number\":5}},\"logs\":[{{\"value\":{n}}}]}}\n");

            let difference = first_difference(legacy.as_bytes(), new.as_bytes());

            assert_eq!(
                difference.map(|d| d.path),
                Some("logs[0].value".to_string()),
                "{l} vs {n}"
            );
        }
    }

    #[test]
    fn a_differing_value_is_named_by_its_path_and_block() {
        let legacy =
            b"{\"header\":{\"number\":5}}\n{\"header\":{\"number\":6},\"logs\":[{\"topics\":[\"0xa\",\"0xb\"]}]}\n";
        let new =
            b"{\"header\":{\"number\":5}}\n{\"header\":{\"number\":6},\"logs\":[{\"topics\":[\"0xa\",\"0xc\"]}]}\n";

        let difference = first_difference(legacy, new).unwrap();

        assert_eq!(difference.line, 1);
        assert_eq!(difference.block, Some(6));
        assert_eq!(difference.path, "logs[0].topics[1]");
        assert_eq!(
            (difference.legacy.as_str(), difference.new.as_str()),
            ("\"0xb\"", "\"0xc\"")
        );
    }

    #[test]
    fn a_field_one_engine_omits_is_a_difference() {
        let legacy = b"{\"header\":{\"number\":5,\"size\":null}}\n";
        let new = b"{\"header\":{\"number\":5}}\n";

        let difference = first_difference(legacy, new).unwrap();

        assert_eq!(difference.path, "header.size");
        assert_eq!(difference.new, "<absent>");
    }

    #[test]
    fn a_missing_block_is_a_difference() {
        let legacy = b"{\"header\":{\"number\":5}}\n{\"header\":{\"number\":6}}\n";
        let new = b"{\"header\":{\"number\":5}}\n";

        let difference = first_difference(legacy, new).unwrap();

        assert_eq!((difference.line, difference.block), (1, Some(6)));
        assert_eq!(difference.new, "<no block>");
    }
}
