use base64::{engine::general_purpose::URL_SAFE_NO_PAD as base64, Engine};

pub type Dataset = String;

pub fn encode_dataset(dataset: &str) -> String {
    base64.encode(dataset.as_bytes())
}

pub fn decode_dataset(str: &str) -> Option<Dataset> {
    base64
        .decode(str)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
}
