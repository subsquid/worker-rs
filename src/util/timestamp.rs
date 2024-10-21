#[inline(always)]
pub fn timestamp_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Invalid current time")
        .as_millis() as u64
}