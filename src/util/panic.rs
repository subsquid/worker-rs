/// The message a caught panic carried, for the typed error replacing it (ADR-5, ADR-21).
/// By value: `&Box<dyn Any>` unsizes to the box, not its contents, so every downcast misses.
pub fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

#[cfg(test)]
mod tests {
    use super::panic_message;

    #[test]
    fn reads_both_payload_shapes_and_neither() {
        let caught = |f: fn()| match std::panic::catch_unwind(f) {
            Ok(()) => unreachable!("the closure panics"),
            Err(payload) => panic_message(payload),
        };
        assert_eq!(caught(|| panic!("static")), "static");
        assert_eq!(caught(|| panic!("{}", "owned".to_string())), "owned");
        assert_eq!(caught(|| std::panic::panic_any(7u8)), "unknown panic");
    }
}
