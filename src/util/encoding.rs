/// How a fetched blob is compressed. Nothing in the network state names it — an assignment
/// pointer's `version` names the format, not the encoding — so the url's suffix is the signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Gzip,
    Zstd,
}

impl Encoding {
    /// Anything not explicitly zstd is gzip — what the network published before zstd.
    pub fn of(url: &str) -> Self {
        // A presigned url carries a query and sometimes a fragment; neither names the encoding.
        let path = url.split(['?', '#']).next().unwrap_or(url);
        match path.rsplit_once('.') {
            Some((_, "zst" | "zstd")) => Self::Zstd,
            _ => Self::Gzip,
        }
    }

    pub fn magic(self) -> &'static [u8] {
        match self {
            Self::Gzip => &[0x1f, 0x8b],
            Self::Zstd => &[0x28, 0xb5, 0x2f, 0xfd],
        }
    }
}

impl std::fmt::Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_zstd_suffix_on_the_path_names_zstd() {
        let cases = [
            ("https://example.test/a.fb.gz", Encoding::Gzip),
            ("https://example.test/a.fb.zst", Encoding::Zstd),
            ("https://example.test/a.fb.zstd", Encoding::Zstd),
            ("https://example.test/b.tar.zst", Encoding::Zstd),
            (
                "https://example.test/a.fb.zst?X-Amz-Signature=deadbeef",
                Encoding::Zstd,
            ),
            ("https://example.test/a.fb.gz?token=1", Encoding::Gzip),
            ("https://example.test/a.fb.zst#part", Encoding::Zstd),
            // A suffix elsewhere in the path is not the encoding.
            ("https://example.test/zst/a.fb", Encoding::Gzip),
            ("https://example.test/a.zst.gz", Encoding::Gzip),
            ("https://example.test/a", Encoding::Gzip),
        ];
        for (url, want) in cases {
            assert_eq!(Encoding::of(url), want, "{url}");
        }
    }
}
