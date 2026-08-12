/// Identifies a write schema: the layout version a chunk's parquet files were written with.
///
/// Assignments reference schemas by this id, bundles are keyed on it, and query execution
/// resolves it — three paths where a bare `u32` sits next to block numbers, chunk indexes and
/// table counts.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SchemaId(u32);

impl SchemaId {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// The wire value, for the flatbuffer and bundle-filename boundaries that speak `u32`.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for SchemaId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
