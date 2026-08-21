/// Identifies the layout version used to write a chunk's Parquet files.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SchemaId(u32);

impl SchemaId {
    pub const fn new(id: u32) -> Self {
        Self(id)
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
