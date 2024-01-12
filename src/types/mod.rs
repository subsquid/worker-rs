use std::collections::HashMap;

pub type Dataset = String;

// Inclusive range
pub struct Range {
    begin: u64,
    end: u64,
}

pub type RangeSet = Vec<Range>;

pub type State = HashMap<Dataset, RangeSet>;
