use std::collections::HashMap;

pub type Dataset = String;

// Inclusive range
pub struct Range {
    begin: u32,
    end: u32,
}

pub type RangeSet = Vec<Range>;

pub type State = HashMap<Dataset, RangeSet>;
