use tracing::error;

use super::layout::DataChunk;
use crate::types::dataset::Dataset;
use std::collections::{BTreeMap, HashMap};

pub type OrdinalMap = HashMap<DataChunk, u64>;

#[derive(Default, Clone)]
pub struct Ordinals {
    datasets: HashMap<Dataset, OrdinalMap>,
    assignment_id: String,
}

impl Ordinals {
    pub fn new(
        assigned_data: Vec<sqd_messages::assignments::Dataset>,
        assignment_id: String,
    ) -> Self {
        let mut datasets = HashMap::new();
        let mut ordinal = 0;
        for dataset in assigned_data {
            let dataset_id = dataset.id;
            let mut chunks_ordinals_map = HashMap::new();
            for chunk in dataset.chunks {
                let data_chunk = DataChunk::from_path(&chunk.id).unwrap();
                chunks_ordinals_map.insert(data_chunk, ordinal);
                ordinal += 1;
            }
            datasets.insert(dataset_id, chunks_ordinals_map);
        }

        Ordinals {
            datasets,
            assignment_id,
        }
    }

    pub fn get_ordinals_len(&self) -> usize {
        self.datasets.values().map(|v| v.len()).sum()
    }

    pub fn get_ordinal(&self, dataset: &Dataset, chunk: &DataChunk) -> Option<u64> {
        self.datasets
            .get(dataset)
            .and_then(|v| v.get(chunk).copied())
    }

    pub fn get_assignment_id(&self) -> String {
        self.assignment_id.clone()
    }
}

#[derive(Default)]
pub struct OrdinalsHolder {
    pub ordinals: BTreeMap<u64, Ordinals>,
}

impl OrdinalsHolder {
    pub fn populate_with_ordinals(&mut self, ordinals: Ordinals, timestamp: u64) {
        error!("New ordinals for id: {:?}@{timestamp}", ordinals.assignment_id);
        self.ordinals.insert(timestamp, ordinals);
    }

    pub fn populate_with_data(
        &mut self,
        assigned_data: Vec<sqd_messages::assignments::Dataset>,
        timestamp: u64,
        assignment_id: String,
    ) {
        let ordinals = Ordinals::new(assigned_data, assignment_id);
        self.populate_with_ordinals(ordinals, timestamp);
    }

    pub fn get_active_ordinals(&mut self) -> Option<Ordinals> {
        let system_time = std::time::SystemTime::now();
        let unix_time = system_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let t = self.get_ordinals_by_time(unix_time);
        match t.clone() {
            Some(a) => error!("Got ordinal {:?}", a.get_assignment_id()),
            None => error!("No ordinals at this time"),
        }
        t
    }

    pub fn get_ordinals_by_time(&mut self, timestamp: u64) -> Option<Ordinals> {
        if !self.ordinals.is_empty() {
            // We BTreeMap keys are sorted so we get sorted list of effective_from times for each allocations
            let keys = self.ordinals.keys().cloned().collect::<Vec<_>>();
            // We want state in which only first allocation may be active, so we pop till _second_ effective_from is greater than current time.
            // If first effective_from is less than current time, than first allocation is active, otherwise no allocation is active.
            for &from_time in keys.iter().skip(1) {
                if from_time < timestamp {
                    let t = self.ordinals.pop_first().unwrap();
                    error!("Dropping ordinal: {:?} ({:?} against {:?})", t.1.get_assignment_id(), t.0, timestamp);
                } else {
                    break;
                }
            }
        };
        self.ordinals
            .first_key_value()
            .filter(|(&ts, _)| ts < timestamp)
            .map(|(_, ordinals)| ordinals.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_populate_with() {
        let mut holder = OrdinalsHolder::default();
        let assigned_data = vec![sqd_messages::assignments::Dataset {
            id: "dataset1".to_owned(),
            base_url: "http://sqd.dev".to_owned(),
            chunks: vec![],
        }];
        holder.populate_with_data(assigned_data, 42, "1".to_owned());
        assert!(holder.ordinals.contains_key(&42));
    }

    #[test]
    fn test_get_ordinals_by_time() {
        let mut holder = OrdinalsHolder::default();
        let assigned_data = vec![sqd_messages::assignments::Dataset {
            id: "dataset1".to_owned(),
            base_url: "http://sqd.dev".to_owned(),
            chunks: vec![],
        }];
        holder.populate_with_data(assigned_data, 100, "1".to_owned());

        // Test that get_ordinals_by_time returns None for times before first
        let ordinals = holder.get_ordinals_by_time(42);
        assert!(ordinals.is_none());

        // Test that get_ordinals_by_time returns something for times after first
        let ordinals = holder.get_ordinals_by_time(142);
        assert!(ordinals.is_some());

        let assigned_data = vec![sqd_messages::assignments::Dataset {
            id: "dataset2".to_owned(),
            base_url: "http://sqd.dev".to_owned(),
            chunks: vec![],
        }];
        holder.populate_with_data(assigned_data, 200, "2".to_owned());

        // Test that get_ordinals_by_time returns first allocation if time in between
        let ordinals = holder.get_ordinals_by_time(142);
        assert!(ordinals.is_some());
        let ordinals = ordinals.unwrap();
        assert_eq!(ordinals.assignment_id, "1".to_owned());

        let assigned_data = vec![sqd_messages::assignments::Dataset {
            id: "dataset3".to_owned(),
            base_url: "http://sqd.dev".to_owned(),
            chunks: vec![],
        }];
        holder.populate_with_data(assigned_data, 300, "3".to_owned());

        // Test that get_ordinals_by_time returns first allocation if time after last
        let ordinals = holder.get_ordinals_by_time(342);
        assert!(ordinals.is_some());
        let ordinals = ordinals.unwrap();
        assert_eq!(ordinals.assignment_id, "3".to_owned());
    }
}
