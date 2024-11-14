use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};

use reqwest::Url;
use tracing::error;

use crate::types::{
    dataset::Dataset,
    state::{ChunkRef, ChunkSet},
};

use super::layout::DataChunk;

#[derive(Default)]
pub struct DatasetsIndex {
    datasets: HashMap<Arc<Dataset>, DatasetIndex>,
    http_headers: reqwest::header::HeaderMap,
    assignment_id: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub url: Url,
    pub name: String,
}

struct DatasetIndex {
    url: Url,
    files: HashMap<DataChunk, Vec<String>>,
    chunks_ordinals_map: HashMap<DataChunk, u64>,
}

impl DatasetsIndex {
    pub fn list_files(&self, dataset: &Dataset, chunk: &DataChunk) -> Option<Vec<RemoteFile>> {
        let ds = self.datasets.get(dataset)?;
        ds.files.get(chunk).map(|files| {
            files
                .iter()
                .map(|filename| RemoteFile {
                    url: ds
                        .url
                        .join(&format!("{}/{}", chunk.path(), filename))
                        .unwrap_or_else(|_| panic!("Couldn't form URL for {chunk}")),
                    name: filename.clone(),
                })
                .collect()
        })
    }
    pub fn from(
        assigned_data: Vec<sqd_messages::assignments::Dataset>,
        headers: BTreeMap<String, String>,
        assignment_id: String,
    ) -> Self {
        let mut datasets = HashMap::new();
        let mut ordinal = 0;
        for dataset in assigned_data {
            let dataset_id = dataset.id;
            let dataset_url = dataset.base_url;
            let mut dataset_files: HashMap<DataChunk, Vec<String>> = Default::default();
            let mut chunks_ordinals_map = HashMap::new();
            for chunk in dataset.chunks {
                let data_chunk = DataChunk::from_path(&chunk.id).unwrap();
                let mut files: Vec<String> = Default::default();
                // TODO: Introduce structure to hold overriding urls and use them for download
                for (file, _) in chunk.files {
                    files.push(file);
                }
                dataset_files.insert(data_chunk.clone(), files);
                chunks_ordinals_map.insert(data_chunk, ordinal);
                ordinal += 1;
            }
            datasets.insert(
                Arc::from(dataset_id),
                DatasetIndex {
                    url: Url::parse(&dataset_url).unwrap(),
                    files: dataset_files,
                    chunks_ordinals_map,
                },
            );
        }

        let http_headers = headers
            .into_iter()
            .filter_map(|(k, v)| {
                let key = match reqwest::header::HeaderName::from_str(&k) {
                    Ok(key) => key,
                    Err(err) => {
                        error!("Couldn't parse header name: {}: {err:?}", k);
                        return None;
                    }
                };
                let val = match reqwest::header::HeaderValue::from_str(&v) {
                    Ok(val) => val,
                    Err(err) => {
                        error!("Couldn't parse header value: {}: {err:?}", k);
                        return None;
                    }
                };
                Some((key, val))
            })
            .collect();

        DatasetsIndex {
            datasets,
            http_headers,
            assignment_id: Some(assignment_id),
        }
    }

    pub fn create_chunks_set(&self) -> ChunkSet {
        let mut chunk_set = ChunkSet::new();
        for (dataset_id, dataset_index) in &self.datasets {
            for files_by_chunk in dataset_index.files.keys() {
                chunk_set.insert(ChunkRef {
                    dataset: dataset_id.clone(),
                    chunk: files_by_chunk.clone(),
                });
            }
        }
        chunk_set
    }

    pub fn get_headers(&self) -> &reqwest::header::HeaderMap {
        &self.http_headers
    }

    pub fn get_ordinals_len(&self) -> usize {
        self.datasets
            .values()
            .map(|v| v.chunks_ordinals_map.len())
            .sum()
    }

    pub fn get_ordinal(&self, dataset: &Dataset, chunk: &DataChunk) -> Option<u64> {
        self.datasets
            .get(dataset)
            .and_then(|v| v.chunks_ordinals_map.get(chunk).copied())
    }

    pub fn get_assignment_id(&self) -> Option<String> {
        self.assignment_id.clone()
    }
}
