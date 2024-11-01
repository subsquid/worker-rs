use std::{collections::HashMap, str::FromStr, sync::Arc};

use reqwest::Url;
use sqd_messages::{DatasetChunks, WorkerAssignment};

use crate::types::{
    dataset::Dataset,
    state::{ChunkRef, ChunkSet},
};

use super::layout::DataChunk;

#[derive(Default)]
pub struct DatasetsIndex {
    datasets: HashMap<Arc<Dataset>, DatasetIndex>,
    pub chunks_ordinals_map: HashMap<DataChunk, u64>,
    http_headers: reqwest::header::HeaderMap,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub url: Url,
    pub name: Arc<str>,
}

struct DatasetIndex {
    url: Url,
    files: HashMap<DataChunk, Vec<Arc<str>>>,
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
    pub fn from(assigned_data: Vec<crate::util::assignment::Dataset>, headers: HashMap<String, String>) -> Self {
        let mut datasets = HashMap::new();
        let mut chunks_ordinals_map = HashMap::new();
        let mut ordinal = 0;
        for dataset in assigned_data {
            let dataset_id = dataset.id;
            let dataset_url = dataset.base_url;
            let mut dataset_files: HashMap<DataChunk, Vec<Arc<str>>> = Default::default();
            for chunk in dataset.chunks {
                let data_chunk = DataChunk::from_path(&chunk.id).unwrap();
                let mut files: Vec<Arc<str>> = Default::default();
                for (file, _) in chunk.files {
                    files.push(Arc::from(file.as_str()));
                }
                dataset_files.insert(data_chunk, files);
                chunks_ordinals_map.insert(DataChunk::from_path(&chunk.id).unwrap(), ordinal);
                ordinal += 1;
            }
            datasets.insert(
                Arc::from(dataset_id),
                DatasetIndex {
                    url: Url::parse(&dataset_url).unwrap(),
                    files: dataset_files,
                },
            );
        }
        DatasetsIndex { 
            datasets,
            chunks_ordinals_map,
            http_headers: headers.into_iter().map(|(k, v)| {
                (
                    reqwest::header::HeaderName::from_str(&k).unwrap_or_else(|e| {
                        panic!("Couldn't parse header name: {}: {e:?}", k)
                    }),
                    reqwest::header::HeaderValue::from_str(&v).unwrap_or_else(|e| {
                        panic!("Couldn't parse header value: {}: {e:?}", v)
                    }),
                )
            }).collect() 
        }
    }

    pub fn create_chunks_set(&self) -> ChunkSet {
        let mut chunk_set = ChunkSet::new();
        for (dataset_id, dataset_index) in &self.datasets {
            for data_chunk in dataset_index.files.keys() {
                chunk_set.insert(ChunkRef {
                    dataset: dataset_id.clone(),
                    chunk: data_chunk.clone(),
                });
            };
        };
        chunk_set
    }

    pub fn get_headers(&self) -> &reqwest::header::HeaderMap {
        &self.http_headers
    }
}