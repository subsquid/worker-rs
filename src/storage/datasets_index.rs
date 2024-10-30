use std::{collections::HashMap, str::FromStr, sync::Arc};
use base64::{engine::general_purpose::STANDARD as base64, Engine};

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
    chunks_ordinals_map: HashMap<DataChunk, u64>,
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

// Reuses memory building both ChunkSet and DatasetsIndex simultaneously
pub fn parse_assignment(assignment: WorkerAssignment) -> anyhow::Result<(ChunkSet, DatasetsIndex)> {
    let mut datasets = HashMap::new();
    let mut chunk_set = ChunkSet::new();
    let mut chunks_ordinals_map = HashMap::new();
    let mut ordinal = 0;

    let filename_refs: HashMap<u32, Arc<str>> = assignment
        .known_filenames
        .into_iter()
        .enumerate()
        .map(|(i, filename)| (i as u32, Arc::from(filename.as_str())))
        .collect();

    for DatasetChunks {
        dataset_id,
        download_url,
        chunks,
        ..
    } in assignment.dataset_chunks
    {
        let dataset_id: Arc<Dataset> = Arc::from(dataset_id);
        let mut dataset_files = HashMap::new();
        for chunk in chunks {
            let data_chunk = DataChunk::from_str(&chunk.path)
                .map_err(|e| anyhow::anyhow!("Invalid chunk path: {e}"))?;
            let files = chunk
                .filenames
                .iter()
                .map(|index| filename_refs.get(index).unwrap().clone())
                .collect();
            chunk_set.insert(ChunkRef {
                dataset: dataset_id.clone(),
                chunk: data_chunk.clone(),
            });
            dataset_files.insert(data_chunk.clone(), files);
            chunks_ordinals_map.insert(data_chunk, ordinal);
            ordinal += 1;
        }
        datasets.insert(
            dataset_id,
            DatasetIndex {
                url: Url::parse(&download_url)?,
                files: dataset_files,
            },
        );
    }
    Ok((
        chunk_set,
        DatasetsIndex {
            datasets,
            chunks_ordinals_map,
            http_headers: assignment
                .http_headers
                .into_iter()
                .map(|header| {
                    (
                        reqwest::header::HeaderName::from_str(&header.name).unwrap_or_else(|e| {
                            panic!("Couldn't parse header name: {}: {e:?}", header.name)
                        }),
                        reqwest::header::HeaderValue::from_str(&header.value).unwrap_or_else(|e| {
                            panic!("Couldn't parse header value: {}: {e:?}", header.value)
                        }),
                    )
                })
                .collect(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use reqwest::Url;
    use sqd_messages::{AssignedChunk, HttpHeader};

    use super::{parse_assignment, Arc, ChunkRef, ChunkSet, DatasetChunks, WorkerAssignment};
    use crate::storage::layout::DataChunk;

    #[test]
    fn test_assignment_to_chunk_set() {
        let dataset_1 = "s3://ethereum-mainnet".to_string();
        let dataset_2 = "s3://moonbeam-evm".to_string();
        let assignment = WorkerAssignment {
            dataset_chunks: vec![
                DatasetChunks {
                    dataset_id: dataset_1.clone(),
                    chunks: vec![
                        AssignedChunk {
                            path: "0000000000/0000000000-0000697499-f6275b81".to_string(),
                            filenames: vec![0, 1, 2],
                        },
                        AssignedChunk {
                            path: "0000000000/0000697500-0000962739-4eff9837".to_string(),
                            filenames: vec![0, 1, 2],
                        },
                        AssignedChunk {
                            path: "0007293060/0007300860-0007308199-6aeb1a56".to_string(),
                            filenames: vec![0, 1, 2],
                        },
                    ],
                    download_url: "https://example.com".to_string(),
                    ..Default::default()
                },
                DatasetChunks {
                    dataset_id: dataset_2.clone(),
                    chunks: vec![
                        AssignedChunk {
                            path: "0000000000/0000190280-0000192679-0999c6ce".to_string(),
                            filenames: vec![0, 1, 2, 3, 4],
                        },
                        AssignedChunk {
                            path: "0003510340/0003515280-0003518379-b8613f00".to_string(),
                            filenames: vec![0, 1, 2, 3, 4],
                        },
                    ],
                    download_url: "https://example.com".to_string(),
                    ..Default::default()
                },
            ],
            http_headers: vec![HttpHeader {
                name: "Auth".to_string(),
                value: "None".to_string(),
            }],
            known_filenames: vec![
                "blocks.parquet".to_string(),
                "transactions.parquet".to_string(),
                "logs.parquet".to_string(),
                "statediffs.parquet".to_string(),
                "traces.parquet".to_string(),
            ],
        };
        let dataset_1 = Arc::new(dataset_1);
        let dataset_2 = Arc::new(dataset_2);
        let exp_chunk_set: ChunkSet = [
            ChunkRef {
                dataset: dataset_1.clone(),
                chunk: DataChunk {
                    last_block: 697499.into(),
                    first_block: 0.into(),
                    last_hash: "f6275b81".into(),
                    top: 0.into(),
                },
            },
            ChunkRef {
                dataset: dataset_1.clone(),
                chunk: DataChunk {
                    last_block: 962739.into(),
                    first_block: 697500.into(),
                    last_hash: "4eff9837".into(),
                    top: 0.into(),
                },
            },
            ChunkRef {
                dataset: dataset_1.clone(),
                chunk: DataChunk {
                    last_block: 7308199.into(),
                    first_block: 7300860.into(),
                    last_hash: "6aeb1a56".into(),
                    top: 7293060.into(),
                },
            },
            ChunkRef {
                dataset: dataset_2.clone(),
                chunk: DataChunk {
                    last_block: 192679.into(),
                    first_block: 190280.into(),
                    last_hash: "0999c6ce".into(),
                    top: 0.into(),
                },
            },
            ChunkRef {
                dataset: dataset_2.clone(),
                chunk: DataChunk {
                    last_block: 3518379.into(),
                    first_block: 3515280.into(),
                    last_hash: "b8613f00".into(),
                    top: 3510340.into(),
                },
            },
        ]
        .into_iter()
        .collect();

        let (chunk_set, ds_index) = parse_assignment(assignment).expect("Valid assignment");
        assert_eq!(chunk_set, exp_chunk_set);
        assert_eq!(ds_index.get_headers()["Auth"], "None");
        assert_eq!(ds_index.list_files(&dataset_1, &chunk_set.first().unwrap().chunk), Some(vec![
            super::RemoteFile {
                url: Url::parse("https://example.com/0000000000/0000000000-0000697499-f6275b81/blocks.parquet").unwrap(),
                name: "blocks.parquet".into(),
            },
            super::RemoteFile {
                url: Url::parse("https://example.com/0000000000/0000000000-0000697499-f6275b81/transactions.parquet").unwrap(),
                name: "transactions.parquet".into(),
            },
            super::RemoteFile {
                url: Url::parse("https://example.com/0000000000/0000000000-0000697499-f6275b81/logs.parquet").unwrap(),
                name: "logs.parquet".into(),
            },
        ]));
    }
}
