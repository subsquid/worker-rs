use std::{path::PathBuf, str::FromStr, sync::Arc};

use polars::prelude::{LazyFrame, ScanArgsParquet};

use crate::controller::polars_target::{self, DataStore};
pub struct WorkerChunkStore {
    pub path: String,
}

impl DataStore for WorkerChunkStore {
    fn get_data_source(
        &self,
        _dataset: &str,
        table_name: &str,
        _chunks: &[String],
    ) -> polars_target::PolarsTargetResult<LazyFrame> {
        let path = PathBuf::from_str(&format!("{}/{}.parquet", self.path, table_name)).unwrap();

        Ok(LazyFrame::scan_parquet_files(
            Arc::from([path]),
            ScanArgsParquet::default(),
        )?)
    }
}
