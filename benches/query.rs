use anyhow::Result;
use camino::Utf8PathBuf as PathBuf;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

use crate::query::eth::BatchRequest;
use subsquid_worker::query;
#[cfg(test)]
use subsquid_worker::util::tests::tests_data;

async fn prepare_context() -> Result<SessionContext> {
    let root = tests_data().join("0017881390/0017881390-0017882786-32ee9457");
    query::context::prepare_query_context(&root).await
}

fn get_query(name: &str) -> Result<BatchRequest> {
    let query_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.json"));
    serde_json::from_reader(std::fs::File::open(&query_path)?).map_err(From::from)
}

pub fn query_processing(c: &mut Criterion) {
    let ctx = futures::executor::block_on(prepare_context()).unwrap();
    for name in [
        "empty_filter",
        "include_all_blocks",
        "load_all_tx_and_logs",
        "all_logs_and_logs+tx_regression",
        "logs_from_transaction_and_request",
        "topics_filtering",
        "sighash_filtering",
    ] {
        let query: BatchRequest = get_query(name).unwrap();
        c.bench_function(name, |b| {
            b.to_async(Runtime::new().unwrap()).iter(|| {
                let ctx = ctx.clone();
                let query = query.clone();
                async move {
                    query::processor::process_query(&ctx, query).await.unwrap();
                }
            })
        });
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = query_processing
);
criterion_main!(benches);
