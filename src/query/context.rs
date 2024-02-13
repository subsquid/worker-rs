use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ParquetReadOptions;

pub async fn prepare_query_context(
    path: &std::path::Path,
) -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "blocks",
        path.join("blocks.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "transactions",
        path.join("transactions.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "logs",
        path.join("logs.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "traces",
        path.join("traces.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "statediffs",
        path.join("statediffs.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(ctx)
}
