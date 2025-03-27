use std::io::Write;

use sqd_query::{ParquetChunk, Query};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        anyhow::bail!("Usage: {} <query_path> <chunk_path>", args[0]);
    }

    let query_path = &args[1];
    let chunk_path = &args[2];

    let query_str = std::fs::read_to_string(query_path).expect("Failed to read the query file");
    let query = Query::from_json_bytes(query_str.as_bytes())?;
    let plan = query.compile();
    let chunk = ParquetChunk::new(chunk_path);
    let data = Vec::with_capacity(1024 * 1024);
    let mut writer = sqd_query::JsonLinesWriter::new(data);
    let blocks = plan.execute(&chunk);
    if let Some(mut blocks) = blocks? {
        writer.write_blocks(&mut blocks)?;
    }
    let bytes = writer.finish()?;
    std::io::stdout().write_all(&bytes)?;
    Ok(())
}
