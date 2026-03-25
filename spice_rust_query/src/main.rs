use anyhow::{anyhow, Context, Result};
use arrow::compute::concat_batches;
use arrow::util::pretty::pretty_format_batches;
use futures::StreamExt;
use spice_rust_query::connect_spice;

const DEFAULT_QUERY: &str = "SELECT trip_distance, total_amount FROM taxi_trips ORDER BY trip_distance DESC LIMIT 10;";

#[tokio::main]
async fn main() -> Result<()> {
    let query = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_QUERY.to_string());

    let client = connect_spice()
        .await
        .map_err(|error| anyhow!("failed to connect to Spice runtime: {error}"))?;

    let data = client
        .query(&query)
        .await
        .context("failed to run Spice query")?;

    let mut stream = data;
    let mut batches = Vec::new();

    while let Some(batch) = stream.next().await {
        batches.push(batch.map_err(|error| anyhow!("failed to read Spice result stream: {error}"))?);
    }

    println!("query: {query}");
    if batches.is_empty() {
        println!("result: <empty>");
        return Ok(());
    }

    let combined = concat_batches(&batches[0].schema(), &batches)
        .map_err(|error| anyhow!("failed to combine result batches: {error}"))?;
    let formatted = pretty_format_batches(&[combined])
        .map_err(|error| anyhow!("failed to format query result: {error}"))?;

    println!("{formatted}");

    Ok(())
}
