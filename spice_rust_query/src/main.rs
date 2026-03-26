use anyhow::{Context, Result, anyhow};
use arrow::compute::concat_batches;
use arrow::util::pretty::pretty_format_batches;
use futures::StreamExt;
use spice_rust_query::connect_spice;

const SINGLE_TABLE_EXAMPLE: &str =
    "SELECT attacker_ip, city_name FROM pg_attacker_ip_perf LIMIT 5;";
const JOIN_QUERY_EXAMPLE: &str = "SELECT a.attacker_ip, c.nation FROM pg_attacker_ip_perf a JOIN pg_city_perf c ON a.city_id = c.id LIMIT 5;";
const DEFAULT_QUERY: &str = SINGLE_TABLE_EXAMPLE;

fn print_examples() {
    println!("可直接执行的示例 SQL:");
    println!("1. 单表查询\n   {SINGLE_TABLE_EXAMPLE}");
    println!("2. 表连接查询\n   {JOIN_QUERY_EXAMPLE}");
}

#[tokio::main]
async fn main() -> Result<()> {
    let query = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_QUERY.to_string());

    print_examples();

    let client = connect_spice()
        .await
        .map_err(|error| anyhow!("failed to connect to Spice runtime: {error}"))?;

    let mut stream = client
        .query(&query)
        .await
        .with_context(|| format!("failed to run Spice query: {query}"))?;

    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.map_err(|error| anyhow!("failed to read Spice result stream: {error}"))?);
    }

    println!("\n执行 SQL:\n{query}");

    if batches.is_empty() {
        println!("\n结果: <empty>");
        return Ok(());
    }

    let combined = concat_batches(&batches[0].schema(), &batches)
        .map_err(|error| anyhow!("failed to combine result batches: {error}"))?;
    let formatted = pretty_format_batches(&[combined])
        .map_err(|error| anyhow!("failed to format query result: {error}"))?;

    println!("\n查询结果:\n{formatted}");

    Ok(())
}
