use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use spice_rust_query::{
    CK_DB, CK_ENDPOINT, CK_PASSWORD, CK_USER, PERF_BATCH_SIZE, PERF_CITY_COUNT, PERF_ROW_COUNT,
    attacker_ip_for_id, city_id_for_row, city_name, nation_name,
};

struct ClickHouseConfig {
    endpoint: String,
    database: String,
    user: String,
    password: String,
}

async fn execute_sql(client: &Client, config: &ClickHouseConfig, sql: &str) -> Result<()> {
    let response = client
        .post(&config.endpoint)
        .basic_auth(&config.user, Some(&config.password))
        .body(sql.to_string())
        .send()
        .await
        .with_context(|| format!("failed to send ClickHouse SQL: {sql}"))?;

    if response.status().is_success() {
        return Ok(());
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(anyhow!("ClickHouse SQL failed with status {status}: {body}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ClickHouseConfig {
        endpoint: CK_ENDPOINT.to_string(),
        database: CK_DB.to_string(),
        user: CK_USER.to_string(),
        password: CK_PASSWORD.to_string(),
    };

    let client = Client::builder()
        .build()
        .context("failed to build ClickHouse HTTP client")?;

    execute_sql(&client, &config, &format!("CREATE DATABASE IF NOT EXISTS {}", config.database))
        .await?;
    execute_sql(
        &client,
        &config,
        &format!(
            "CREATE TABLE IF NOT EXISTS {}.city_perf (id UInt64, city_name String, nation String) ENGINE = MergeTree() ORDER BY id",
            config.database
        ),
    )
    .await?;
    execute_sql(
        &client,
        &config,
        &format!(
            "CREATE TABLE IF NOT EXISTS {}.attacker_ip_perf (id UInt64, attacker_ip String, city_id UInt64, city_name String) ENGINE = MergeTree() ORDER BY id",
            config.database
        ),
    )
    .await?;
    execute_sql(
        &client,
        &config,
        &format!("TRUNCATE TABLE IF EXISTS {}.city_perf", config.database),
    )
    .await?;
    execute_sql(
        &client,
        &config,
        &format!("TRUNCATE TABLE IF EXISTS {}.attacker_ip_perf", config.database),
    )
    .await?;

    for start in (1..=PERF_CITY_COUNT).step_by(PERF_BATCH_SIZE as usize) {
        let end = (start + PERF_BATCH_SIZE - 1).min(PERF_CITY_COUNT);
        let mut values = Vec::with_capacity((end - start + 1) as usize);
        for id in start..=end {
            values.push(format!(
                "({}, '{}', '{}')",
                id,
                city_name(id),
                nation_name(id)
            ));
        }

        execute_sql(
            &client,
            &config,
            &format!(
                "INSERT INTO {}.city_perf (id, city_name, nation) VALUES {}",
                config.database,
                values.join(",")
            ),
        )
        .await
        .with_context(|| format!("failed to insert ClickHouse city batch {start}-{end}"))?;
    }

    for start in (1..=PERF_ROW_COUNT).step_by(PERF_BATCH_SIZE as usize) {
        let end = (start + PERF_BATCH_SIZE - 1).min(PERF_ROW_COUNT);
        let mut values = Vec::with_capacity((end - start + 1) as usize);
        for id in start..=end {
            let city_id = city_id_for_row(id, PERF_CITY_COUNT);
            values.push(format!(
                "({}, '{}', {}, '{}')",
                id,
                attacker_ip_for_id(id),
                city_id,
                city_name(city_id)
            ));
        }

        execute_sql(
            &client,
            &config,
            &format!(
                "INSERT INTO {}.attacker_ip_perf (id, attacker_ip, city_id, city_name) VALUES {}",
                config.database,
                values.join(",")
            ),
        )
        .await
        .with_context(|| format!("failed to insert ClickHouse attacker batch {start}-{end}"))?;
    }

    println!(
        "seeded ClickHouse benchmark tables: {}.city_perf={} rows, {}.attacker_ip_perf={} rows",
        config.database, PERF_CITY_COUNT, config.database, PERF_ROW_COUNT
    );

    Ok(())
}
