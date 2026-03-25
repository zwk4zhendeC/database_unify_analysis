use anyhow::{Context, Result};
use spice_rust_query::{
    PERF_BATCH_SIZE, PERF_CITY_COUNT, PERF_ROW_COUNT, PG_DB, PG_HOST, PG_PASSWORD, PG_PORT,
    PG_USER, attacker_ip_for_id, city_id_for_row, city_name, nation_name,
};
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<()> {
    let connection_string = format!(
        "host={} port={} dbname={} user={} password={} sslmode=disable",
        PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .context("failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {error}");
        }
    });

    client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS public.city_perf (
                id BIGSERIAL PRIMARY KEY,
                city_name VARCHAR(50),
                nation VARCHAR(50)
            );
            CREATE TABLE IF NOT EXISTS public.attacker_ip_perf (
                id BIGSERIAL PRIMARY KEY,
                attacker_ip VARCHAR(50),
                city_id BIGINT,
                city_name VARCHAR(50)
            );
            TRUNCATE TABLE public.attacker_ip_perf, public.city_perf RESTART IDENTITY;
            ",
        )
        .await
        .context("failed to create or truncate PostgreSQL benchmark tables")?;

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

        let sql = format!(
            "INSERT INTO public.city_perf (id, city_name, nation) VALUES {};",
            values.join(",")
        );
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("failed to insert PostgreSQL city batch {start}-{end}"))?;
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

        let sql = format!(
            "INSERT INTO public.attacker_ip_perf (id, attacker_ip, city_id, city_name) VALUES {};",
            values.join(",")
        );
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("failed to insert PostgreSQL attacker batch {start}-{end}"))?;
    }

    client
        .execute(
            "SELECT setval(pg_get_serial_sequence('public.city_perf', 'id'), $1, true)",
            &[&(PERF_CITY_COUNT as i64)],
        )
        .await
        .context("failed to advance PostgreSQL city sequence")?;
    client
        .execute(
            "SELECT setval(pg_get_serial_sequence('public.attacker_ip_perf', 'id'), $1, true)",
            &[&(PERF_ROW_COUNT as i64)],
        )
        .await
        .context("failed to advance PostgreSQL attacker sequence")?;

    println!(
        "seeded PostgreSQL benchmark tables: public.city_perf={} rows, public.attacker_ip_perf={} rows",
        PERF_CITY_COUNT, PERF_ROW_COUNT
    );

    Ok(())
}
