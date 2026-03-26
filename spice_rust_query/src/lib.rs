use anyhow::{Context, Result, anyhow};
use futures::future::try_join_all;
use futures::StreamExt;
use spiceai::{Client, ClientBuilder};
use std::future::Future;
use std::time::{Duration, Instant};

pub const SPICE_FLIGHT_URL: &str = "http://127.0.0.1:50012";

pub const PERF_ROW_COUNT: u64 = 1000_0000;
pub const PERF_CITY_COUNT: u64 = 10_0000;
pub const PERF_BATCH_SIZE: u64 = 10_0000;
pub const PERF_ITERATIONS: usize = 1000;
pub const PERF_PARALLELISM: usize = 4;
pub const TEST_WORKER_THREADS: usize = 4;

pub const PG_HOST: &str = "127.0.0.1";
pub const PG_PORT: u16 = 5432;
pub const PG_DB: &str = "postgres";
pub const PG_USER: &str = "root";
pub const PG_PASSWORD: &str = "123456";

pub const CK_ENDPOINT: &str = "http://127.0.0.1:8123";
pub const CK_HOST: &str = "localhost";
pub const CK_TCP_PORT: u16 = 9000;
pub const CK_DB: &str = "test_db";
pub const CK_USER: &str = "default";
pub const CK_PASSWORD: &str = "default";

pub const CSV_OUTPUT_DIR: &str = ".run/data/csv";
pub const CSV_ATTACKER_IP_FILE: &str = ".run/data/csv/attacker_ip_perf.csv";
pub const CSV_CITY_FILE: &str = ".run/data/csv/city_perf.csv";

pub const NONEXISTENT_IP: &str = "203.255.255.255";
pub const HOT_IP_POOL_SIZE: usize = 10;

const NATIONS: [&str; 8] = [
    "China",
    "Japan",
    "Singapore",
    "Germany",
    "UnitedStates",
    "France",
    "Canada",
    "Australia",
];

#[derive(Debug, Clone)]
pub struct BenchmarkReport {
    pub name: String,
    pub iterations: usize,
    pub rows: usize,
    pub total: Duration,
    pub parallelism: usize,
    pub min: Duration,
    pub max: Duration,
    pub avg: Duration,
}

#[derive(Debug, Clone, Copy)]
pub enum BenchmarkKind {
    ExactIpCityNoCache,
    PartialIpCacheHit,
    NonexistentIp,
    JoinNationNoCache,
    JoinNationCacheHit,
}

#[derive(Debug, Clone, Copy)]
pub struct SpiceSource {
    pub label: &'static str,
    pub attacker_dataset: &'static str,
    pub city_dataset: &'static str,
}

pub const SPICE_SOURCE_PG: SpiceSource = SpiceSource {
    label: "postgres",
    attacker_dataset: "pg_attacker_ip_perf",
    city_dataset: "pg_city_perf",
};

pub const SPICE_SOURCE_CK: SpiceSource = SpiceSource {
    label: "clickhouse",
    attacker_dataset: "ck_attacker_ip_perf",
    city_dataset: "ck_city_perf",
};

pub const SPICE_SOURCE_CSV: SpiceSource = SpiceSource {
    label: "csv",
    attacker_dataset: "csv_attacker_ip_perf",
    city_dataset: "csv_city_perf",
};

impl BenchmarkKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::ExactIpCityNoCache => "exact_ip_city_no_cache",
            Self::PartialIpCacheHit => "partial_ip_cache_hit",
            Self::NonexistentIp => "nonexistent_ip",
            Self::JoinNationNoCache => "join_nation_no_cache",
            Self::JoinNationCacheHit => "join_nation_cache_hit",
        }
    }

    pub fn uses_warmup(self) -> bool {
        matches!(self, Self::PartialIpCacheHit | Self::JoinNationCacheHit)
    }
}

pub fn city_name(id: u64) -> String {
    format!("city_{id:06}")
}

pub fn nation_name(city_id: u64) -> &'static str {
    let index = ((city_id - 1) as usize) % NATIONS.len();
    NATIONS[index]
}

pub fn city_id_for_row(id: u64, city_count: u64) -> u64 {
    ((id - 1) % city_count.max(1)) + 1
}

pub fn attacker_ip_for_id(id: u64) -> String {
    if id % 10 == 0 {
        format!(
            "10.42.{}.{}",
            (id / 256) % 256,
            id % 256,
        )
    } else {
        format!(
            "172.{}.{}.{}",
            (id / 65_536) % 256,
            (id / 256) % 256,
            id % 256,
        )
    }
}

pub fn existing_ip_for_iteration(iteration: usize, row_count: u64) -> String {
    let row_count = row_count.max(1);
    let id = 1 + (((iteration as u64) * 7_919) % row_count);
    attacker_ip_for_id(id)
}

pub fn cached_ip(row_count: u64) -> String {
    existing_ip_for_iteration(7, row_count)
}

pub fn exact_lookup_ip_for_iteration(iteration: usize) -> String {
    let mut id = 1 + (iteration as u64) * 11;
    if id % 10 == 0 {
        id += 1;
    }
    format!(
        "172.{}.{}.{}",
        (id / 65_536) % 256,
        (id / 256) % 256,
        id % 256,
    )
}

pub fn cached_exact_lookup_ip() -> String {
    exact_lookup_ip_for_iteration(7)
}

pub fn hot_ip_for_iteration(iteration: usize) -> String {
    exact_lookup_ip_for_iteration(10_000 + (iteration % HOT_IP_POOL_SIZE))
}

pub fn cached_hot_ip() -> String {
    hot_ip_for_iteration(7)
}

pub async fn connect_spice() -> Result<Client> {
    ClientBuilder::new()
        .flight_url(SPICE_FLIGHT_URL)
        .build()
        .await
        .map_err(|error| anyhow!("failed to connect to Spice runtime: {error}"))
}

pub async fn execute_spice_query(client: &Client, query: &str) -> Result<usize> {
    let mut stream = client
        .query(query)
        .await
        .with_context(|| format!("failed to run Spice query: {query}"))?;

    let mut row_count = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| anyhow!("failed to read Spice result stream: {error}"))?;
        row_count += batch.num_rows();
    }

    Ok(row_count)
}

pub async fn run_benchmark_case<F>(
    name: &str,
    iterations: usize,
    parallelism: usize,
    warmup: bool,
    mut executor: F,
) -> Result<BenchmarkReport>
where
    F: FnMut(usize, usize) -> Vec<futures::future::BoxFuture<'static, Result<usize>>>,
{
    if warmup {
        let mut warmup_rows = None;
        for result in try_join_all(executor(0, parallelism)).await? {
            if let Some(existing) = warmup_rows {
                if existing != result {
                    return Err(anyhow!(
                        "warmup returned inconsistent row counts: expected {existing}, got {result}"
                    ));
                }
            } else {
                warmup_rows = Some(result);
            }
        }
    }

    let wall_clock_start = Instant::now();
    let mut durations = Vec::with_capacity(iterations);
    let mut rows = 0usize;

    for iteration in 0..iterations {
        let start = Instant::now();
        let results = try_join_all(executor(iteration, parallelism)).await?;
        let current_rows = *results
            .first()
            .ok_or_else(|| anyhow!("benchmark iteration returned no query results"))?;
        for result in &results {
            if *result != current_rows {
                return Err(anyhow!(
                    "iteration {iteration} returned inconsistent row counts: expected {current_rows}, got {result}"
                ));
            }
        }
        rows = current_rows;
        durations.push(start.elapsed());
    }

    let min = durations.iter().copied().min().unwrap_or_default();
    let max = durations.iter().copied().max().unwrap_or_default();
    let total_nanos: u128 = durations.iter().map(Duration::as_nanos).sum();
    let avg = if iterations == 0 {
        Duration::default()
    } else {
        Duration::from_nanos((total_nanos / iterations as u128) as u64)
    };

    Ok(BenchmarkReport {
        name: name.to_string(),
        iterations,
        rows,
        total: wall_clock_start.elapsed(),
        parallelism,
        min,
        max,
        avg,
    })
}

pub fn print_benchmark_report(report: &BenchmarkReport) {
    println!(
        "- 测试项：{} | 迭代次数：{} | 单次返回行数：{} | 并行度：{} | 墙钟总耗时：{:.3} ms | 最小耗时：{:.3} ms | 平均耗时：{:.3} ms | 最大耗时：{:.3} ms",
        benchmark_kind_zh(&report.name),
        report.iterations,
        report.rows,
        report.parallelism,
        report.total.as_secs_f64() * 1000.0,
        report.min.as_secs_f64() * 1000.0,
        report.avg.as_secs_f64() * 1000.0,
        report.max.as_secs_f64() * 1000.0,
    );
}

pub fn benchmark_kind_zh(name: &str) -> &str {
    match name {
        "exact_ip_city_no_cache" => "任意 IP 查询城市（避免缓存）",
        "partial_ip_cache_hit" => "查询热点 IP（命中缓存）",
        "nonexistent_ip" => "查询不存在的 IP",
        "join_nation_no_cache" => "查询 IP 对应国家（表连接，避免缓存）",
        "join_nation_cache_hit" => "查询 IP 对应国家（表连接，命中缓存）",
        _ => name,
    }
}

pub fn build_spice_query(
    kind: BenchmarkKind,
    attacker_dataset: &str,
    city_dataset: &str,
    iteration: usize,
) -> String {
    match kind {
        BenchmarkKind::ExactIpCityNoCache => {
            let ip = exact_lookup_ip_for_iteration(iteration);
            format!("SELECT city_name FROM {attacker_dataset} WHERE attacker_ip = '{ip}';")
        }
        BenchmarkKind::PartialIpCacheHit => {
            let ip = cached_hot_ip();
            format!(
                "SELECT attacker_ip, city_name FROM {attacker_dataset} WHERE attacker_ip = '{ip}';"
            )
        }
        BenchmarkKind::NonexistentIp => format!(
            "SELECT city_name FROM {attacker_dataset} WHERE attacker_ip = '{NONEXISTENT_IP}';"
        ),
        BenchmarkKind::JoinNationNoCache => {
            let ip = exact_lookup_ip_for_iteration(iteration);
            format!(
                "WITH filtered AS (SELECT attacker_ip, city_id FROM {attacker_dataset} WHERE attacker_ip = '{ip}') SELECT a.attacker_ip, c.nation FROM filtered a JOIN {city_dataset} c ON a.city_id = c.id;"
            )
        }
        BenchmarkKind::JoinNationCacheHit => {
            let repeated_ip = cached_exact_lookup_ip();
            format!(
                "WITH filtered AS (SELECT attacker_ip, city_id FROM {attacker_dataset} WHERE attacker_ip = '{repeated_ip}') SELECT a.attacker_ip, c.nation FROM filtered a JOIN {city_dataset} c ON a.city_id = c.id;"
            )
        }
    }
}

pub async fn run_single_source_benchmark(
    label: &str,
    attacker_dataset: &str,
    city_dataset: &str,
    kind: BenchmarkKind,
    iterations: usize,
    parallelism: usize,
) -> Result<()> {
    let client = connect_spice().await?;

    println!("source={label}, attacker_dataset={attacker_dataset}, city_dataset={city_dataset}");
    println!(
        "iterations={}, row_count={}, hot_ip_pool_size={}, flight_url={}",
        iterations, PERF_ROW_COUNT, HOT_IP_POOL_SIZE, SPICE_FLIGHT_URL
    );

    let report = run_benchmark_case(
        kind.name(),
        iterations,
        parallelism,
        kind.uses_warmup(),
        |iteration, parallelism| {
            (0..parallelism)
                .map(|offset| {
                    let query = build_spice_query(
                        kind,
                        attacker_dataset,
                        city_dataset,
                        iteration * parallelism + offset,
                    );
                    let client = client.clone();
                    Box::pin(async move { execute_spice_query(&client, &query).await })
                        as futures::future::BoxFuture<'static, Result<usize>>
                })
                .collect()
        },
    )
    .await?;

    print_benchmark_report(&report);

    Ok(())
}

pub async fn collect_spice_benchmark(
    source: SpiceSource,
    kind: BenchmarkKind,
    iterations: usize,
    parallelism: usize,
) -> Result<BenchmarkReport> {
    let client = connect_spice().await?;
    let report = run_benchmark_case(
        kind.name(),
        iterations,
        parallelism,
        kind.uses_warmup(),
        |iteration, parallelism| {
            (0..parallelism)
                .map(|offset| {
                    let query = build_spice_query(
                        kind,
                        source.attacker_dataset,
                        source.city_dataset,
                        iteration * parallelism + offset,
                    );
                    let client = client.clone();
                    Box::pin(async move { execute_spice_query(&client, &query).await })
                        as futures::future::BoxFuture<'static, Result<usize>>
                })
                .collect()
        },
    )
    .await?;
    Ok(report)
}

pub async fn run_native_benchmark_case<F, Fut>(
    name: &str,
    iterations: usize,
    parallelism: usize,
    warmup: bool,
    mut executor: F,
) -> Result<BenchmarkReport>
where
    F: FnMut(usize, usize) -> Fut,
    Fut: Future<Output = Result<usize>>,
{
    if warmup {
        let _ = executor(0, parallelism).await?;
    }

    let wall_clock_start = Instant::now();
    let mut durations = Vec::with_capacity(iterations);
    let mut rows = 0usize;

    for iteration in 0..iterations {
        let start = Instant::now();
        rows = executor(iteration, parallelism).await?;
        durations.push(start.elapsed());
    }

    let min = durations.iter().copied().min().unwrap_or_default();
    let max = durations.iter().copied().max().unwrap_or_default();
    let total_nanos: u128 = durations.iter().map(Duration::as_nanos).sum();
    let avg = if iterations == 0 {
        Duration::default()
    } else {
        Duration::from_nanos((total_nanos / iterations as u128) as u64)
    };

    Ok(BenchmarkReport {
        name: name.to_string(),
        iterations,
        rows,
        total: wall_clock_start.elapsed(),
        parallelism,
        min,
        max,
        avg,
    })
}
