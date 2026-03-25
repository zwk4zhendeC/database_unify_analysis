#![allow(dead_code)]

use anyhow::{Context, Result};
use clickhouse::Row;
use serde::Deserialize;
use spice_rust_query::{
    BenchmarkKind, BenchmarkReport, CK_DB, CK_ENDPOINT, CK_PASSWORD, CK_USER, CSV_ATTACKER_IP_FILE,
    CSV_CITY_FILE, HOT_IP_POOL_SIZE, NONEXISTENT_IP, PERF_ITERATIONS, PERF_ROW_COUNT, PG_DB,
    PG_HOST, PG_PASSWORD, PG_PORT, PG_USER, build_spice_query, cached_exact_lookup_ip,
    cached_hot_ip, city_name, exact_lookup_ip_for_iteration, run_native_benchmark_case,
};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::process::Command;
use std::process::{Child, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread::{self, sleep};
use std::time::Duration;
use tokio_postgres::{Client as PgClient, NoTls};

const DOCKER_COMPOSE_FILE: &str = "component/docker-compose.yml";
const RUN_REPORT_DIR: &str = ".run/report";
const SPICE_PERF_DIR: &str = "spice/perf";
const SPICE_LOG_DIR: &str = ".run/spice_logs";
const SPICE_HTTP_ENDPOINT: &str = "127.0.0.1:8090";
const SPICE_FLIGHT_ENDPOINT: &str = "127.0.0.1:50012";
const SPICE_READY_TIMEOUT_SECS: u64 = 60;
const SOURCE_READY_TIMEOUT_SECS: u64 = 60;
const SPICE_MONITOR_INTERVAL_MILLIS: u64 = 500;
const ALL_KINDS: [BenchmarkKind; 5] = [
    BenchmarkKind::ExactIpCityNoCache,
    BenchmarkKind::PartialIpCacheHit,
    BenchmarkKind::NonexistentIp,
    BenchmarkKind::JoinNationNoCache,
    BenchmarkKind::JoinNationCacheHit,
];

#[derive(Debug, Clone)]
pub struct CsvAttackerRow {
    pub id: u64,
    pub attacker_ip: String,
    pub city_id: u64,
    pub city_name: String,
}

#[derive(Debug, Clone)]
pub struct CsvCityRow {
    pub id: u64,
    pub city_name: String,
    pub nation: String,
}

#[derive(Debug, Row, Deserialize)]
struct CkAttackerIpRow {
    attacker_ip: String,
    city_name: String,
}

#[derive(Debug, Row, Deserialize)]
struct CkCityNameRow {
    city_name: String,
}

#[derive(Debug, Row, Deserialize)]
struct CkNationRow {
    attacker_ip: String,
    nation: String,
}

#[derive(Debug, Clone, Copy)]
struct ResourceSample {
    cpu_percent: f64,
    memory_kb: u64,
}

#[derive(Debug, Clone)]
struct ResourceStats {
    sample_count: usize,
    avg_cpu_percent: f64,
    peak_cpu_percent: f64,
    avg_memory_mb: f64,
    peak_memory_mb: f64,
}

#[derive(Debug, Clone)]
struct SpiceCaseSummary {
    kind: BenchmarkKind,
    report: BenchmarkReport,
    resource_stats: ResourceStats,
}

#[derive(Debug, Clone)]
struct NativeCaseSummary {
    kind: BenchmarkKind,
    report: BenchmarkReport,
}

struct SpiceRuntimeHandle {
    child: Child,
    monitor_stop: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<ResourceSample>>>,
    monitor_thread: Option<thread::JoinHandle<()>>,
    stopped: bool,
}

pub fn expected_rows(kind: BenchmarkKind) -> usize {
    match kind {
        BenchmarkKind::ExactIpCityNoCache => 1,
        BenchmarkKind::PartialIpCacheHit => 1,
        BenchmarkKind::NonexistentIp => 0,
        BenchmarkKind::JoinNationNoCache => 1,
        BenchmarkKind::JoinNationCacheHit => 1,
    }
}

pub fn assert_report(report: &BenchmarkReport, kind: BenchmarkKind) {
    assert_eq!(report.iterations, PERF_ITERATIONS);
    assert_eq!(report.rows, expected_rows(kind));
}

fn ensure_consistent_rows(results: &[usize], iteration: usize) -> Result<usize> {
    let first = *results
        .first()
        .ok_or_else(|| anyhow::anyhow!("iteration {iteration} returned no results"))?;
    for result in results {
        if *result != first {
            return Err(anyhow::anyhow!(
                "iteration {iteration} returned inconsistent row counts: expected {first}, got {result}"
            ));
        }
    }
    Ok(first)
}

fn sample_process_usage(pid: u32) -> Result<Option<ResourceSample>> {
    let output = Command::new("ps")
        .args(["-o", "%cpu=,rss=", "-p", &pid.to_string()])
        .output()
        .context("failed to sample Spice process usage")?;

    if !output.status.success() {
        return Ok(None);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.trim();
    if line.is_empty() {
        return Ok(None);
    }

    let mut parts = line.split_whitespace();
    let cpu_percent = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing CPU column in ps output"))?
        .parse::<f64>()
        .context("failed to parse CPU percentage")?;
    let memory_kb = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing RSS column in ps output"))?
        .parse::<u64>()
        .context("failed to parse RSS memory")?;

    Ok(Some(ResourceSample {
        cpu_percent,
        memory_kb,
    }))
}

fn summarize_samples(samples: &[ResourceSample]) -> ResourceStats {
    if samples.is_empty() {
        return ResourceStats {
            sample_count: 0,
            avg_cpu_percent: 0.0,
            peak_cpu_percent: 0.0,
            avg_memory_mb: 0.0,
            peak_memory_mb: 0.0,
        };
    }

    let sample_count = samples.len();
    let total_cpu: f64 = samples.iter().map(|sample| sample.cpu_percent).sum();
    let peak_cpu_percent = samples
        .iter()
        .map(|sample| sample.cpu_percent)
        .fold(0.0, f64::max);
    let total_memory_kb: u64 = samples.iter().map(|sample| sample.memory_kb).sum();
    let peak_memory_kb = samples
        .iter()
        .map(|sample| sample.memory_kb)
        .max()
        .unwrap_or(0);

    ResourceStats {
        sample_count,
        avg_cpu_percent: total_cpu / sample_count as f64,
        peak_cpu_percent,
        avg_memory_mb: total_memory_kb as f64 / sample_count as f64 / 1024.0,
        peak_memory_mb: peak_memory_kb as f64 / 1024.0,
    }
}

fn suite_report_path(source: &str, mode: &str) -> String {
    format!("{RUN_REPORT_DIR}/{source}.{mode}.md")
}

fn format_duration_ms(duration: Duration) -> String {
    format!("{:.3} ms", duration.as_secs_f64() * 1000.0)
}

fn build_markdown_header() -> String {
    "| 测试项 | 总耗时 | 平均耗时 | 最小耗时 | 最大耗时 | Spice 平均CPU占用 | Spice 平均内存占用 | Spice 峰值CPU | Spice峰值内存 |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|".to_string()
}

fn format_native_markdown_row(summary: &NativeCaseSummary) -> String {
    format!(
        "| {} | {} | {} | {} | {} | - | - | - | - |",
        spice_rust_query::benchmark_kind_zh(summary.kind.name()),
        format_duration_ms(summary.report.total),
        format_duration_ms(summary.report.avg),
        format_duration_ms(summary.report.min),
        format_duration_ms(summary.report.max),
    )
}

fn format_spice_markdown_row(summary: &SpiceCaseSummary) -> String {
    format!(
        "| {} | {} | {} | {} | {} | {:.2}% | {:.2} MiB | {:.2}% | {:.2} MiB |",
        spice_rust_query::benchmark_kind_zh(summary.kind.name()),
        format_duration_ms(summary.report.total),
        format_duration_ms(summary.report.avg),
        format_duration_ms(summary.report.min),
        format_duration_ms(summary.report.max),
        summary.resource_stats.avg_cpu_percent,
        summary.resource_stats.avg_memory_mb,
        summary.resource_stats.peak_cpu_percent,
        summary.resource_stats.peak_memory_mb,
    )
}

fn build_suite_header(source: &str, mode: &str) -> String {
    format!(
        "# 测试报告\n\n- 数据源：{source}\n- 查询方式：{}\n- 测试总数：5\n- 迭代次数：{}\n- 并行度：{}\n- 数据规模：{}\n- 热点 IP 池大小：{}\n- 示例城市：{}",
        if mode == "spice" { "Spice" } else { "原生 SDK" },
        PERF_ITERATIONS,
        spice_rust_query::PERF_PARALLELISM,
        PERF_ROW_COUNT,
        HOT_IP_POOL_SIZE,
        city_name(1)
    )
}

fn write_report_file(source: &str, mode: &str, content: &str) -> Result<()> {
    fs::create_dir_all(RUN_REPORT_DIR).context("failed to create .run/report directory")?;
    fs::write(suite_report_path(source, mode), content).context("failed to write report file")?;
    Ok(())
}

fn print_and_write_native_suite_report(
    source: &str,
    summaries: &[NativeCaseSummary],
) -> Result<()> {
    let header = build_suite_header(source, "native");
    let mut lines = vec![header.clone(), String::new(), String::from("## 测试结果"), String::new(), build_markdown_header()];
    let mut total_wall_ms = 0.0f64;
    let mut fastest: Option<(&str, f64)> = None;
    let mut slowest: Option<(&str, f64)> = None;

    for summary in summaries {
        let line = format_native_markdown_row(summary);
        lines.push(line);
        let wall_ms = summary.report.total.as_secs_f64() * 1000.0;
        total_wall_ms += wall_ms;
        let kind_zh = spice_rust_query::benchmark_kind_zh(summary.kind.name());
        if fastest.is_none_or(|(_, value)| wall_ms < value) {
            fastest = Some((kind_zh, wall_ms));
        }
        if slowest.is_none_or(|(_, value)| wall_ms > value) {
            slowest = Some((kind_zh, wall_ms));
        }
    }

    lines.push(String::new());
    lines.push(String::from("## 汇总"));
    lines.push(String::new());
    let summary_line = format!(
        "- 总耗时：{:.3} ms | 平均单项总耗时：{:.3} ms | 最快测试项：{} ({:.3} ms) | 最慢测试项：{} ({:.3} ms)",
        total_wall_ms,
        total_wall_ms / summaries.len() as f64,
        fastest.map(|(name, _)| name).unwrap_or("N/A"),
        fastest.map(|(_, value)| value).unwrap_or(0.0),
        slowest.map(|(name, _)| name).unwrap_or("N/A"),
        slowest.map(|(_, value)| value).unwrap_or(0.0),
    );
    lines.push(summary_line);
    let content = lines.join("\n");
    println!("{content}");
    write_report_file(source, "native", &content)
}

fn print_and_write_spice_suite_report(
    source: &str,
    summaries: &[SpiceCaseSummary],
    total_stats: &ResourceStats,
) -> Result<()> {
    let header = build_suite_header(source, "spice");
    let mut lines = vec![header.clone(), String::new(), String::from("## 测试结果"), String::new(), build_markdown_header()];
    let mut total_wall_ms = 0.0f64;
    let mut fastest: Option<(&str, f64)> = None;
    let mut slowest: Option<(&str, f64)> = None;

    for summary in summaries {
        lines.push(format_spice_markdown_row(summary));

        let wall_ms = summary.report.total.as_secs_f64() * 1000.0;
        total_wall_ms += wall_ms;
        let kind_zh = spice_rust_query::benchmark_kind_zh(summary.kind.name());
        if fastest.is_none_or(|(_, value)| wall_ms < value) {
            fastest = Some((kind_zh, wall_ms));
        }
        if slowest.is_none_or(|(_, value)| wall_ms > value) {
            slowest = Some((kind_zh, wall_ms));
        }
    }

    lines.push(String::new());
    lines.push(String::from("## 汇总"));
    lines.push(String::new());
    let summary_line = format!(
        "- 总耗时：{:.3} ms | 平均单项总耗时：{:.3} ms | 最快测试项：{} ({:.3} ms) | 最慢测试项：{} ({:.3} ms)",
        total_wall_ms,
        total_wall_ms / summaries.len() as f64,
        fastest.map(|(name, _)| name).unwrap_or("N/A"),
        fastest.map(|(_, value)| value).unwrap_or(0.0),
        slowest.map(|(name, _)| name).unwrap_or("N/A"),
        slowest.map(|(_, value)| value).unwrap_or(0.0),
    );
    lines.push(summary_line);

    let total_resource_line = format!(
        "- Spice 汇总：平均CPU {:.2}% | 平均内存 {:.2} MiB | 峰值CPU {:.2}% | 峰值内存 {:.2} MiB | 采样次数 {}",
        total_stats.avg_cpu_percent,
        total_stats.avg_memory_mb,
        total_stats.peak_cpu_percent,
        total_stats.peak_memory_mb,
        total_stats.sample_count,
    );
    lines.push(total_resource_line);
    let content = lines.join("\n");
    println!("{content}");
    write_report_file(source, "spice", &content)
}

fn cleanup_stale_spice_processes() -> Result<()> {
    for endpoint in [SPICE_HTTP_ENDPOINT, SPICE_FLIGHT_ENDPOINT] {
        let port = endpoint
            .rsplit(':')
            .next()
            .ok_or_else(|| anyhow::anyhow!("failed to parse port from endpoint {endpoint}"))?;
        let output = Command::new("lsof")
            .args(["-ti", &format!("tcp:{port}")])
            .output()
            .with_context(|| format!("failed to inspect listeners on port {port}"))?;

        if !output.status.success() && !output.stdout.is_empty() {
            continue;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for pid in stdout.lines().map(str::trim).filter(|line| !line.is_empty()) {
            let ps_output = Command::new("ps")
                .args(["-p", pid, "-o", "comm="])
                .output()
                .with_context(|| format!("failed to inspect process {pid} on port {port}"))?;

            let command = String::from_utf8_lossy(&ps_output.stdout);
            if !command.contains("spice") {
                return Err(anyhow::anyhow!(
                    "port {port} is occupied by non-Spice process {pid}: {}",
                    command.trim()
                ));
            }

            let status = Command::new("kill")
                .args(["-TERM", pid])
                .status()
                .with_context(|| format!("failed to terminate stale Spice process {pid}"))?;

            if !status.success() {
                return Err(anyhow::anyhow!(
                    "failed to terminate stale Spice process {pid} on port {port}"
                ));
            }
        }
    }

    thread::sleep(Duration::from_millis(500));
    Ok(())
}

fn wait_for_spice_ready(child: &mut Child) -> Result<()> {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(SPICE_READY_TIMEOUT_SECS) {
        if let Some(status) = child
            .try_wait()
            .context("failed to poll Spice runtime process status")?
        {
            return Err(anyhow::anyhow!(
                "Spice runtime exited before becoming ready with status: {status}"
            ));
        }

        let output = Command::new("curl")
            .args(["-sS", &format!("http://{SPICE_HTTP_ENDPOINT}/v1/status")])
            .output();

        if let Ok(output) = output {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                if stdout.contains("\"status\":\"Ready\"") || stdout.contains("Ready") {
                    return Ok(());
                }
            }
        }

        thread::sleep(Duration::from_millis(500));
    }

    Err(anyhow::anyhow!(
        "Spice runtime did not become ready within {} seconds",
        SPICE_READY_TIMEOUT_SECS
    ))
}

async fn wait_for_postgres_ready() -> Result<()> {
    let start = std::time::Instant::now();
    let connection_string = format!(
        "host={} port={} dbname={} user={} password={} sslmode=disable",
        PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    );

    while start.elapsed() < Duration::from_secs(SOURCE_READY_TIMEOUT_SECS) {
        if let Ok((client, connection)) = tokio_postgres::connect(&connection_string, NoTls).await {
            let connection_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let ready = client.simple_query("SELECT 1").await.is_ok();
            connection_task.abort();

            if ready {
                return Ok(());
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(anyhow::anyhow!(
        "PostgreSQL did not become ready within {} seconds",
        SOURCE_READY_TIMEOUT_SECS
    ))
}

async fn wait_for_clickhouse_ready() -> Result<()> {
    let start = std::time::Instant::now();

    while start.elapsed() < Duration::from_secs(SOURCE_READY_TIMEOUT_SECS) {
        if let Ok(output) = Command::new("curl")
            .args(["-fsS", &format!("{CK_ENDPOINT}/ping")])
            .output()
        {
            if output.status.success() {
                let body = String::from_utf8_lossy(&output.stdout);
                if body.trim() == "Ok." {
                    return Ok(());
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(anyhow::anyhow!(
        "ClickHouse did not become ready within {} seconds",
        SOURCE_READY_TIMEOUT_SECS
    ))
}

async fn wait_for_sources_ready() -> Result<()> {
    wait_for_postgres_ready().await?;
    wait_for_clickhouse_ready().await?;
    Ok(())
}

fn start_spice_runtime(kind: BenchmarkKind) -> Result<SpiceRuntimeHandle> {
    cleanup_stale_spice_processes()?;
    fs::create_dir_all(SPICE_LOG_DIR).context("failed to create Spice log directory")?;
    let stdout_log = File::create(format!(
        "{SPICE_LOG_DIR}/{}_stdout.log",
        kind.name()
    ))
    .context("failed to create Spice stdout log")?;
    let stderr_log = File::create(format!(
        "{SPICE_LOG_DIR}/{}_stderr.log",
        kind.name()
    ))
    .context("failed to create Spice stderr log")?;

    let mut child = Command::new("spice")
        .args([
            "run",
            "--http-endpoint",
            SPICE_HTTP_ENDPOINT,
            "--flight-endpoint",
            SPICE_FLIGHT_ENDPOINT,
        ])
        .current_dir(SPICE_PERF_DIR)
        .stdout(Stdio::from(stdout_log))
        .stderr(Stdio::from(stderr_log))
        .spawn()
        .context("failed to start Spice runtime")?;

    wait_for_spice_ready(&mut child)?;

    let pid = child.id();
    println!("Spice runtime started with PID {pid} for test case {}", kind.name());
    let monitor_stop = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let monitor_stop_clone = Arc::clone(&monitor_stop);
    let samples_clone = Arc::clone(&samples);

    let monitor_thread = thread::spawn(move || {
        while !monitor_stop_clone.load(Ordering::Relaxed) {
            if let Ok(Some(sample)) = sample_process_usage(pid) {
                if let Ok(mut guard) = samples_clone.lock() {
                    guard.push(sample);
                }
            }
            thread::sleep(Duration::from_millis(SPICE_MONITOR_INTERVAL_MILLIS));
        }
    });

    Ok(SpiceRuntimeHandle {
        child,
        monitor_stop,
        samples,
        monitor_thread: Some(monitor_thread),
        stopped: false,
    })
}

impl SpiceRuntimeHandle {
    fn terminate(&mut self) {
        if self.stopped {
            return;
        }

        self.monitor_stop.store(true, Ordering::Relaxed);

        if let Some(handle) = self.monitor_thread.take() {
            let _ = handle.join();
        }

        let _ = self.child.kill();
        let _ = self.child.wait();
        self.stopped = true;
    }

    fn stop_and_collect(mut self) -> Result<ResourceStats> {
        self.terminate();

        let samples = self
            .samples
            .lock()
            .map_err(|_| anyhow::anyhow!("failed to lock Spice samples"))?
            .clone();
        Ok(summarize_samples(&samples))
    }
}

impl Drop for SpiceRuntimeHandle {
    fn drop(&mut self) {
        self.terminate();
    }
}

pub async fn assert_spice_benchmark(
    source: spice_rust_query::SpiceSource,
    kind: BenchmarkKind,
) -> Result<()> {
    let report = spice_rust_query::collect_spice_benchmark(source, kind).await?;
    assert_report(&report, kind);
    Ok(())
}

pub async fn connect_postgres() -> Result<Arc<PgClient>> {
    let connection_string = format!(
        "host={} port={} dbname={} user={} password={} sslmode=disable",
        PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .context("failed to connect to PostgreSQL for native test")?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {error}");
        }
    });
    Ok(Arc::new(client))
}

pub async fn run_postgres_native(kind: BenchmarkKind) -> Result<BenchmarkReport> {
    let client = connect_postgres().await?;
    run_native_benchmark_case(
        kind.name(),
        PERF_ITERATIONS,
        spice_rust_query::PERF_PARALLELISM,
        kind.uses_warmup(),
        |iteration, parallelism| {
        let client = Arc::clone(&client);
        async move {
            let mut tasks = Vec::with_capacity(parallelism);
            for offset in 0..parallelism {
                let client = Arc::clone(&client);
                tasks.push(tokio::spawn(async move {
                    let sequence = iteration * parallelism + offset;
                    let rows = match kind {
                        BenchmarkKind::ExactIpCityNoCache => {
                            let ip = exact_lookup_ip_for_iteration(sequence);
                            client
                                .query(
                                    "SELECT city_name FROM public.attacker_ip_perf WHERE attacker_ip = $1",
                                    &[&ip],
                                )
                                .await?
                        }
                        BenchmarkKind::PartialIpCacheHit => {
                            let ip = cached_hot_ip();
                            client
                                .query(
                                    "SELECT attacker_ip, city_name FROM public.attacker_ip_perf WHERE attacker_ip = $1",
                                    &[&ip],
                                )
                                .await?
                        }
                        BenchmarkKind::NonexistentIp => {
                            client
                                .query(
                                    "SELECT city_name FROM public.attacker_ip_perf WHERE attacker_ip = $1",
                                    &[&NONEXISTENT_IP],
                                )
                                .await?
                        }
                        BenchmarkKind::JoinNationNoCache => {
                            let ip = exact_lookup_ip_for_iteration(sequence);
                            client
                                .query(
                                    "SELECT a.attacker_ip, c.nation FROM public.attacker_ip_perf a JOIN public.city_perf c ON a.city_id = c.id WHERE a.attacker_ip = $1",
                                    &[&ip],
                                )
                                .await?
                        }
                        BenchmarkKind::JoinNationCacheHit => {
                            let ip = cached_exact_lookup_ip();
                            client
                                .query(
                                    "SELECT a.attacker_ip, c.nation FROM public.attacker_ip_perf a JOIN public.city_perf c ON a.city_id = c.id WHERE a.attacker_ip = $1",
                                    &[&ip],
                                )
                                .await?
                        }
                    };
                    Ok::<usize, anyhow::Error>(rows.len())
                }));
            }

            let mut results = Vec::with_capacity(parallelism);
            for task in tasks {
                results.push(task.await??);
            }
            ensure_consistent_rows(&results, iteration)
        }
    },
    )
    .await
}

pub async fn run_clickhouse_native(kind: BenchmarkKind) -> Result<BenchmarkReport> {
    let client = clickhouse::Client::default()
        .with_url(CK_ENDPOINT)
        .with_user(CK_USER)
        .with_password(CK_PASSWORD)
        .with_database(CK_DB);

    run_native_benchmark_case(
        kind.name(),
        PERF_ITERATIONS,
        spice_rust_query::PERF_PARALLELISM,
        kind.uses_warmup(),
        |iteration, parallelism| {
        let client = client.clone();
        async move {
            let mut tasks = Vec::with_capacity(parallelism);
            for offset in 0..parallelism {
                let client = client.clone();
                tasks.push(tokio::spawn(async move {
                    let sequence = iteration * parallelism + offset;
                    let rows = match kind {
                        BenchmarkKind::ExactIpCityNoCache => {
                            let ip = exact_lookup_ip_for_iteration(sequence);
                            let rows: Vec<CkCityNameRow> = client
                                .query("SELECT city_name FROM attacker_ip_perf WHERE attacker_ip = ?")
                                .bind(ip)
                                .fetch_all()
                                .await?;
                            rows.len()
                        }
                        BenchmarkKind::PartialIpCacheHit => {
                            let ip = cached_hot_ip();
                            let rows: Vec<CkAttackerIpRow> = client
                                .query(
                                    "SELECT ?fields FROM attacker_ip_perf WHERE attacker_ip = ?",
                                )
                                .bind(ip)
                                .fetch_all()
                                .await?;
                            rows.len()
                        }
                        BenchmarkKind::NonexistentIp => {
                            let rows: Vec<CkCityNameRow> = client
                                .query("SELECT city_name FROM attacker_ip_perf WHERE attacker_ip = ?")
                                .bind(NONEXISTENT_IP)
                                .fetch_all()
                                .await?;
                            rows.len()
                        }
                        BenchmarkKind::JoinNationNoCache => {
                            let ip = exact_lookup_ip_for_iteration(sequence);
                            let rows: Vec<CkNationRow> = client
                                .query(
                                    "SELECT ?fields FROM attacker_ip_perf a JOIN city_perf c ON a.city_id = c.id WHERE a.attacker_ip = ?",
                                )
                                .bind(ip)
                                .fetch_all()
                                .await?;
                            rows.len()
                        }
                        BenchmarkKind::JoinNationCacheHit => {
                            let rows: Vec<CkNationRow> = client
                                .query(
                                    "SELECT ?fields FROM attacker_ip_perf a JOIN city_perf c ON a.city_id = c.id WHERE a.attacker_ip = ?",
                                )
                                .bind(cached_exact_lookup_ip())
                                .fetch_all()
                                .await?;
                            rows.len()
                        }
                    };
                    Ok::<usize, anyhow::Error>(rows)
                }));
            }

            let mut results = Vec::with_capacity(parallelism);
            for task in tasks {
                results.push(task.await??);
            }
            ensure_consistent_rows(&results, iteration)
        }
    },
    )
    .await
}

pub async fn run_csv_native(kind: BenchmarkKind) -> Result<BenchmarkReport> {
    let (attackers, cities) = load_csv_data()?;
    let attackers = Arc::new(attackers);
    let cities = Arc::new(cities);

    run_native_benchmark_case(
        kind.name(),
        PERF_ITERATIONS,
        spice_rust_query::PERF_PARALLELISM,
        kind.uses_warmup(),
        |iteration, parallelism| {
        let attackers = Arc::clone(&attackers);
        let cities = Arc::clone(&cities);
        async move {
            let mut tasks = Vec::with_capacity(parallelism);
            for offset in 0..parallelism {
                let attackers = Arc::clone(&attackers);
                let cities = Arc::clone(&cities);
                tasks.push(tokio::spawn(async move {
                    let sequence = iteration * parallelism + offset;
                    let rows = match kind {
                        BenchmarkKind::ExactIpCityNoCache => {
                            let ip = exact_lookup_ip_for_iteration(sequence);
                            attackers.iter().filter(|row| row.attacker_ip == ip).count()
                        }
                        BenchmarkKind::PartialIpCacheHit => attackers
                            .iter()
                            .filter(|row| row.attacker_ip == cached_hot_ip())
                            .count(),
                        BenchmarkKind::NonexistentIp => attackers
                            .iter()
                            .filter(|row| row.attacker_ip == NONEXISTENT_IP)
                            .count(),
                        BenchmarkKind::JoinNationNoCache => {
                            let ip = exact_lookup_ip_for_iteration(sequence);
                            attackers
                                .iter()
                                .filter(|row| row.attacker_ip == ip)
                                .filter(|row| cities.contains_key(&row.city_id))
                                .count()
                        }
                        BenchmarkKind::JoinNationCacheHit => attackers
                            .iter()
                            .filter(|row| row.attacker_ip == cached_exact_lookup_ip())
                            .filter(|row| cities.contains_key(&row.city_id))
                            .count(),
                    };
                    Ok::<usize, anyhow::Error>(rows)
                }));
            }

            let mut results = Vec::with_capacity(parallelism);
            for task in tasks {
                results.push(task.await??);
            }
            ensure_consistent_rows(&results, iteration)
        }
    },
    )
    .await
}
pub fn load_csv_data() -> Result<(Vec<CsvAttackerRow>, HashMap<u64, CsvCityRow>)> {
    let attacker_content = fs::read_to_string(CSV_ATTACKER_IP_FILE)
        .with_context(|| format!("failed to read {}", CSV_ATTACKER_IP_FILE))?;
    let city_content =
        fs::read_to_string(CSV_CITY_FILE).with_context(|| format!("failed to read {}", CSV_CITY_FILE))?;

    let attackers = attacker_content
        .lines()
        .skip(1)
        .map(|line| {
            let parts: Vec<&str> = line.split(',').collect();
            CsvAttackerRow {
                id: parts[0].parse().unwrap(),
                attacker_ip: parts[1].to_string(),
                city_id: parts[2].parse().unwrap(),
                city_name: parts[3].to_string(),
            }
        })
        .collect();

    let cities = city_content
        .lines()
        .skip(1)
        .map(|line| {
            let parts: Vec<&str> = line.split(',').collect();
            let id = parts[0].parse::<u64>().unwrap();
            (
                id,
                CsvCityRow {
                    id,
                    city_name: parts[1].to_string(),
                    nation: parts[2].to_string(),
                },
            )
        })
        .collect();

    Ok((attackers, cities))
}

pub fn print_header(source: &str, mode: &str, kind: BenchmarkKind) {
    println!(
        "数据源：{source} | 查询方式：{} | 测试项：{} | 迭代次数：{} | 并行度：{} | 数据规模：{} | 示例城市：{}",
        if mode == "spice" { "Spice" } else { "原生 SDK" },
        spice_rust_query::benchmark_kind_zh(kind.name()),
        PERF_ITERATIONS,
        spice_rust_query::PERF_PARALLELISM,
        PERF_ROW_COUNT,
        city_name(1)
    );
}

pub fn print_suite_header(source: &str, mode: &str) {
    println!(
        "数据源：{source} | 查询方式：{} | 测试总数：5 | 迭代次数：{} | 并行度：{} | 数据规模：{} | 热点 IP 池大小：{} | 示例城市：{}",
        if mode == "spice" { "Spice" } else { "原生 SDK" },
        PERF_ITERATIONS,
        spice_rust_query::PERF_PARALLELISM,
        PERF_ROW_COUNT,
        HOT_IP_POOL_SIZE,
        city_name(1)
    );
}

pub fn benchmark_kinds() -> &'static [BenchmarkKind] {
    &ALL_KINDS
}

fn suite_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub fn acquire_suite_lock() -> MutexGuard<'static, ()> {
    suite_lock().lock().expect("failed to lock test suite mutex")
}

pub async fn restart_docker_compose() -> Result<()> {
    let status = Command::new("docker")
        .args(["compose", "-f", DOCKER_COMPOSE_FILE, "restart"])
        .status()
        .context("failed to run docker compose restart")?;

    if !status.success() {
        return Err(anyhow::anyhow!(
            "docker compose restart failed with status: {status}"
        ));
    }

    wait_for_sources_ready().await?;
    Ok(())
}

pub async fn run_spice_suite(source: spice_rust_query::SpiceSource) -> Result<()> {
    let mut summaries = Vec::new();
    for kind in benchmark_kinds() {
        restart_docker_compose().await?;
        sleep(Duration::from_secs(3));
        let runtime = start_spice_runtime(*kind)?;
        let report = spice_rust_query::collect_spice_benchmark(source, *kind).await?;
        assert_report(&report, *kind);
        let resource_stats = runtime.stop_and_collect()?;
        summaries.push(SpiceCaseSummary {
            kind: *kind,
            report,
            resource_stats,
        });
    }

    let total_sample_count: usize = summaries
        .iter()
        .map(|summary| summary.resource_stats.sample_count)
        .sum();
    let weighted_avg_cpu: f64 = if total_sample_count == 0 {
        0.0
    } else {
        summaries
            .iter()
            .map(|summary| summary.resource_stats.avg_cpu_percent * summary.resource_stats.sample_count as f64)
            .sum::<f64>()
            / total_sample_count as f64
    };
    let weighted_avg_memory_mb: f64 = if total_sample_count == 0 {
        0.0
    } else {
        summaries
            .iter()
            .map(|summary| summary.resource_stats.avg_memory_mb * summary.resource_stats.sample_count as f64)
            .sum::<f64>()
            / total_sample_count as f64
    };
    let total_stats = ResourceStats {
        sample_count: total_sample_count,
        avg_cpu_percent: weighted_avg_cpu,
        peak_cpu_percent: summaries
            .iter()
            .map(|summary| summary.resource_stats.peak_cpu_percent)
            .fold(0.0, f64::max),
        avg_memory_mb: weighted_avg_memory_mb,
        peak_memory_mb: summaries
            .iter()
            .map(|summary| summary.resource_stats.peak_memory_mb)
            .fold(0.0, f64::max),
    };
    print_and_write_spice_suite_report(source.label, &summaries, &total_stats)
}

pub async fn run_native_suite_postgres() -> Result<()> {
    let mut summaries = Vec::new();
    for kind in benchmark_kinds() {
        restart_docker_compose().await?;
        let report = run_postgres_native(*kind).await?;
        assert_report(&report, *kind);
        summaries.push(NativeCaseSummary { kind: *kind, report });
    }
    print_and_write_native_suite_report("postgres", &summaries)
}

pub async fn run_native_suite_clickhouse() -> Result<()> {
    let mut summaries = Vec::new();
    for kind in benchmark_kinds() {
        restart_docker_compose().await?;
        let report = run_clickhouse_native(*kind).await?;
        assert_report(&report, *kind);
        summaries.push(NativeCaseSummary { kind: *kind, report });
    }
    print_and_write_native_suite_report("clickhouse", &summaries)
}

pub async fn run_native_suite_csv() -> Result<()> {
    let mut summaries = Vec::new();
    for kind in benchmark_kinds() {
        let report = run_csv_native(*kind).await?;
        assert_report(&report, *kind);
        summaries.push(NativeCaseSummary { kind: *kind, report });
        // restart_docker_compose()?;
    }
    print_and_write_native_suite_report("csv", &summaries)
}

pub fn build_spice_sql_preview(source: spice_rust_query::SpiceSource, kind: BenchmarkKind) -> String {
    build_spice_query(kind, source.attacker_dataset, source.city_dataset, 0)
}
