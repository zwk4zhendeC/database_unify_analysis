# spice_rust_query

Rust 示例与测试工程：连接 Spice runtime、准备性能测试数据，并对 `PostgreSQL`、`ClickHouse`、`CSV` 做 `Spice` 与原生查询对比。

## 测试场景
1. 频繁查询不同的单条（避免缓存）
2. 查询固定的单条（命中缓存）
3. 频繁查询不存在的内容
4. 查询不同的join（避免缓存）
5. 查询固定的一些join（命中缓存）

- 查询参数：
    - 数据总量: 主表1000w，副表10w（用于表连接）
    - 查询次数：1w次，ck是1k次
    - 并行度: 1、4
    - 返回条数：1

## 运行前提

先启动 Spice runtime，例如在你的 Spice 项目目录里：

```bash
spice run
```

当前代码固定连接 `spice_rust_query/src/lib.rs` 中的 `SPICE_FLIGHT_URL`，默认值是 `http://127.0.0.1:50012`。

## 运行

使用默认查询：

```bash
cargo run
```

自定义查询：

```bash
cargo run -- "SELECT * FROM taxi_trips LIMIT 5;"
```

## 大数据准备 examples

项目内新增了 3 个用于准备性能测试数据的 examples：

```bash
cargo run --example seed_pg_perf
cargo run --example seed_ck_perf
cargo run --example seed_csv_perf
```

所有参数都已经改成全局常量，定义在 `spice_rust_query/src/lib.rs`：

- `PERF_ROW_COUNT`
- `PERF_CITY_COUNT`
- `PERF_BATCH_SIZE`
- `PG_HOST` / `PG_PORT` / `PG_DB` / `PG_USER` / `PG_PASSWORD`
- `CK_ENDPOINT` / `CK_DB` / `CK_USER` / `CK_PASSWORD`
- `CSV_OUTPUT_DIR`

数据库默认连接参数：

- PostgreSQL：`127.0.0.1:5432/postgres root/123456`
- ClickHouse：`http://127.0.0.1:8123 test_db default/default`
- CSV 输出目录：`generated/csv`

## 性能测试

性能测试已经迁移到 `tests` 目录，以 `cargo test` 方式运行。

当前测试文件：

- `tests/postgres.rs`
- `tests/clickhouse.rs`
- `tests/csv.rs`

每个数据源文件里都包含 10 个测试：

- 5 个 `Spice` 查询测试
- 5 个原生 SDK 查询测试

每类数据源都包含 5 类测试：

1. 任意 IP 查询城市（避免缓存）
2. 查询部分 IP（命中缓存）
3. 查询不存在的 IP
4. 查询 IP 对应国家，表连接（避免缓存）
5. 查询 IP 对应国家，表连接（命中缓存）

常用运行方式：

```bash
cargo test --test postgres -- --nocapture
cargo test --test clickhouse -- --nocapture
cargo test --test csv -- --nocapture
```

性能测试次数和 Spice 连接地址都改成了全局常量：

- `PERF_ITERATIONS`
- `SPICE_FLIGHT_URL`

## 对应 Spice 配置

已经提供了两份等价的 Spice 配置：

- `spice/perf_spicepod.yaml`
- `spice/perf/spicepod.yaml`

如果你想直接进入目录运行 `spice run`，优先使用 `spice/perf/spicepod.yaml` 所在目录。

## 当前目录约定

- `examples/`：仅保留数据准备脚本
- `tests/`：正式性能测试
- `src/lib.rs`：全局常量、查询构造与共享基准逻辑
