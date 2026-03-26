# spice_rust_query

`spice_rust_query` 是一个 Rust 基准测试与示例工程，用来比较 3 类数据源在 `Spice` 和原生 SDK 下的查询表现：

- `PostgreSQL`
- `ClickHouse`
- `CSV`

项目主要包含两部分：

- 一个简单的 `Spice Flight` 查询示例程序
- 一套对比 `Spice` / 原生查询的集成测试与数据准备脚本

## 项目结构
- `report/`：测试报告输出目录，包含每次测试的 Markdown 格式报告
- `src/lib.rs`：共享常量、查询构造、benchmark 核心逻辑
- `src/main.rs`：最小查询示例程序:需要提前准备数据，并启动 Spice
- `examples/seed_pg_perf.rs`：写入 PostgreSQL 基准数据
- `examples/seed_ck_perf.rs`：写入 ClickHouse 基准数据
- `examples/seed_csv_perf.rs`：生成 CSV 基准数据
- `tests/postgres.rs`：PostgreSQL 集成测试入口
- `tests/clickhouse.rs`：ClickHouse 集成测试入口
- `tests/csv.rs`：CSV 集成测试入口
- `tests/common/mod.rs`：测试公共逻辑、Spice 生命周期管理、报告输出
- `spice/perf/spicepod.yaml`：当前测试实际使用的 Spice 配置
- `component/docker-compose.yml`：本地 PostgreSQL / ClickHouse 容器
- `.run/report/`：测试报告输出目录
- `.run/spice_logs/`：Spice 运行日志

## 测试场景

每个数据源都覆盖 5 类查询：

1. 任意 IP 查询城市，避免缓存
2. 热点 IP 查询，命中缓存
3. 查询不存在的 IP
4. IP 对应国家 join 查询，避免缓存
5. IP 对应国家 join 查询，命中缓存

固定假设：

- 主表数据量：`1000w`
- 城市表数据量：`10w`
- 单次查询预期返回：`0` 或 `1` 行

## 运行前提

本项目依赖以下工具：

- Rust / Cargo
- `spice` CLI
- Docker + `docker compose`
- `curl`
- `lsof`

如果要跑完整测试，请确保本机端口未被占用：

- `5432` PostgreSQL
- `8123` ClickHouse HTTP
- `9000` ClickHouse TCP
- `8090` Spice HTTP
- `50012` Spice Flight

## 关键配置

核心连接配置在 `src/lib.rs`：

- `SPICE_FLIGHT_URL`
- `PG_HOST` / `PG_PORT` / `PG_DB` / `PG_USER` / `PG_PASSWORD`
- `CK_ENDPOINT` / `CK_HOST` / `CK_TCP_PORT` / `CK_DB` / `CK_USER` / `CK_PASSWORD`
- `CSV_OUTPUT_DIR` / `CSV_ATTACKER_IP_FILE` / `CSV_CITY_FILE`
- `PERF_ROW_COUNT` / `PERF_CITY_COUNT` / `PERF_BATCH_SIZE`

当前默认值：

- PostgreSQL：`127.0.0.1:5432/postgres`，用户 `root`，密码 `123456`
- ClickHouse HTTP：`http://127.0.0.1:8123`
- ClickHouse TCP：`127.0.0.1:9000`
- ClickHouse DB：`test_db`
- CSV 目录：`.run/data/csv`
- Spice Flight：`http://127.0.0.1:50012`

## 哪些地方通常需要改

如果你不是直接复用作者本机环境，通常至少要检查这些地方：

- `src/lib.rs` 里的数据库地址、用户名、密码、数据库名
- `spice/perf/spicepod.yaml` 里的 `pg_*` / `clickhouse_*` 连接参数
- `spice/perf/spicepod.yaml` 里的 CSV 文件相对路径
- `tests/postgres.rs`、`tests/clickhouse.rs`、`tests/csv.rs` 里的：
  - `TEST_ITERATIONS`
  - `TEST_PARALLELISM`
  - `TEST_WORKER_THREADS`
- `component/docker-compose.yml` 里的端口映射与容器环境变量

尤其注意下面几个容易踩坑的点：

- 当前测试真正使用的是 `spice/perf/spicepod.yaml`，不是 `spice/perf_spicepod.yaml`
- `spice/perf/spicepod.yaml` 的 CSV 路径是 `file:../../.run/data/csv/...`
- `spice/perf_spicepod.yaml` 里还是旧路径 `file:../generated/csv/...`，如果你用这份配置，需要同步改路径
- `src/main.rs` 的默认 SQL 仍然是 `taxi_trips` 示例查询，如果你直接运行 `cargo run`，大概率需要改成当前数据集对应的 SQL
- `component/docker-compose.yml` 里 PostgreSQL 容器环境变量是 `POSTGRES_DB=default`，但代码和 spicepod 用的是 `postgres`；如果你的环境里没有默认 `postgres` 数据库，需要统一配置

## 启动依赖容器

在项目根目录执行：

```bash
docker compose -f component/docker-compose.yml up -d
```

## 生成基准数据

建议按顺序执行：

```bash
cargo run --example seed_pg_perf
cargo run --example seed_ck_perf
cargo run --example seed_csv_perf
```

数据生成完成后：

- PostgreSQL 表：`public.attacker_ip_perf`、`public.city_perf`
- ClickHouse 表：`test_db.attacker_ip_perf`、`test_db.city_perf`
- CSV 文件：`.run/data/csv/attacker_ip_perf.csv`、`.run/data/csv/city_perf.csv`

## 启动 Spice

当前测试默认从 `spice/perf/` 目录启动 Spice：

```bash
spice run
```

如果你手动启动，建议先进入：

```bash
cd spice/perf
spice run
```

测试代码会自动管理 Spice 进程；如果你只是想手工验证查询，可以自己单独启动。

## 运行示例查询

直接运行：

```bash
cargo run -- "SELECT city_name FROM pg_attacker_ip_perf LIMIT 5;"
```

说明：

- `src/main.rs` 默认查询仍是 `taxi_trips`
- 更推荐显式传入 SQL，而不是直接用默认值

## 运行测试

每个数据源入口文件各有 2 个测试：

- `spice_all_queries`
- `native_all_queries`

常用命令：

```bash
cargo test --test postgres -- --nocapture
cargo test --test clickhouse -- --nocapture
cargo test --test csv -- --nocapture
```

只跑单个测试：

```bash
cargo test --package spice_rust_query --test clickhouse -- spice_all_queries --exact --nocapture
cargo test --package spice_rust_query --test postgres -- native_all_queries --exact --nocapture
```

## 测试参数从哪里改

现在测试次数和并发度是从入口文件显式传入，而不是只依赖 `src/lib.rs` 中的全局常量。

也就是说，如果你想调整测试强度，优先改这里：

- `tests/postgres.rs`
- `tests/clickhouse.rs`
- `tests/csv.rs`

每个入口里都有：

```rust
const TEST_ITERATIONS: usize = ...;
const TEST_PARALLELISM: usize = ...;
```

## 报告与日志

测试输出会落到：

- 报告：`.run/report/*.md`
- Spice 日志：`.run/spice_logs/*`

如果测试异常，优先看：

- `.run/spice_logs/<case>_stdout.log`
- `.run/spice_logs/<case>_stderr.log`

## 项目 review 结论

这个项目现在已经可以稳定承担“Spice vs 原生查询”的本地验证，但有几个点需要明确：

- 优点
  - 测试入口清晰，数据准备和测试职责分离
  - Spice 生命周期管理已经比之前严格，失败时也会尝试清理进程
  - 报告输出统一，便于比较不同数据源
- 当前局限
  - 监控的是 `Spice` 进程本身，不是整条链路的资源占用
  - CSV、Spice、docker-compose 里仍有路径和默认配置耦合，本机迁移时需要人工检查
  - `src/main.rs` 仍是通用 demo，不完全贴合这个 benchmark 项目
  - `spice/perf_spicepod.yaml` 和 `spice/perf/spicepod.yaml` 还没有完全收敛到一套路径约定

如果后面继续维护，优先建议做这几件事：

1. 统一 `spicepod` 路径配置，删掉旧版 CSV 路径
2. 把数据库连接参数和测试参数收敛成环境变量或单独配置文件
3. 把 `src/main.rs` 的默认 SQL 改成当前项目真实可跑的查询
4. 如果要做性能结论，补充对 PostgreSQL / ClickHouse 进程本身的监控
