mod common;

use anyhow::Result;
use common::{acquire_suite_lock, run_native_suite_csv, run_spice_suite};
use spice_rust_query::SPICE_SOURCE_CSV;

const TEST_WORKER_THREADS: usize = 4;

#[test]
fn spice_all_queries() -> Result<()> {
    let _guard = acquire_suite_lock();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(TEST_WORKER_THREADS)
        .enable_all()
        .build()?
        .block_on(async { run_spice_suite(SPICE_SOURCE_CSV).await })
}

#[test]
fn native_all_queries() -> Result<()> {
    let _guard = acquire_suite_lock();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(TEST_WORKER_THREADS)
        .enable_all()
        .build()?
        .block_on(async { run_native_suite_csv().await })
}
