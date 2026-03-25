mod common;

use anyhow::Result;
use common::{acquire_suite_lock, run_native_suite_postgres, run_spice_suite};
use spice_rust_query::SPICE_SOURCE_PG;

const TEST_WORKER_THREADS: usize = 4;


#[test]
fn native_all_queries() -> Result<()> {
    let _guard = acquire_suite_lock();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(TEST_WORKER_THREADS)
        .enable_all()
        .build()?
        .block_on(async { run_native_suite_postgres().await })
}

#[test]
fn spice_all_queries() -> Result<()> {
    let _guard = acquire_suite_lock();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(TEST_WORKER_THREADS)
        .enable_all()
        .build()?
        .block_on(async { run_spice_suite(SPICE_SOURCE_PG).await })
}

