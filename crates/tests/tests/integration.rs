//! Placeholder integration tests.
//!
//! Two scenarios are covered:
//!   1. Drive the cli's entry point with parsed args.
//!   2. Use `transaction-processor` directly through its public API.
//!
//! Each test wraps its body in `MetricsGuard`, which prints elapsed time and
//! peak heap usage at drop. Run with `--test-threads=1` if you want per-test
//! peak isolation (heap stats are process-global).

use std::path::PathBuf;

use tests::{metrics::PEAK_ALLOC, MetricsGuard};
use transaction_processor::TransactionProcessor;

#[global_allocator]
static GLOBAL: peak_alloc::PeakAlloc = PEAK_ALLOC;

#[tokio::test]
async fn cli_main_runs_with_input_file() -> anyhow::Result<()> {
    let _m = MetricsGuard::new("cli_main_runs_with_input_file");

    let path = write_sample_csv("cli-main").await?;
    let argv = [
        "csv-ingestor".to_string(),
        path.to_string_lossy().into_owned(),
    ];
    cli::run_from_args(argv).await?;

    Ok(())
}

#[tokio::test]
async fn cli_main_with_debug_flag() -> anyhow::Result<()> {
    let _m = MetricsGuard::new("cli_main_with_debug_flag");

    let path = write_sample_csv("cli-debug").await?;
    let argv = [
        "csv-ingestor".to_string(),
        "--debug".to_string(),
        path.to_string_lossy().into_owned(),
    ];
    cli::run_from_args(argv).await?;

    Ok(())
}

#[tokio::test]
async fn transaction_processor_direct_use() -> anyhow::Result<()> {
    let _m = MetricsGuard::new("transaction_processor_direct_use");

    let processor = TransactionProcessor::builder()
        .parallelism(4)
        .channel_capacity(128)
        .build()
        .await?;

    assert_eq!(processor.parallelism(), 4);
    assert_eq!(processor.channel_capacity(), 128);

    let idx_a = processor.route(42);
    let idx_b = processor.route(42);
    assert_eq!(idx_a, idx_b, "router must be deterministic");
    assert!(idx_a < 4);

    processor.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn transaction_processor_ingests_csv_end_to_end() -> anyhow::Result<()> {
    let _m = MetricsGuard::new("transaction_processor_ingests_csv_end_to_end");

    let path = write_sample_csv("processor-e2e").await?;

    let processor = TransactionProcessor::builder()
        .parallelism(2)
        .channel_capacity(16)
        .build()
        .await?;
    processor.ingest_csv(&path).await?;
    processor.shutdown().await?;

    Ok(())
}

async fn write_sample_csv(tag: &str) -> anyhow::Result<PathBuf> {
    let mut path = std::env::temp_dir();
    path.push(format!("csv-ingestor-{tag}-{}.csv", std::process::id()));
    tokio::fs::write(&path, "id\n1\n2\n3\n4\n5\n").await?;
    Ok(path)
}
