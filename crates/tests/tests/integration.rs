//! Placeholder integration tests.
//!
//! Two scenarios are covered:
//!   1. Drive the cli's entry point with parsed args.
//!   2. Use `transaction-processor` directly through its public API.
//!
//! Each test wraps its body in `MetricsGuard`, which prints elapsed time and
//! peak heap usage at drop. Run with `--test-threads=1` if you want per-test
//! peak isolation (heap stats are process-global).

use std::path::{Path, PathBuf};

use tests::{metrics::PEAK_ALLOC, MetricsGuard};
use tracing_subscriber::EnvFilter;
use transaction_processor::TransactionProcessor;

#[global_allocator]
static GLOBAL: peak_alloc::PeakAlloc = PEAK_ALLOC;

fn init_tracing(debug: bool) {
    let filter = if debug {
        EnvFilter::new("debug")
    } else {
        EnvFilter::new("warn")
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init();
}

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
async fn transaction_processor_direct_use_simple_single_worker() -> anyhow::Result<()> {
    let _m = MetricsGuard::new("transaction_processor_direct_use_simple_single_worker");
    init_tracing(true);

    let processor = TransactionProcessor::builder()
        .parallelism(1)
        .channel_capacity(2)
        .build()
        .await?;

    assert_eq!(processor.parallelism(), 1);
    assert_eq!(processor.channel_capacity(), 2);

    let idx_a = processor.route(2341);
    let idx_b = processor.route(2341);
    assert_eq!(idx_a, idx_b, "router must be deterministic");
    assert!(idx_a < 1);

    let result = processor.ingest_csv(Path::new("crates/tests/tests/data/test_input_simple.csv")).await;
    if result.is_err() {
        eprintln!("ingest_csv error: {:?}", result.as_ref().err());
    }
    assert!(result.is_ok(), "ingest_csv should succeed with valid input");

    let accounts = processor.snapshot_accounts(0, 1).await?;
    assert!(!accounts.is_empty(), "snapshot_accounts should return some accounts");
    // Loop until there are no more items. Print each account as a line of output
    let mut page = 0;
    let page_size = 1;
    loop {
        let accounts = processor.snapshot_accounts(page, page_size).await?;
        if accounts.is_empty() {
            break;
        }
        for account in accounts {
            println!("{:?}", account);
        }
        page += 1;
    }
    
    processor.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn transaction_processor_direct_use() -> anyhow::Result<()> {
    let _m = MetricsGuard::new("transaction_processor_direct_use");
    init_tracing(true);

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
