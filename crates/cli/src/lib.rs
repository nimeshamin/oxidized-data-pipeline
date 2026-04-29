use std::ffi::OsString;
use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "csv-ingestor",
    version,
    about = "Ingest a CSV file via the transaction-processor pipeline"
)]
pub struct Cli {
    /// Input CSV filename (positional, required).
    pub input: PathBuf,

    /// Set log level to debug (default is warn).
    #[arg(long, default_value_t = false)]
    pub debug: bool,
}

pub async fn run(args: Cli) -> anyhow::Result<()> {
    init_tracing(args.debug);
    tracing::debug!(input = ?args.input, "starting csv-ingestor");

    let processor = transaction_processor::TransactionProcessor::builder()
        .build()
        .await?;
    processor.ingest_csv(&args.input).await?;
    processor.shutdown().await?;
    Ok(())
}

pub async fn run_from_args<I, T>(iter: I) -> anyhow::Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let args = Cli::parse_from(iter);
    run(args).await
}

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
