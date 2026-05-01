use std::ffi::OsString;
use std::path::PathBuf;

use clap::Parser;
use tracing::Level;
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

    /// Enable tracing output.
    #[arg(long, default_value_t = false)]
    pub tracing: bool,

    /// Log level when --tracing is enabled (trace, debug, info, warn, error).
    #[arg(long, default_value_t = Level::INFO, requires = "tracing")]
    pub log_level: Level,
}

pub async fn run(args: Cli) -> anyhow::Result<()> {
    if args.tracing {
        init_tracing(args.log_level);
    }
    tracing::debug!(input = ?args.input, "starting csv-ingestor");

    let processor = transaction_processor::TransactionProcessor::builder()
        .build()
        .await?;
    processor.ingest_csv(&args.input).await?;

    // Output accounts in CSV format to stdout, bypassing any structured logging that might be enabled
    output_csv_header();

    let mut page = 0;
    let page_size = 512;
    loop {
        let accounts = processor.snapshot_accounts(page, page_size).await?;
        if accounts.is_empty() {
            break;
        }
        output_formatted_accounts(&accounts);
        page += 1;
    }

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

fn output_csv_header() {
    println!("client,available,held,total,locked");
}

fn output_formatted_accounts(accounts: &[transaction_processor::Account]) {
    for account in accounts {
        println!(
            "{},{},{},{},{}",
            account.client_id,
            account.available.round_dp(4).normalize().to_string(),
            account.held.round_dp(4).normalize().to_string(),
            account.total.round_dp(4).normalize().to_string(),
            account.locked
        );
    }
}

fn init_tracing(level: Level) {
    let filter = EnvFilter::new(level.as_str());
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
