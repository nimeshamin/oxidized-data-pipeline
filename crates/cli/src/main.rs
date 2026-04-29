#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::run_from_args(std::env::args_os()).await
}
