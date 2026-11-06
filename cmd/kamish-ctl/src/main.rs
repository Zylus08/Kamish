use clap::Parser;
use kamish_ctl::{Cli, run_command};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    run_command(cli.command).await
}
