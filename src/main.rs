mod archive;
mod cmd_mirror;
mod cmd_scan;
mod history_archive;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "stellar-archivist")]
#[command(about = "Stellar History Archive tools and utilities", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, global = true, default_value_t = 32)]
    concurrency: usize,

    #[arg(long, global = true)]
    skip_optional: bool,

    #[arg(long, global = true)]
    debug: bool,

    #[arg(long, global = true)]
    trace: bool,
}

#[derive(Subcommand)]
enum Commands {
    Mirror {
        src: String,
        dst: String,

        #[arg(long)]
        high: Option<u32>,

        #[arg(long)]
        force: bool,
    },
    Scan {
        archive: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let log_level = if cli.trace {
        Level::TRACE
    } else if cli.debug {
        Level::DEBUG
    } else {
        Level::INFO
    };

    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    match cli.command {
        Commands::Mirror {
            src,
            dst,
            high,
            force,
        } => {
            let config = cmd_mirror::MirrorConfig {
                src,
                dst,
                concurrency: cli.concurrency,
                skip_optional: cli.skip_optional,
                high,
                force,
                window_size: None,
                max_bucket_cache: None,
                window_workers: None,
            };
            cmd_mirror::run(config).await?;
        }
        Commands::Scan { archive } => {
            let config = cmd_scan::ScanConfig {
                archive,
                concurrency: cli.concurrency,
                skip_optional: cli.skip_optional,
            };
            cmd_scan::run(config).await?;
        }
    }

    Ok(())
}
