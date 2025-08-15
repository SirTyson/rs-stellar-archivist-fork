use stellar_archivist::{cmd_mirror, cmd_scan};

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

    /// Number of concurrent checkpoints to process
    #[arg(short, long, global = true, default_value_t = 32)]
    concurrency: usize,

    /// Skip optional files (scp, history, results)
    #[arg(long, global = true)]
    skip_optional: bool,

    /// Enable debug logging
    #[arg(long, global = true)]
    debug: bool,

    /// Enable trace logging
    #[arg(long, global = true)]
    trace: bool,

    /// Maximum concurrent HTTP requests (default: 256)
    #[arg(long, global = true, default_value_t = 256)]
    http_connections: usize,

    /// Chunk size for ranged downloads in MB (default: 8)
    #[arg(long, global = true, default_value_t = 8)]
    chunk_size_mb: u64,

    /// Minimum file size for ranged downloads in MB (default: 2)
    #[arg(long, global = true, default_value_t = 2)]
    min_ranged_size_mb: u64,
}

#[derive(Subcommand)]
enum Commands {
    /// Mirror files from source archive to destination
    Mirror {
        /// Source archive URL (http://, https://, file://)
        src: String,
        /// Destination path (must be file://)
        dst: String,

        /// Mirror starting from this ledger (will round to nearest checkpoint)
        #[arg(long)]
        low: Option<u32>,

        /// Mirror up to this checkpoint only
        #[arg(long)]
        high: Option<u32>,

        /// Overwrite existing files within the mirrored range
        #[arg(long)]
        overwrite: bool,

        /// Allow mirroring even when it would create gaps in the destination archive
        #[arg(long, default_value_t = false)]
        allow_mirror_gaps: bool,
    },
    /// Scan archive and verify integrity
    Scan {
        /// Archive URL to scan (http://, https://, file://)
        archive: String,

        /// Scan starting from this ledger (will round to nearest checkpoint)
        #[arg(long)]
        low: Option<u32>,

        /// Scan up to this checkpoint only
        #[arg(long)]
        high: Option<u32>,
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
            low,
            high,
            overwrite,
            allow_mirror_gaps,
        } => {
            let config = cmd_mirror::MirrorConfig {
                src,
                dst,
                concurrency: cli.concurrency,
                skip_optional: cli.skip_optional,
                low,
                high,
                overwrite,
                allow_mirror_gaps,
                max_bucket_cache: None,
                http_connections: cli.http_connections,
            };
            cmd_mirror::run(config).await?;
        }
        Commands::Scan { archive, low, high } => {
            let config = cmd_scan::ScanConfig {
                archive,
                concurrency: cli.concurrency,
                skip_optional: cli.skip_optional,
                http_connections: cli.http_connections,
                low,
                high,
            };
            cmd_scan::run(config).await?;
        }
    }

    Ok(())
}
