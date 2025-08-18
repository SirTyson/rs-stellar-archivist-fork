use stellar_archivist::{
    history_file,
    mirror_operation::MirrorOperation,
    pipeline::{Pipeline, PipelineConfig},
    scan_operation::ScanOperation,
};

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::sync::Arc;
use tracing::{debug, info, warn, Level};
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

    /// Maximum number of HTTP retry attempts
    #[arg(long, global = true, default_value_t = 3)]
    max_retries: u32,

    /// Initial backoff in milliseconds for HTTP retries
    #[arg(long, global = true, default_value_t = 100)]
    initial_backoff_ms: u64,
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
            run_mirror(
                src,
                dst,
                cli.concurrency,
                cli.skip_optional,
                low,
                high,
                overwrite,
                allow_mirror_gaps,
                cli.max_retries,
                cli.initial_backoff_ms,
            )
            .await?;
        }
        Commands::Scan { archive, low, high } => {
            run_scan(
                archive,
                cli.concurrency,
                cli.skip_optional,
                low,
                high,
                cli.max_retries,
                cli.initial_backoff_ms,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_scan(
    archive: String,
    concurrency: usize,
    skip_optional: bool,
    low: Option<u32>,
    high: Option<u32>,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<()> {
    info!("Starting scan of {}", archive);

    if let Some(low) = low {
        info!("Scanning from ledger {} onwards", low);
    }
    if let Some(high) = high {
        info!("Scanning up to checkpoint {}", high);
    }

    // Create the scan operation
    let operation = ScanOperation::new().await?;

    // Configure the pipeline with low/high bounds and retry config
    let pipeline_config = PipelineConfig {
        source: archive.clone(),
        concurrency,
        skip_optional,
        low,
        high,
        max_retries,
        initial_backoff_ms,
    };

    // Create and run the pipeline
    let pipeline = Arc::new(Pipeline::new(operation, pipeline_config).await?);
    pipeline.run().await?;

    Ok(())
}

async fn run_mirror(
    src: String,
    dst: String,
    concurrency: usize,
    skip_optional: bool,
    low: Option<u32>,
    high: Option<u32>,
    overwrite: bool,
    allow_mirror_gaps: bool,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<()> {
    info!(
        "Starting mirror from {} to {} with {} workers",
        src, dst, concurrency
    );

    // Check destination URL is supported (must be file://)
    if !dst.starts_with("file://") {
        if dst.starts_with("http://") || dst.starts_with("https://") {
            anyhow::bail!("Destination must be a filesystem path (file://). HTTP/HTTPS destinations are read-only.");
        } else if dst.starts_with("s3://") {
            anyhow::bail!("S3 support not yet implemented for destinations.");
        } else {
            anyhow::bail!(
                "Unsupported URL scheme for destination. Must use file:// for filesystem paths."
            );
        }
    }

    let operation = MirrorOperation::new(&dst, overwrite).await?;

    // Check if destination has an existing HAS file to resume from.
    // If we do, check that the --low flag won't create a "gap" in the archive.
    let mut effective_low = low;
    if let Some(requested_low) = low {
        match operation.get_latest_dest_checkpoint().await {
            Ok(dest_ledger) => {
                let dest_checkpoint = history_file::checkpoint_number(dest_ledger);
                let requested_checkpoint = history_file::checkpoint_number(requested_low);

                // Check if destination's last checkpoint is >= requested low
                if dest_checkpoint < requested_checkpoint {
                    // There would be a gap!
                    if !allow_mirror_gaps {
                        anyhow::bail!(
                            "Cannot mirror: destination archive ends at ledger {} (checkpoint 0x{:08x}) but --low is {} (checkpoint 0x{:08x}). This would create a gap in the archive. Use --allow-mirror-gaps to proceed anyway.",
                            dest_ledger, dest_checkpoint, requested_low, requested_checkpoint
                        );
                    } else {
                        warn!(
                            "WARNING: Creating gap in archive! Destination ends at ledger {} (checkpoint 0x{:08x}) but mirroring from {} (checkpoint 0x{:08x})",
                            dest_ledger, dest_checkpoint, requested_low, requested_checkpoint
                        );
                    }
                } else {
                    info!(
                        "Destination archive at ledger {} is compatible with --low {}",
                        dest_ledger, requested_low
                    );
                }
            }
            Err(e) => {
                debug!(
                    "Could not read destination HAS: {}, proceeding with --low {}",
                    e, requested_low
                );
            }
        }
        // Use the explicit --low value
        effective_low = Some(requested_low);
    } else if !overwrite {
        // No --low specified and not overwriting, try to resume from destination
        match operation.get_latest_dest_checkpoint().await {
            Ok(checkpoint_ledger) => {
                info!(
                    "Found existing archive at destination with ledger {}, resuming from next checkpoint",
                    checkpoint_ledger
                );
                // Resume from the next checkpoint after what's already mirrored
                effective_low = Some(checkpoint_ledger + 1);
            }
            Err(e) => {
                debug!(
                    "Could not read destination HAS: {}, starting from beginning",
                    e
                );
            }
        }
    } else {
        // No --low specified but --overwrite is set, start from beginning
        debug!("Overwrite mode enabled, starting from beginning");
    }

    // Configure the pipeline with retry config
    let pipeline_config = PipelineConfig {
        source: src.clone(),
        concurrency,
        skip_optional,
        low: effective_low,
        high,
        max_retries,
        initial_backoff_ms,
    };

    // Create and run the pipeline
    let pipeline = Arc::new(Pipeline::new(operation, pipeline_config).await?);
    pipeline.run().await?;

    Ok(())
}
