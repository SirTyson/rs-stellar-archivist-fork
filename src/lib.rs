pub mod history_file;
pub mod mirror_operation;
pub mod pipeline;
pub mod scan_operation;
pub mod storage;
pub mod utils;

// Test helper modules with config structures and run functions
// Available for integration tests
pub mod test_helpers {
    use crate::{
        history_file,
        mirror_operation::MirrorOperation,
        pipeline::{Pipeline, PipelineConfig},
        scan_operation::ScanOperation,
    };
    use anyhow::Result;
    use std::sync::Arc;
    use tracing::{debug, info, warn};

    #[derive(Debug)]
    pub struct ScanConfig {
        pub archive: String,
        pub concurrency: usize,
        pub skip_optional: bool,
        pub low: Option<u32>,
        pub high: Option<u32>,
    }

    pub struct MirrorConfig {
        pub src: String,
        pub dst: String,
        pub concurrency: usize,
        pub skip_optional: bool,
        pub low: Option<u32>,
        pub high: Option<u32>,
        pub overwrite: bool,
        pub allow_mirror_gaps: bool,
        pub max_bucket_cache: Option<usize>,
    }

    pub async fn run_scan(config: ScanConfig) -> Result<()> {
        info!("Starting scan of {}", config.archive);

        if let Some(low) = config.low {
            info!("Scanning from ledger {} onwards", low);
        }
        if let Some(high) = config.high {
            info!("Scanning up to checkpoint {}", high);
        }

        let operation = ScanOperation::new().await?;

        let pipeline_config = PipelineConfig {
            source: config.archive.clone(),
            concurrency: config.concurrency,
            skip_optional: config.skip_optional,
            low: config.low,
            high: config.high,
            max_retries: 3,          // Default value for library usage
            initial_backoff_ms: 100, // Default value for library usage
        };

        let pipeline = Arc::new(Pipeline::new(operation, pipeline_config).await?);
        pipeline.run().await?;

        Ok(())
    }

    pub async fn run_mirror(config: MirrorConfig) -> Result<()> {
        info!(
            "Starting mirror from {} to {} with {} workers",
            config.src, config.dst, config.concurrency
        );

        // Check destination URL is supported (must be file://)
        if !config.dst.starts_with("file://") {
            if config.dst.starts_with("http://") || config.dst.starts_with("https://") {
                anyhow::bail!("Destination must be a filesystem path (file://). HTTP/HTTPS destinations are read-only.");
            } else if config.dst.starts_with("s3://") {
                anyhow::bail!("S3 support not yet implemented for destinations.");
            } else {
                anyhow::bail!("Unsupported URL scheme for destination. Must use file:// for filesystem paths.");
            }
        }

        let operation = MirrorOperation::new(&config.dst, config.overwrite).await?;

        let mut effective_low = config.low;
        if let Some(requested_low) = config.low {
            match operation.get_latest_dest_checkpoint().await {
                Ok(dest_ledger) => {
                    let dest_checkpoint = history_file::checkpoint_number(dest_ledger);
                    let requested_checkpoint = history_file::checkpoint_number(requested_low);

                    if dest_checkpoint < requested_checkpoint {
                        if !config.allow_mirror_gaps {
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
            effective_low = Some(requested_low);
        } else if !config.overwrite {
            match operation.get_latest_dest_checkpoint().await {
                Ok(checkpoint_ledger) => {
                    info!(
                        "Found existing archive at destination with ledger {}, resuming from next checkpoint",
                        checkpoint_ledger
                    );
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
            debug!("Overwrite mode enabled, starting from beginning");
        }

        let pipeline_config = PipelineConfig {
            source: config.src.clone(),
            concurrency: config.concurrency,
            skip_optional: config.skip_optional,
            low: effective_low,
            high: config.high,
            max_retries: 3,          // Default value for library usage
            initial_backoff_ms: 100, // Default value for library usage
        };

        let pipeline = Arc::new(Pipeline::new(operation, pipeline_config).await?);
        pipeline.run().await?;

        Ok(())
    }
}
