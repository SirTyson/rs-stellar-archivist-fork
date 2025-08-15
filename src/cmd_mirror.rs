//! Mirror command implementation using the simplified pipeline

use crate::mirror_operation::MirrorOperation;
use crate::pipeline::{Pipeline, PipelineConfig};
use anyhow::Result;
use std::sync::Arc;
use tracing::{debug, info, warn};

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
    pub http_connections: usize,
}

pub async fn run(config: MirrorConfig) -> Result<()> {
    info!(
        "Starting mirror from {} to {} with {} workers",
        config.src, config.dst, config.concurrency
    );

    // Create the mirror operation with custom HTTP concurrency
    let operation = MirrorOperation::new_with_concurrency(
        &config.src,
        &config.dst,
        config.overwrite,
        config.http_connections,
    )
    .await?;

    // Get the source storage operator from the operation to ensure consistent policy
    let source_op = operation.source_storage().clone();

    // Check if destination has an existing HAS file to resume from
    let mut effective_low = config.low;

    // Check for gap validation when --low is provided
    if let Some(requested_low) = config.low {
        // Always check for gaps when --low is specified (regardless of --overwrite)
        // Gap validation is only bypassed with --allow-mirror-gaps
        match operation.get_destination_checkpoint().await {
            Ok(Some(dest_ledger)) => {
                let dest_checkpoint = crate::history_archive::checkpoint_number(dest_ledger);
                let requested_checkpoint = crate::history_archive::checkpoint_number(requested_low);

                // Check if destination's last checkpoint is >= requested low
                if dest_checkpoint < requested_checkpoint {
                    // There would be a gap!
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
            Ok(None) => {
                // No existing archive, --low is fine
                debug!(
                    "No existing archive at destination, --low {} will be used",
                    requested_low
                );
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
    } else if !config.overwrite {
        // No --low specified and not overwriting, try to resume from destination
        match operation.get_destination_checkpoint().await {
            Ok(Some(checkpoint_ledger)) => {
                info!(
                    "Found existing archive at destination with ledger {}, resuming from next checkpoint",
                    checkpoint_ledger
                );
                // Resume from the next checkpoint after what's already mirrored
                effective_low = Some(checkpoint_ledger + 1);
            }
            Ok(None) => {
                debug!("No existing HAS file found at destination, starting from beginning");
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

    // Configure the pipeline
    let pipeline_config = PipelineConfig {
        source: config.src.clone(),
        concurrency: config.concurrency,
        skip_optional: config.skip_optional,
        low: effective_low,
        high: config.high,
    };

    // Create the pipeline with the same storage operator
    let pipeline =
        Arc::new(Pipeline::new_with_storage(operation, pipeline_config, source_op).await?);

    // Set the pipeline reference in the operation
    // Set the pipeline reference safely
    pipeline.operation.set_pipeline(pipeline.clone());

    // Run the pipeline
    pipeline.run().await?;

    Ok(())
}
