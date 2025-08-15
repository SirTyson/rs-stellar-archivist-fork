//! Simplified mirror operation - only handles file copying

use crate::fetcher::{exists, read_file, write_file, StorageRef as Storage};
use crate::history_archive;
use crate::pipeline::{async_trait, ArchiveOperation, Pipeline, ProcessResult};
use anyhow::Result;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub struct MirrorOperation {
    // Source and destination operators
    src_op: Storage,
    dst_op: Storage,
    pipeline: RwLock<Option<Arc<Pipeline<MirrorOperation>>>>,
    overwrite: bool,
    // Tracking
    total_files: Arc<AtomicUsize>,
    failed_files: Arc<AtomicUsize>,
    skipped_files: Arc<AtomicUsize>,
}

impl MirrorOperation {
    /// Create a new mirror operation
    pub async fn new(src: &str, dst: &str, overwrite: bool) -> Result<Self> {
        Self::new_with_concurrency(src, dst, overwrite, 256).await
    }

    /// Create a new mirror operation with custom HTTP concurrency
    pub async fn new_with_concurrency(
        src: &str,
        dst: &str,
        overwrite: bool,
        http_connections: usize,
    ) -> Result<Self> {
        let src_op = crate::fetcher::new_operator_with_concurrency(src, http_connections).await?;
        let dst_op = crate::fetcher::new_operator_with_concurrency(dst, http_connections).await?;

        // Validate destination is filesystem
        if !dst.starts_with("file://") {
            anyhow::bail!("Destination must be a filesystem path (file://...)");
        }

        Ok(Self {
            src_op,
            dst_op,
            pipeline: RwLock::new(None),
            overwrite,
            total_files: Arc::new(AtomicUsize::new(0)),
            failed_files: Arc::new(AtomicUsize::new(0)),
            skipped_files: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Get the source storage operator
    pub fn source_storage(&self) -> &Storage {
        &self.src_op
    }

    /// Get the destination's current checkpoint from its HAS file if it exists
    pub async fn get_destination_checkpoint(&self) -> Result<Option<u32>> {
        use crate::fetcher::fetch_history_archive_state;

        // Try to read the destination's HAS file
        match fetch_history_archive_state(&self.dst_op).await {
            Ok(has) => {
                // Return the current ledger from the destination's HAS
                Ok(Some(has.current_ledger))
            }
            Err(_) => {
                // No HAS file or couldn't read it - destination is empty or invalid
                Ok(None)
            }
        }
    }

    /// Set the pipeline reference (called by Pipeline after creation)
    pub fn set_pipeline(&self, pipeline: Arc<Pipeline<MirrorOperation>>) {
        *self.pipeline.write() = Some(pipeline);
    }

    /// Copy a file from source to destination
    async fn copy_file_impl(&self, path: &str) -> Result<()> {
        crate::fetcher::copy_file(&self.src_op, &self.dst_op, path).await?;
        debug!("Copied: {}", path);
        Ok(())
    }

    /// Update the HAS file to destination if needed
    async fn update_has_if_needed(&self, highest_checkpoint: u32) -> Result<()> {
        // Check if we should update the HAS file
        // We should only update if:
        // 1. There's no existing HAS file, OR
        // 2. The new checkpoint is higher than the existing HAS

        let should_update = match self.get_destination_checkpoint().await {
            Ok(Some(existing_ledger)) => {
                let existing_checkpoint = history_archive::checkpoint_number(existing_ledger);
                if highest_checkpoint > existing_checkpoint {
                    info!(
                        "Updating HAS from checkpoint {:08x} to {:08x}",
                        existing_checkpoint, highest_checkpoint
                    );
                    true
                } else {
                    info!(
                        "Keeping existing HAS at checkpoint {:08x} (mirrored up to {:08x})",
                        existing_checkpoint, highest_checkpoint
                    );
                    false
                }
            }
            Ok(None) => {
                info!(
                    "No existing HAS, creating new one at checkpoint {:08x}",
                    highest_checkpoint
                );
                true
            }
            Err(_) => {
                info!(
                    "Could not read existing HAS, creating new one at checkpoint {:08x}",
                    highest_checkpoint
                );
                true
            }
        };

        if should_update {
            // Copy the history file at the specified checkpoint to be our HAS file
            let history_path = history_archive::checkpoint_path("history", highest_checkpoint);

            // IMPORTANT: Read the history file from DESTINATION, not source!
            // The source may have advanced since we started mirroring.
            // We want the HAS to reflect what we actually mirrored.
            let history_content = read_file(&self.dst_op, &history_path).await?;

            // Write it as the .well-known/stellar-history.json
            let has_path = ".well-known/stellar-history.json";
            write_file(&self.dst_op, has_path, history_content).await?;

            info!(
                "Updated destination HAS to checkpoint {:08x}",
                highest_checkpoint
            );
        }

        Ok(())
    }
}

#[async_trait]
impl ArchiveOperation for MirrorOperation {
    async fn process_file(&self, path: &str, is_optional: bool) -> Result<ProcessResult> {
        // With overwrite mode, we overwrite files within the mirrored range
        // Without overwrite mode, we skip existing files
        if !self.overwrite {
            if exists(&self.dst_op, path).await? {
                debug!("Skipping existing file: {}", path);
                self.skipped_files.fetch_add(1, Ordering::Relaxed);
                return Ok(ProcessResult::Skipped);
            }
        } else {
            // In overwrite mode, we re-download files that are within the range
            // Files outside the range are preserved (this is handled by the pipeline)
            if exists(&self.dst_op, path).await? {
                debug!("Overwriting existing file: {}", path);
            }
        }

        // Copy the file
        match self.copy_file_impl(path).await {
            Ok(_) => {
                self.total_files.fetch_add(1, Ordering::Relaxed);
                Ok(ProcessResult::Success)
            }
            Err(e) => {
                if is_optional {
                    warn!("Failed to copy optional file {}: {}", path, e);
                    self.skipped_files.fetch_add(1, Ordering::Relaxed);
                    Ok(ProcessResult::Skipped)
                } else {
                    error!("Failed to copy required file {}: {}", path, e);
                    self.failed_files.fetch_add(1, Ordering::Relaxed);
                    Ok(ProcessResult::Failed(e.to_string()))
                }
            }
        }
    }

    async fn handle_result(&self, path: &str, result: &ProcessResult) -> Result<()> {
        match result {
            ProcessResult::Success => {
                debug!("Successfully copied: {}", path);
            }
            ProcessResult::Failed(err) => {
                error!("Failed to copy {}: {}", path, err);
            }
            ProcessResult::Skipped => {
                debug!("Skipped: {}", path);
            }
        }
        Ok(())
    }

    async fn finalize(&self) -> Result<()> {
        let total = self.total_files.load(Ordering::Relaxed);
        let failed = self.failed_files.load(Ordering::Relaxed);
        let skipped = self.skipped_files.load(Ordering::Relaxed);

        info!(
            "Mirror completed: {} files copied, {} failed, {} skipped",
            total, failed, skipped
        );

        // Get the highest checkpoint from the pipeline
        let pipeline_ref = self.pipeline.read().clone();
        if let Some(ref pipeline) = pipeline_ref {
            if let Some(highest) = pipeline.get_highest_checkpoint().await {
                // Update HAS file if needed (only if we're advancing it)
                if let Err(e) = self.update_has_if_needed(highest).await {
                    error!("Failed to update HAS file: {}", e);
                    return Err(e);
                }
            }
        }

        let total_failed = self.failed_files.load(Ordering::Relaxed);
        if total_failed > 0 {
            anyhow::bail!("Mirror completed with {} failures", total_failed);
        }

        Ok(())
    }
}
