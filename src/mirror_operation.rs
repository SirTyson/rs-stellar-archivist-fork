//! Mirror operation - copies files from source to destination

use crate::history_file;
use crate::pipeline::{async_trait, Operation, ReaderResult};
use crate::storage::WRITE_BUF_BYTES;
use crate::utils::ArchiveStats;
use anyhow::Result;
use tracing::{debug, error, info};

pub struct MirrorOperation {
    dst_op: crate::utils::StorageRef,
    overwrite: bool,
    stats: ArchiveStats,
}

impl MirrorOperation {
    pub async fn new(dst: &str, overwrite: bool) -> Result<Self> {
        let dst_op = crate::storage::StorageBackend::from_url(dst, None).await?;

        // Destination must be a filesystem
        if !dst.starts_with("file://") {
            anyhow::bail!("Destination must be a filesystem path (file://...)");
        }

        Ok(Self {
            dst_op,
            overwrite,
            stats: ArchiveStats::new(),
        })
    }

    /// Get the destination's latest checkpoint from its HAS file
    pub async fn get_latest_dest_checkpoint(&self) -> Result<u32> {
        use crate::utils::fetch_history_archive_state;

        // Read the destination's HAS file - error if it doesn't exist
        let has = fetch_history_archive_state(&self.dst_op).await?;
        Ok(has.current_ledger)
    }

    /// Update the HAS file to destination if needed
    async fn maybe_update_has(&self, highest_checkpoint: u32) -> Result<()> {
        // Check if we should update the HAS file
        // We should only update if:
        // 1. There's no existing HAS file, OR
        // 2. The new checkpoint is higher than the existing HAS

        let should_update = match self.get_latest_dest_checkpoint().await {
            Ok(existing_ledger) => {
                let existing_checkpoint = history_file::checkpoint_number(existing_ledger);
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
            Err(_) => {
                // No existing HAS file - create a new one
                info!(
                    "No existing HAS, creating new one at checkpoint {:08x}",
                    highest_checkpoint
                );
                true
            }
        };

        if should_update {
            // Copy the history file at the specified checkpoint to be our HAS file
            let history_path = history_file::checkpoint_path("history", highest_checkpoint);

            // IMPORTANT: Read the history file from DESTINATION, not source!
            // The source may have advanced since we started mirroring.
            // We want the HAS to reflect what we actually mirrored.
            use tokio::io::AsyncReadExt;
            let mut reader = self
                .dst_op
                .open_reader(&history_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read history file: {}", e))?;
            let mut history_content = Vec::new();
            reader
                .read_to_end(&mut history_content)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read history content: {}", e))?;

            // Write it as the .well-known/stellar-history.json
            let has_path = ".well-known/stellar-history.json";
            let mut writer = self
                .dst_op
                .open_writer(has_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open HAS file for writing: {}", e))?;
            use tokio::io::AsyncWriteExt;
            writer
                .write_all(&history_content)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to write HAS file: {}", e))?;
            writer
                .flush()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to flush HAS file: {}", e))?;

            info!(
                "Updated destination HAS to checkpoint {:08x}",
                highest_checkpoint
            );
        }

        Ok(())
    }
}

#[async_trait]
impl Operation for MirrorOperation {
    async fn process_object(&self, path: &str, reader_result: ReaderResult) {
        // Handle case where we couldn't get a reader (source file missing/inaccessible)
        let mut reader = match reader_result {
            ReaderResult::Ok(r) => r,
            ReaderResult::Err(e) => {
                // Source file couldn't be read - count as failure
                error!("Failed to read source file {}: {}", path, e);
                self.stats
                    .record_failure(path, &format!("Source read error: {}", e))
                    .await;
                return;
            }
        };
        // Check if file exists and handle based on overwrite mode
        match self.dst_op.exists(path).await {
            Ok(true) => {
                if !self.overwrite {
                    debug!("Skipping existing file: {}", path);
                    self.stats.record_skipped(path);
                    return;
                } else {
                    // Proceed with overwrite
                    debug!("Overwriting existing file: {}", path);
                }
            }
            Ok(false) => {
                // File doesn't exist, proceed with download
            }
            Err(e) => {
                // Failed to check existence - treat as error
                error!("Failed to check existence of file {}: {}", path, e);
                self.stats
                    .record_failure(path, &format!("Existence check failed: {}", e))
                    .await;
                return;
            }
        }

        use tokio::io::{AsyncWriteExt, BufWriter};

        // Stream from source to destination
        let write_result = async {
            let writer = self.dst_op.open_writer(path).await?;
            let mut buf_writer = BufWriter::with_capacity(WRITE_BUF_BYTES, writer);
            tokio::io::copy(&mut reader, &mut buf_writer).await?;
            buf_writer.flush().await?;
            Ok::<(), std::io::Error>(())
        }
        .await;

        match write_result {
            Ok(_) => {
                debug!("Successfully copied: {}", path);
                self.stats.record_success(path);
            }
            Err(e) => {
                error!("Failed to write file {}: {}", path, e);
                self.stats
                    .record_failure(path, &format!("Write error: {}", e))
                    .await;
            }
        }
    }

    async fn finalize(&self, highest_checkpoint: Option<u32>) -> Result<()> {
        // Generate and log complete report
        self.stats.report("mirror").await;

        // Update HAS file if we processed any checkpoints
        if let Some(highest) = highest_checkpoint {
            // Update HAS file if needed (only if we're advancing it)
            if let Err(e) = self.maybe_update_has(highest).await {
                error!("Failed to update HAS file: {}", e);
                return Err(e);
            }
        }

        // Report failure if there were any failed files
        if self.stats.has_failures() {
            anyhow::bail!("Archive mirror failed");
        }

        Ok(())
    }
}
