/// Scan operation - validates that files exist
use crate::pipeline::{async_trait, Operation};
use crate::storage::{ReaderResult, StorageRef};
use crate::utils::{compute_checkpoint_bounds, ArchiveStats};
use anyhow::Result;
use tracing::{debug, error};

pub struct ScanOperation {
    stats: ArchiveStats,

    // User-specified arguments from CLI
    low: Option<u32>,
    high: Option<u32>,
}

impl ScanOperation {
    pub async fn new(low: Option<u32>, high: Option<u32>) -> Result<Self> {
        Ok(Self {
            stats: ArchiveStats::new(),
            low,
            high,
        })
    }
}

#[async_trait]
impl Operation for ScanOperation {
    async fn get_checkpoint_bounds(&self, source: &StorageRef) -> Result<(u32, u32)> {
        compute_checkpoint_bounds(source, self.low, self.high).await
    }

    async fn process_object(&self, path: &str, reader_result: ReaderResult) {
        use tokio::io::AsyncReadExt;

        let mut reader = match reader_result {
            ReaderResult::Ok(r) => r,
            ReaderResult::Err(_e) => {
                // Source file couldn't be read
                error!("Invalid file: {} - File not found or inaccessible", path);
                self.stats.record_failure(path).await;
                return;
            }
        };

        // Stream the file and check that we can read at least one byte
        // This validates the file exists and is readable without buffering the entire content
        let mut buffer = [0u8; 1];
        match reader.read(&mut buffer).await {
            Ok(n) if n > 0 => {
                // File exists and has content
                debug!("Validated: {}", path);
                self.stats.record_success(path);
            }
            Ok(_) => {
                // Empty file
                error!("Invalid file: {} - Empty file", path);
                self.stats.record_failure(path).await;
            }
            Err(e) => {
                // Error reading file
                error!("Invalid file: {} - Read error: {}", path, e);
                self.stats.record_failure(path).await;
            }
        }
    }

    async fn finalize(&self, _highest_checkpoint: u32) -> Result<()> {
        self.stats.report("scan").await;

        // Fail if there were any failures
        if self.stats.has_failures() {
            return Err(anyhow::anyhow!("Archive scan failed"));
        }

        Ok(())
    }
}
