/// Scan operation - checks if files exist in an archive
use crate::fetcher::{exists, StorageRef as Storage};
use crate::pipeline::{async_trait, ArchiveOperation, ProcessResult};
use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub struct ScanOperation {
    op: Storage,
    existing_files: AtomicU64,
    missing_files: AtomicU64,
    missing_required: AtomicU64,
    missing_buckets: AtomicU64,
    missing_list: Arc<tokio::sync::Mutex<Vec<String>>>,
}

impl ScanOperation {
    /// Create a new scan operation
    pub async fn new(archive: &str) -> Result<Self> {
        Self::new_with_concurrency(archive, 256).await
    }

    /// Create a new scan operation with custom HTTP concurrency
    pub async fn new_with_concurrency(archive: &str, http_connections: usize) -> Result<Self> {
        let op = crate::fetcher::new_operator_with_concurrency(archive, http_connections).await?;

        Ok(Self {
            op,
            existing_files: AtomicU64::new(0),
            missing_files: AtomicU64::new(0),
            missing_required: AtomicU64::new(0),
            missing_buckets: AtomicU64::new(0),
            missing_list: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        })
    }

    /// Get the storage operator for this scan
    pub fn storage(&self) -> &Storage {
        &self.op
    }
}

#[async_trait]
impl ArchiveOperation for ScanOperation {
    async fn process_file(&self, path: &str, is_optional: bool) -> Result<ProcessResult> {
        // Check if file exists
        match exists(&self.op, path).await {
            Ok(true) => {
                debug!("Found: {}", path);
                self.existing_files.fetch_add(1, Ordering::Relaxed);
                Ok(ProcessResult::Success)
            }
            Ok(false) => {
                self.missing_files.fetch_add(1, Ordering::Relaxed);

                if is_optional {
                    warn!("Missing optional file: {}", path);
                } else {
                    error!("Missing required file: {}", path);
                    self.missing_required.fetch_add(1, Ordering::Relaxed);

                    if path.contains("/bucket-") {
                        self.missing_buckets.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Add to missing list
                let mut missing = self.missing_list.lock().await;
                missing.push(path.to_string());

                Ok(ProcessResult::Failed(format!("File not found: {}", path)))
            }
            Err(e) => {
                error!("Error checking {}: {}", path, e);
                self.missing_files.fetch_add(1, Ordering::Relaxed);
                if !is_optional {
                    self.missing_required.fetch_add(1, Ordering::Relaxed);
                }

                // Add to missing list
                let mut missing = self.missing_list.lock().await;
                missing.push(path.to_string());

                Ok(ProcessResult::Failed(e.to_string()))
            }
        }
    }

    async fn handle_result(&self, path: &str, result: &ProcessResult) -> Result<()> {
        match result {
            ProcessResult::Success => {
                debug!("File exists: {}", path);
            }
            ProcessResult::Failed(err) => {
                debug!("File missing or error: {} - {}", path, err);
            }
            ProcessResult::Skipped => {
                debug!("Skipped: {}", path);
            }
        }
        Ok(())
    }

    async fn finalize(&self) -> Result<()> {
        let total_existing = self.existing_files.load(Ordering::Relaxed);
        let total_missing = self.missing_files.load(Ordering::Relaxed);
        let missing_required = self.missing_required.load(Ordering::Relaxed);
        let missing_buckets = self.missing_buckets.load(Ordering::Relaxed);

        info!(
            "Scan complete: {} files found, {} missing ({} required)",
            total_existing, total_missing, missing_required
        );

        if total_missing > 0 {
            let missing_list = self.missing_list.lock().await;

            // Count missing files by category
            let missing_history = missing_list
                .iter()
                .filter(|f| f.contains("/history-"))
                .count();
            let missing_ledger = missing_list
                .iter()
                .filter(|f| f.contains("/ledger-"))
                .count();
            let missing_transactions = missing_list
                .iter()
                .filter(|f| f.contains("/transactions-"))
                .count();
            let missing_results = missing_list
                .iter()
                .filter(|f| f.contains("/results-"))
                .count();
            let missing_scp = missing_list
                .iter()
                .filter(|f| f.contains("/scp-") || f.starts_with("scp/"))
                .count();

            if missing_history > 0 {
                error!("Missing {} history files", missing_history);
            }
            if missing_ledger > 0 {
                error!("Missing {} ledger files", missing_ledger);
            }
            if missing_transactions > 0 {
                error!("Missing {} transactions files", missing_transactions);
            }
            if missing_results > 0 {
                error!("Missing {} results files", missing_results);
            }
            if missing_scp > 0 {
                warn!("Missing {} optional scp files", missing_scp);
            }
            if missing_buckets > 0 {
                error!("Missing {} buckets", missing_buckets);
                return Err(anyhow::anyhow!("Missing {} buckets", missing_buckets));
            }

            if missing_required > 0 {
                return Err(anyhow::anyhow!(
                    "Archive scan failed: {} required files missing",
                    missing_required
                ));
            }
        }

        Ok(())
    }
}
