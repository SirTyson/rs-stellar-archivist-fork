//! Utilities for Stellar History Archives including stats tracking and fetching

use anyhow::{Context, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::history_file::HistoryFileState;

/// Storage reference - unified through Storage trait (no double indirection)
pub type StorageRef = Arc<dyn crate::storage::Storage + Send + Sync>;

/// Shared statistics tracking for archive operations
/// Provides consistent reporting across scan and mirror operations
pub struct ArchiveStats {
    // Successfully processed files
    pub successful_files: AtomicU64,

    // Failed files (any type of failure)
    pub failed_files: AtomicU64,

    // Skipped files (already exist in mirror mode)
    pub skipped_files: AtomicU64,

    // Missing required files (scan mode)
    pub missing_required: AtomicU64,

    // Missing files by category
    pub missing_history: AtomicU64,
    pub missing_ledger: AtomicU64,
    pub missing_transactions: AtomicU64,
    pub missing_results: AtomicU64,
    pub missing_buckets: AtomicU64,
    pub missing_scp: AtomicU64, // Optional files

    // List of all failed/missing files for detailed reporting
    pub failed_list: Arc<tokio::sync::Mutex<Vec<String>>>,
}

impl ArchiveStats {
    pub fn new() -> Self {
        Self {
            successful_files: AtomicU64::new(0),
            failed_files: AtomicU64::new(0),
            skipped_files: AtomicU64::new(0),
            missing_required: AtomicU64::new(0),
            missing_history: AtomicU64::new(0),
            missing_ledger: AtomicU64::new(0),
            missing_transactions: AtomicU64::new(0),
            missing_results: AtomicU64::new(0),
            missing_buckets: AtomicU64::new(0),
            missing_scp: AtomicU64::new(0),
            failed_list: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// Record a successful file processing
    pub fn record_success(&self, _path: &str) {
        self.successful_files.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a skipped file (already exists)
    pub fn record_skipped(&self, _path: &str) {
        self.skipped_files.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed/missing file and categorize it
    pub async fn record_failure(&self, path: &str, _reason: &str) {
        self.failed_files.fetch_add(1, Ordering::Relaxed);

        // Categorize the failure by file type
        if path.contains("history") {
            self.missing_history.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("ledger") {
            self.missing_ledger.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("transactions") {
            self.missing_transactions.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("results") {
            self.missing_results.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("bucket") {
            self.missing_buckets.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("scp") {
            self.missing_scp.fetch_add(1, Ordering::Relaxed);
        }

        // Track if it's a required file (not optional SCP)
        if !path.contains("scp") {
            self.missing_required.fetch_add(1, Ordering::Relaxed);
        }

        // Add to failed list for detailed reporting
        let mut failed_list = self.failed_list.lock().await;
        failed_list.push(path.to_string());
    }

    /// Generate and log a complete report of the operation results
    pub async fn report(&self, operation: &str) {
        let successful = self.successful_files.load(Ordering::Relaxed);
        let failed = self.failed_files.load(Ordering::Relaxed);
        let skipped = self.skipped_files.load(Ordering::Relaxed);

        // Log the summary
        if operation == "mirror" {
            info!(
                "Mirror completed: {} files copied, {} failed, {} skipped",
                successful, failed, skipped
            );
        } else {
            let missing_required = self.missing_required.load(Ordering::Relaxed);
            info!(
                "Scan complete: {} files found, {} missing ({} required)",
                successful, failed, missing_required
            );
        }

        // Log detailed breakdown if there were failures
        if failed == 0 {
            return;
        }

        let missing_history = self.missing_history.load(Ordering::Relaxed);
        let missing_ledger = self.missing_ledger.load(Ordering::Relaxed);
        let missing_transactions = self.missing_transactions.load(Ordering::Relaxed);
        let missing_results = self.missing_results.load(Ordering::Relaxed);
        let missing_buckets = self.missing_buckets.load(Ordering::Relaxed);
        let missing_scp = self.missing_scp.load(Ordering::Relaxed);

        // Log breakdown by category
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
        if missing_buckets > 0 {
            error!("Missing {} buckets", missing_buckets);
        }
        if missing_scp > 0 {
            warn!("Missing {} optional scp files", missing_scp);
        }
    }

    /// Check if there were any failures that should fail the operation
    pub fn has_failures(&self) -> bool {
        self.failed_files.load(Ordering::Relaxed) > 0
    }
}

/// Validate that a source archive exists and is accessible
/// Returns the HAS file if found, to avoid re-reading it
/// Only validates filesystem paths to avoid triggering network requests
pub async fn validate_source_and_get_has(
    store: &StorageRef,
    url: &str,
) -> Result<Option<HistoryFileState>> {
    use crate::history_file::ROOT_HAS_PATH;

    // Only validate file:// URLs to avoid triggering network requests
    // HTTP/HTTPS errors will be caught naturally when we try to fetch
    if url.starts_with("file://") {
        let path = url.trim_start_matches("file://");
        if !std::path::Path::new(path).exists() {
            anyhow::bail!(
                "Source path does not exist: {}\nPlease check the path and try again.",
                path
            );
        }

        // Try to read the HAS file for filesystem paths
        match fetch_history_archive_state(store).await {
            Ok(has) => Ok(Some(has)),
            Err(e) => {
                // Check if it's because the file doesn't exist
                if store.exists(ROOT_HAS_PATH).await? == false {
                    anyhow::bail!(
                        "Source path exists but is not a valid Stellar History Archive: {}\nNo {} file found.",
                        path, ROOT_HAS_PATH
                    );
                }
                // Other error accessing the HAS
                Err(e)
                    .with_context(|| format!("Failed to read History Archive State from: {}", url))
            }
        }
    } else {
        // For HTTP/HTTPS, we'll fetch the HAS during normal operation
        Ok(None)
    }
}

/// Fetch the root History Archive State
pub async fn fetch_history_archive_state(store: &StorageRef) -> Result<HistoryFileState> {
    use crate::history_file::ROOT_HAS_PATH;
    fetch_history_archive_state_path(store, ROOT_HAS_PATH).await
}

/// Fetch a History Archive State from a specific path
pub async fn fetch_history_archive_state_path(
    store: &StorageRef,
    path: &str,
) -> Result<HistoryFileState> {
    use tokio::io::AsyncReadExt;
    use tracing::debug;

    debug!("Fetching HAS from path: {}", path);

    // Read the file content
    let mut reader = store
        .open_reader(path)
        .await
        .with_context(|| format!("Failed to open reader for: {}", path))?;
    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .await
        .with_context(|| format!("Failed to read file: {}", path))?;

    // Parse the JSON
    let has: HistoryFileState = serde_json::from_slice(&buffer)
        .with_context(|| format!("Failed to parse History Archive State from: {}", path))?;

    // Validate the HAS format
    has.validate()
        .map_err(|e| anyhow::anyhow!("Invalid HAS format in {}: {}", path, e))?;

    Ok(has)
}
