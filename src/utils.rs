use anyhow::{Context, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::history_file::HistoryFileState;
use crate::storage::StorageRef;

/// Shared statistics tracking for archive operations
/// for consistent reporting across scan and mirror operations
pub struct ArchiveStats {
    // Successfully processed files
    pub successful_files: AtomicU64,

    // Failed files (any type of failure)
    pub failed_files: AtomicU64,

    // Skipped files (already exist in mirror mode)
    pub skipped_files: AtomicU64,

    pub missing_required: AtomicU64,
    pub missing_history: AtomicU64,
    pub missing_ledger: AtomicU64,
    pub missing_transactions: AtomicU64,
    pub missing_results: AtomicU64,
    pub missing_buckets: AtomicU64,
    pub missing_scp: AtomicU64,

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

    pub fn record_success(&self, _path: &str) {
        self.successful_files.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a skipped file (already exists in mirror mode)
    pub fn record_skipped(&self, _path: &str) {
        self.skipped_files.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn record_failure(&self, path: &str) {
        self.failed_files.fetch_add(1, Ordering::Relaxed);

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

        if !path.contains("scp") {
            self.missing_required.fetch_add(1, Ordering::Relaxed);
        }

        let mut failed_list = self.failed_list.lock().await;
        failed_list.push(path.to_string());
    }

    /// Generate and log a complete report of the operation results
    pub async fn report(&self, operation: &str) {
        let successful = self.successful_files.load(Ordering::Relaxed);
        let failed = self.failed_files.load(Ordering::Relaxed);
        let skipped = self.skipped_files.load(Ordering::Relaxed);

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

        if failed == 0 {
            return;
        }

        let missing_history = self.missing_history.load(Ordering::Relaxed);
        let missing_ledger = self.missing_ledger.load(Ordering::Relaxed);
        let missing_transactions = self.missing_transactions.load(Ordering::Relaxed);
        let missing_results = self.missing_results.load(Ordering::Relaxed);
        let missing_buckets = self.missing_buckets.load(Ordering::Relaxed);
        let missing_scp = self.missing_scp.load(Ordering::Relaxed);

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

    pub fn has_failures(&self) -> bool {
        self.failed_files.load(Ordering::Relaxed) > 0
    }
}

/// Fetch and validate .well-known/stellar-history.json from store
pub async fn fetch_well_known_history_file(store: &StorageRef) -> Result<HistoryFileState> {
    use crate::history_file::ROOT_WELL_KNOWN_PATH;
    use tokio::io::AsyncReadExt;
    use tracing::debug;

    debug!("Fetching .well-known from path: {}", ROOT_WELL_KNOWN_PATH);

    // Read the file content
    let mut reader = store
        .open_reader(ROOT_WELL_KNOWN_PATH)
        .await
        .with_context(|| format!("Failed to open reader for: {}", ROOT_WELL_KNOWN_PATH))?;
    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .await
        .with_context(|| format!("Failed to read file: {}", ROOT_WELL_KNOWN_PATH))?;

    // Parse the JSON
    let state: HistoryFileState = serde_json::from_slice(&buffer).with_context(|| {
        format!(
            "Failed to parse History Archive State from: {}",
            ROOT_WELL_KNOWN_PATH
        )
    })?;

    // Validate the .well-known format
    state.validate().map_err(|e| {
        anyhow::anyhow!(
            "Invalid .well-known format in {}: {}",
            ROOT_WELL_KNOWN_PATH,
            e
        )
    })?;

    Ok(state)
}

/// Compute checkpoint bounds from source archive and user-specified low/high values
/// Returns (first_checkpoint, final_checkpoint)
pub async fn compute_checkpoint_bounds(
    source: &StorageRef,
    low: Option<u32>,
    high: Option<u32>,
) -> Result<(u32, u32)> {
    use crate::history_file;
    use tracing::{info, warn};

    // Fetch the .well-known file from source
    let state = fetch_well_known_history_file(source).await?;
    let current_ledger = state.current_ledger;
    let highest_source_checkpoint = history_file::round_to_lower_checkpoint(current_ledger);

    info!(
        "Source archive reports current ledger: {} (checkpoint: 0x{:08x})",
        current_ledger, highest_source_checkpoint
    );

    // Check that the user-specified low is not below the source's current checkpoint
    // If low is not provided, default to genesis checkpoint
    let low_checkpoint = if let Some(low) = low {
        let low_checkpoint = history_file::round_to_lower_checkpoint(low);

        // Check if the source's current checkpoint is below requested low
        if highest_source_checkpoint < low_checkpoint {
            anyhow::bail!(
                "No checkpoints above the lower bound: archive's latest checkpoint 0x{:08x} (ledger {}) is below requested low 0x{:08x} (ledger {})",
                highest_source_checkpoint, current_ledger, low_checkpoint, low
            );
        }

        low_checkpoint
    } else {
        history_file::GENESIS_CHECKPOINT_LEDGER
    };

    let high_checkpoint = if let Some(high) = high {
        let high_checkpoint = history_file::round_to_upper_checkpoint(high);

        // Warn if the user passed a high ledger that is above what we see in source
        // We don't fail, but use the source's current checkpoint as the high bound
        if highest_source_checkpoint < high_checkpoint {
            warn!(
                "Archive's latest checkpoint 0x{:08x} (ledger {}) is below requested high 0x{:08x} (ledger {}), will process up to latest available",
                highest_source_checkpoint, current_ledger, high_checkpoint, high
            );
            highest_source_checkpoint
        } else {
            high_checkpoint
        }
    } else {
        highest_source_checkpoint
    };

    // Make sure we have at least one checkpoint in range
    if low_checkpoint > high_checkpoint {
        anyhow::bail!(
            "No checkpoints found in range: low checkpoint 0x{:08x} is greater than high checkpoint 0x{:08x}",
            low_checkpoint,
            high_checkpoint
        );
    }

    let total_count = history_file::count_checkpoints_in_range(low_checkpoint, high_checkpoint);
    info!(
        "Processing {} checkpoints from 0x{:08x} to 0x{:08x}",
        total_count, low_checkpoint, high_checkpoint
    );

    Ok((low_checkpoint, high_checkpoint))
}
