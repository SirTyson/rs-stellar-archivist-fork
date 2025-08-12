use crate::archive::new_operator;
use crate::history_archive;
use anyhow::Result;
use futures::stream::{self, StreamExt};
use opendal::Operator;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

#[derive(Debug)]
pub struct ScanConfig {
    pub archive: String,
    pub concurrency: usize,
    pub skip_optional: bool,
}

pub async fn run(config: ScanConfig) -> Result<()> {
    let op = new_operator(&config.archive)?;

    info!("Starting scan of {}", config.archive);

    // Fetch HAS to determine archive state and expected range
    let has = crate::archive::fetch_history_archive_state(&op).await?;
    let current_ledger = has.current_ledger;
    let current_checkpoint = history_archive::checkpoint_number(current_ledger);

    info!(
        "Archive reports current ledger: {} (checkpoint: 0x{:08x})",
        current_ledger, current_checkpoint
    );

    // Generate list of expected checkpoint files based on HAS
    let expected_files = history_archive::enumerate_checkpoint_files(&has, !config.skip_optional);

    info!(
        "Expecting {} checkpoint files for range [0x0000003f, 0x{:08x}]",
        expected_files.len(),
        current_checkpoint
    );

    // Check each expected file exists
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let existing = Arc::new(AtomicU64::new(0));
    let missing = Arc::new(AtomicU64::new(0));
    let missing_files = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let _results: Vec<_> = stream::iter(expected_files)
        .map(|path| {
            let op = op.clone();
            let semaphore = semaphore.clone();
            let existing = existing.clone();
            let missing = missing.clone();
            let missing_files = missing_files.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();
                // Check if file exists
                match op.exists(&path).await {
                    Ok(true) => {
                        existing.fetch_add(1, Ordering::Relaxed);
                        debug!("Found: {}", path);
                    }
                    Ok(false) => {
                        missing.fetch_add(1, Ordering::Relaxed);
                        // Check if it's an SCP file (optional)
                        if path.contains("/scp/") || path.starts_with("scp/") {
                            warn!("Missing optional file: {}", path);
                        } else {
                            error!("Missing required file: {}", path);
                        }
                        missing_files.lock().await.push(path.clone());
                    }
                    Err(e) => {
                        missing.fetch_add(1, Ordering::Relaxed);
                        error!("Error checking {}: {}", path, e);
                        missing_files.lock().await.push(path);
                    }
                }
            }
        })
        .buffer_unordered(config.concurrency)
        .collect()
        .await;

    // Scan for bucket files referenced by ALL history files (not just root HAS)
    let bucket_count = scan_all_buckets(&op, &has, config.concurrency).await?;

    let total_existing = existing.load(Ordering::Relaxed);
    let total_missing = missing.load(Ordering::Relaxed);

    info!(
        "Scan complete: {} checkpoint files found, {} missing, {} buckets verified",
        total_existing, total_missing, bucket_count
    );

    if total_missing > 0 {
        let missing_list = missing_files.lock().await;
        // Count missing files by category
        let missing_history = missing_list
            .iter()
            .filter(|f| f.contains("/history/"))
            .count();
        let missing_ledger = missing_list
            .iter()
            .filter(|f| f.contains("/ledger/"))
            .count();
        let missing_transactions = missing_list
            .iter()
            .filter(|f| f.contains("/transactions/"))
            .count();
        let missing_results = missing_list
            .iter()
            .filter(|f| f.contains("/results/"))
            .count();
        let missing_scp = missing_list
            .iter()
            .filter(|f| f.contains("/scp/") || f.starts_with("scp/"))
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
        if missing_scp > 0 && !config.skip_optional {
            warn!("Missing {} optional scp files", missing_scp);
        }

        // Only fail if required files are missing
        let required_missing = total_missing - missing_scp as u64;
        if required_missing > 0 {
            anyhow::bail!(
                "Archive scan failed: {} required files missing",
                required_missing
            );
        }
    }

    Ok(())
}

async fn scan_all_buckets(
    op: &Operator,
    has: &history_archive::HistoryArchiveState,
    concurrency: usize,
) -> Result<u64> {
    // Collect all bucket hashes from ALL history files in the archive
    let mut all_bucket_hashes = HashSet::new();
    
    // First add buckets from root HAS
    for hash in has.buckets() {
        all_bucket_hashes.insert(hash);
    }
    
    // Now scan all history checkpoint files to collect their buckets
    let checkpoints = has.get_checkpoint_range();
    for checkpoint in checkpoints {
        let history_path = history_archive::checkpoint_path("history", checkpoint);
        
        // Try to read each history file
        match crate::archive::fetch_history_archive_state_path(op, &history_path).await {
            Ok(checkpoint_has) => {
                // Add all buckets from this checkpoint's history
                for hash in checkpoint_has.buckets() {
                    all_bucket_hashes.insert(hash);
                }
            }
            Err(e) => {
                // History file might be missing, which will be caught by the checkpoint file scan
                debug!("Could not read history file {}: {}", history_path, e);
            }
        }
    }

    if all_bucket_hashes.is_empty() {
        return Ok(0);
    }

    info!("Scanning {} unique buckets referenced by all history files", all_bucket_hashes.len());

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let found = Arc::new(AtomicU64::new(0));
    let missing = Arc::new(AtomicU64::new(0));

    let _results: Vec<_> = stream::iter(all_bucket_hashes)
        .map(|hash| {
            let op = op.clone();
            let semaphore = semaphore.clone();
            let found = found.clone();
            let missing = missing.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();

                if let Ok(path) = history_archive::bucket_path(&hash) {
                    match op.exists(&path).await {
                        Ok(true) => {
                            found.fetch_add(1, Ordering::Relaxed);
                            debug!("Found bucket: {}", path);
                        }
                        Ok(false) => {
                            missing.fetch_add(1, Ordering::Relaxed);
                            error!("Missing bucket: {}", path);
                        }
                        Err(e) => {
                            missing.fetch_add(1, Ordering::Relaxed);
                            error!("Error checking bucket {}: {}", path, e);
                        }
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let total_found = found.load(Ordering::Relaxed);
    let total_missing = missing.load(Ordering::Relaxed);

    if total_missing > 0 {
        error!("Missing {} buckets referenced by history files", total_missing);
        return Err(anyhow::anyhow!(
            "Missing {} buckets",
            total_missing
        ));
    }

    Ok(total_found)
}
