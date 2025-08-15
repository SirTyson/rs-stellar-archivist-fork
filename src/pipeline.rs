//! Simplified pipeline architecture for archive operations
//!
//! This module provides a streamlined three-stage pipeline that handles
//! the common logic for all archive operations.
//!
//! ## Pipeline Architecture
//!
//! ### Stage 1: Checkpoint Discovery
//! - Fetches the archive's History Archive State (HAS) from `.well-known/stellar-history.json`
//! - Determines which checkpoints to process based on:
//!   - Archive's current state (latest checkpoint available)
//!   - User-specified bounds (`--low` and `--high` flags)
//! - Generates a list of checkpoint numbers to process
//!
//! ### Stage 2: Sequential Checkpoint Processing with Bounded File Concurrency
//! - Processes checkpoints one at a time to prevent runaway concurrency
//! - For each checkpoint:
//!   - Fetches and parses the checkpoint's `history-XXXXXXXX.json` file
//!   - Extracts the list of bucket hashes referenced by this checkpoint
//!   - Spawns tasks for all files in the checkpoint (history, ledger, transactions, results, buckets)
//!   - Each file task must acquire a semaphore permit before processing
//!   - The semaphore has exactly `concurrency` permits, ensuring at most `concurrency` files
//!     are being downloaded/processed at any given moment
//!   - Waits for all files in the checkpoint to complete before moving to the next checkpoint
//!
//! ### Stage 3: Result Collection
//! - Collects results from all workers via an mpsc channel
//! - Delegates result handling to the operation implementation
//! - Tracks progress and reports completion statistics
//!
//! ## Responsibilities
//!
//! **What the Pipeline IS responsible for:**
//! - **Checkpoint enumeration**: Determining which checkpoints exist and need processing
//! - **Work distribution**: Efficiently distributing checkpoints among workers
//! - **File discovery**: Parsing checkpoint JSON files to find all referenced files
//! - **Bucket deduplication**: Ensuring each unique bucket is only processed once
//! - **Concurrency management**: Coordinating parallel workers and concurrent I/O
//! - **Progress tracking**: Monitoring and reporting processing progress
//! - **Result aggregation**: Collecting results from all workers
//!
//! **What the Pipeline is NOT responsible for:**
//! - **File operations**: The actual fetching/writing is handled by `fetcher`
//! - **Business logic**: Operations (scan/mirror) define what to do with each file
//! - **Storage abstraction**: `fetcher` provides unified storage interface
//!
use anyhow::Result;
use dashmap::DashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::fetcher::{
    fetch_history_archive_state, fetch_history_archive_state_path, new_operator,
    StorageRef as Storage,
};
use crate::history_archive::{self, HistoryArchiveState};

/// Result of processing a work item
#[derive(Debug, Clone)]
pub enum ProcessResult {
    /// Successfully processed
    Success,
    /// Failed to process with error
    Failed(String),
    /// Skipped processing (e.g., file exists)
    Skipped,
}

// WorkItem struct removed - no longer needed with direct checkpoint processing

/// Simplified operation trait - operations only define how to process files
#[async_trait::async_trait]
pub trait ArchiveOperation: Send + Sync + 'static {
    /// Process a single file
    async fn process_file(&self, path: &str, is_optional: bool) -> Result<ProcessResult>;

    /// Handle the result of processing a file
    async fn handle_result(&self, path: &str, result: &ProcessResult) -> Result<()>;

    /// Called when all work is complete
    async fn finalize(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Source archive URL
    pub source: String,
    /// Number of concurrent workers for processing
    pub concurrency: usize,
    /// Whether to skip optional files
    pub skip_optional: bool,
    /// Optional low ledger limit (start from this ledger)
    pub low: Option<u32>,
    /// Optional high ledger limit (stop at this ledger)
    pub high: Option<u32>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            source: String::new(),
            concurrency: 64,
            skip_optional: false,
            low: None,
            high: None,
        }
    }
}

/// Checkpoint range for processing
#[derive(Debug, Clone)]
struct CheckpointRange {
    /// Next checkpoint to process (mutex-guarded)
    next: Arc<Mutex<u32>>,
    /// Upper bound (inclusive)
    upper_bound: u32,
    /// Total number of checkpoints for progress tracking
    total_count: usize,
}

impl CheckpointRange {
    fn new(low: u32, high: u32) -> Self {
        // Calculate total count for progress tracking
        let total_count = ((high - low) / history_archive::CHECKPOINT_FREQUENCY + 1) as usize;
        Self {
            next: Arc::new(Mutex::new(low)),
            upper_bound: high,
            total_count,
        }
    }

    /// Get the next checkpoint to process, or None if we've reached the upper bound
    fn get_next(&self) -> Option<u32> {
        let mut next = self.next.lock().unwrap();
        if *next > self.upper_bound {
            None
        } else {
            let checkpoint = *next;
            *next += history_archive::CHECKPOINT_FREQUENCY;
            Some(checkpoint)
        }
    }
}

pub struct Pipeline<Op: ArchiveOperation> {
    /// The operation to execute on archive files
    pub operation: Arc<Op>,
    config: PipelineConfig,
    source_op: Storage,
    dedup_cache: Arc<DashSet<String>>,
    has: Arc<tokio::sync::RwLock<Option<HistoryArchiveState>>>,
    highest_checkpoint: Arc<tokio::sync::RwLock<Option<u32>>>,
    /// Semaphore to limit total concurrent file operations
    file_semaphore: Arc<tokio::sync::Semaphore>,
}

impl<Op: ArchiveOperation> Pipeline<Op> {
    /// Create a new pipeline with a provided storage operator
    pub async fn new_with_storage(
        operation: Op,
        config: PipelineConfig,
        source_op: Storage,
    ) -> Result<Self> {
        // Create concurrent dedup set
        let dedup_cache = Arc::new(DashSet::new());

        // Create semaphore to limit total concurrent file operations
        // This ensures we never have more than 'concurrency' files being processed at once
        let file_semaphore = Arc::new(tokio::sync::Semaphore::new(config.concurrency));

        Ok(Self {
            operation: Arc::new(operation),
            source_op,
            config,
            dedup_cache,
            has: Arc::new(tokio::sync::RwLock::new(None)),
            highest_checkpoint: Arc::new(tokio::sync::RwLock::new(None)),
            file_semaphore,
        })
    }

    /// Create a new pipeline (deprecated - creates its own operator)
    pub async fn new(operation: Op, config: PipelineConfig) -> Result<Self> {
        let source_op = new_operator(&config.source).await?;
        Self::new_with_storage(operation, config, source_op).await
    }

    /// Run the pipeline
    pub async fn run(self: Arc<Self>) -> Result<()> {
        // Get checkpoint bounds (this fetches HAS internally)
        let (checkpoint_range, _archive_current) = self.get_checkpoint_bounds().await?;

        if checkpoint_range.total_count == 0 {
            info!("No checkpoints to process");
            return Ok(());
        }

        // Store the highest checkpoint we'll be processing
        *self.highest_checkpoint.write().await = Some(checkpoint_range.upper_bound);

        // Create result channel
        let (result_tx, mut result_rx) = mpsc::channel(1000);

        // Create progress tracker
        let progress_tracker = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Spawn a SINGLE task that processes checkpoints sequentially
        // but processes files within each checkpoint concurrently
        let pipeline = self.clone();
        let range = checkpoint_range.clone();
        let tx = result_tx.clone();
        let tracker = progress_tracker.clone();

        let checkpoint_processor = tokio::spawn(async move {
            let mut checkpoints_processed = 0u32;

            loop {
                // Get next checkpoint to process (mutex-guarded increment)
                if let Some(checkpoint) = range.get_next() {
                    debug!("Processing checkpoint {:08x}", checkpoint);

                    // Process this checkpoint with bounded concurrency
                    let start = std::time::Instant::now();

                    match pipeline.process_checkpoint(checkpoint, &tx).await {
                        Ok(_) => {
                            checkpoints_processed += 1;
                            let total =
                                tracker.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                            if total % 10 == 0 || total == range.total_count {
                                info!(
                                    "Progress: {}/{} checkpoints processed",
                                    total, range.total_count
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed on checkpoint {:08x}: {}", checkpoint, e);
                        }
                    }

                    let elapsed = start.elapsed();
                    if elapsed.as_millis() > 100 {
                        debug!(
                            "Checkpoint {:08x} took {}ms",
                            checkpoint,
                            elapsed.as_millis()
                        );
                    }
                } else {
                    // No more work available
                    break;
                }
            }

            debug!("Finished processing {} checkpoints", checkpoints_processed);
            Ok::<(), anyhow::Error>(())
        });

        // Drop the sender so result collection knows when to stop
        drop(result_tx);

        // Collect results
        let mut total_processed = 0;
        let mut total_failed = 0;
        let mut total_skipped = 0;

        while let Some((path, result)) = result_rx.recv().await {
            total_processed += 1;

            match &result {
                ProcessResult::Success => {}
                ProcessResult::Failed(_) => total_failed += 1,
                ProcessResult::Skipped => total_skipped += 1,
            }

            // Let the operation handle the result
            self.operation.handle_result(&path, &result).await?;
        }

        // Wait for checkpoint processor to complete
        checkpoint_processor.await??;

        info!(
            "Pipeline complete: {} files processed, {} failed, {} skipped",
            total_processed, total_failed, total_skipped
        );

        // Finalize operation
        self.operation.finalize().await?;

        Ok(())
    }

    /// Get the HAS and return our copy
    pub async fn get_has(&self) -> Option<HistoryArchiveState> {
        self.has.read().await.clone()
    }

    /// Get the highest checkpoint we're processing
    pub async fn get_highest_checkpoint(&self) -> Option<u32> {
        *self.highest_checkpoint.read().await
    }

    /// Determine checkpoint bounds from HAS (returns range and current ledger)
    async fn get_checkpoint_bounds(&self) -> Result<(CheckpointRange, u32)> {
        // Fetch the HAS
        let has = fetch_history_archive_state(&self.source_op).await?;
        let current_ledger = has.current_ledger;
        let current_checkpoint = history_archive::checkpoint_number(current_ledger);

        info!(
            "Archive reports current ledger: {} (checkpoint: 0x{:08x})",
            current_ledger, current_checkpoint
        );

        // Store HAS for later use
        *self.has.write().await = Some(has.clone());

        // Determine the low checkpoint (starting point)
        let low_checkpoint = if let Some(low) = self.config.low {
            let low_checkpoint = history_archive::checkpoint_number(low);

            // Edge case: Check if the .well-known file's current checkpoint is below low
            if current_checkpoint < low_checkpoint {
                anyhow::bail!(
                    "No checkpoints above the lower bound: archive's latest checkpoint 0x{:08x} (ledger {}) is below requested low 0x{:08x} (ledger {})",
                    current_checkpoint, current_ledger, low_checkpoint, low
                );
            }

            low_checkpoint
        } else {
            // Default to first checkpoint (63)
            history_archive::CHECKPOINT_FREQUENCY - 1
        };

        // Determine the high checkpoint (ending point)
        let high_checkpoint = if let Some(high) = self.config.high {
            let high_checkpoint = history_archive::checkpoint_number(high);

            // Edge case: Warn if the latest checkpoint is below high, but continue
            if current_checkpoint < high_checkpoint {
                warn!(
                    "Archive's latest checkpoint 0x{:08x} (ledger {}) is below requested high 0x{:08x} (ledger {}), will scan up to latest available",
                    current_checkpoint, current_ledger, high_checkpoint, high
                );
                current_checkpoint
            } else {
                high_checkpoint
            }
        } else {
            current_checkpoint
        };

        // Validate that low <= high
        if low_checkpoint > high_checkpoint {
            anyhow::bail!(
                "Low checkpoint 0x{:08x} is greater than high checkpoint 0x{:08x}",
                low_checkpoint,
                high_checkpoint
            );
        }

        // Validate that checkpoints exist in the range
        // The actual checkpoints must be aligned to 63, 127, 191, etc.
        let first_checkpoint = if low_checkpoint < history_archive::CHECKPOINT_FREQUENCY - 1 {
            history_archive::CHECKPOINT_FREQUENCY - 1
        } else {
            // Round up to next checkpoint boundary if needed
            let remainder = (low_checkpoint + 1) % history_archive::CHECKPOINT_FREQUENCY;
            if remainder == 0 {
                low_checkpoint
            } else {
                low_checkpoint + (history_archive::CHECKPOINT_FREQUENCY - remainder)
            }
        };

        // Make sure we have at least one checkpoint in range
        if first_checkpoint > high_checkpoint {
            anyhow::bail!(
                "No checkpoints found in range 0x{:08x} to 0x{:08x}",
                low_checkpoint,
                high_checkpoint
            );
        }

        let checkpoint_range = CheckpointRange::new(first_checkpoint, high_checkpoint);

        info!(
            "Processing {} checkpoints from 0x{:08x} to 0x{:08x}",
            checkpoint_range.total_count, first_checkpoint, high_checkpoint
        );

        Ok((checkpoint_range, current_ledger))
    }

    /// Process checkpoint - now CONCURRENT but with bounded concurrency!
    async fn process_checkpoint(
        &self,
        checkpoint: u32,
        result_tx: &mpsc::Sender<(String, ProcessResult)>,
    ) -> Result<()> {
        let checkpoint_start = std::time::Instant::now();

        // Start ALL file downloads concurrently (but limited by semaphore!)
        let mut tasks = Vec::new();

        // Clone what we need for async operations
        let operation = self.operation.clone();
        let source_op = self.source_op.clone();
        let dedup_cache = self.dedup_cache.clone();
        let file_semaphore = self.file_semaphore.clone();

        // Download history.json ONCE and parse it for buckets
        let history_path = history_archive::checkpoint_path("history", checkpoint);
        let history_path_for_fetch = history_path.clone();
        let source_op_for_history = source_op.clone();

        let history_future = async move {
            fetch_history_archive_state_path(&source_op_for_history, &history_path_for_fetch).await
        };

        // Start downloading other checkpoint files (ledger, transactions, results)
        for category in &["ledger", "transactions", "results"] {
            let path = history_archive::checkpoint_path(category, checkpoint);
            let op = operation.clone();
            let tx = result_tx.clone();
            let sem = file_semaphore.clone();

            tasks.push(tokio::spawn(async move {
                // Acquire semaphore permit before processing
                let _permit = sem.acquire().await.unwrap();

                let result = match op.process_file(&path, false).await {
                    Ok(result) => result,
                    Err(e) => {
                        error!("Failed to process {}: {}", path, e);
                        ProcessResult::Failed(e.to_string())
                    }
                };

                let _ = tx.send((path.clone(), result.clone())).await;
                (path, result)
            }));
        }

        // Add optional SCP if not skipping
        if !self.config.skip_optional {
            let path = history_archive::checkpoint_path("scp", checkpoint);
            let op = operation.clone();
            let tx = result_tx.clone();
            let sem = file_semaphore.clone();

            tasks.push(tokio::spawn(async move {
                // Acquire semaphore permit before processing
                let _permit = sem.acquire().await.unwrap();

                let result = match op.process_file(&path, true).await {
                    Ok(result) => result,
                    Err(e) => {
                        debug!("Failed to process optional {}: {}", path, e);
                        ProcessResult::Skipped
                    }
                };

                let _ = tx.send((path.clone(), result.clone())).await;
                (path, result)
            }));
        }

        // Process history.json itself as a file
        {
            let op = operation.clone();
            let tx = result_tx.clone();
            let history_path_clone = history_path.clone();
            let sem = file_semaphore.clone();

            tasks.push(tokio::spawn(async move {
                // Acquire semaphore permit before processing
                let _permit = sem.acquire().await.unwrap();

                let result = match op.process_file(&history_path_clone, false).await {
                    Ok(result) => result,
                    Err(e) => {
                        error!("Failed to process {}: {}", history_path_clone, e);
                        ProcessResult::Failed(e.to_string())
                    }
                };

                let _ = tx.send((history_path_clone.clone(), result.clone())).await;
                (history_path_clone, result)
            }));
        }

        // Wait for history JSON to be fetched (for bucket list)
        match history_future.await {
            Ok(checkpoint_has) => {
                // Now start downloading all bucket files concurrently
                for bucket_hash in checkpoint_has.buckets() {
                    if let Ok(path) = history_archive::bucket_path(&bucket_hash) {
                        // Dedup check
                        let dedup_key = format!("bucket:{}", bucket_hash);
                        if !dedup_cache.insert(dedup_key) {
                            continue; // Already processed
                        }

                        let op = operation.clone();
                        let tx = result_tx.clone();
                        let sem = file_semaphore.clone();

                        tasks.push(tokio::spawn(async move {
                            // Acquire semaphore permit before processing
                            let _permit = sem.acquire().await.unwrap();

                            let result = match op.process_file(&path, false).await {
                                Ok(result) => result,
                                Err(e) => {
                                    error!("Failed to process {}: {}", path, e);
                                    ProcessResult::Failed(e.to_string())
                                }
                            };

                            let _ = tx.send((path.clone(), result.clone())).await;
                            (path, result)
                        }));
                    }
                }
            }
            Err(e) => {
                debug!(
                    "Failed to fetch history JSON for checkpoint {:08x}: {}",
                    checkpoint, e
                );
                // Continue anyway - other files are still being processed
            }
        }

        // Wait for all downloads to complete
        for task in tasks {
            let _ = task.await;
        }

        let elapsed = checkpoint_start.elapsed();
        if elapsed.as_millis() > 100 {
            tracing::debug!(
                "Checkpoint {:08x} took {}ms (concurrent processing with semaphore)",
                checkpoint,
                elapsed.as_millis()
            );
        }
        Ok(())
    }

    /// Get bucket dedup key from path
    #[allow(dead_code)]
    fn get_bucket_dedup_key(&self, path: &str) -> Option<String> {
        path.split('/')
            .last()
            .and_then(|name| name.strip_suffix(".xdr.gz"))
            .and_then(|name| name.strip_prefix("bucket-"))
            .map(|hash| format!("bucket:{}", hash))
    }
}

// Re-export async_trait
pub use async_trait::async_trait;
