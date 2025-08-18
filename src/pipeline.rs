//! Pipeline for processing Stellar History Archives
//!
//! The pipeline coordinates parallel processing of archive checkpoints and their files.
//! It discovers checkpoints to process, distributes work to concurrent workers, and
//! delegates file processing to the provided Operation implementation.
//!
//! ## Architecture
//!
//! 1. **Checkpoint Discovery**: Reads the archive's HAS file to determine checkpoint range
//! 2. **Work Distribution**: Spawns workers that process checkpoints concurrently  
//! 3. **File Processing**: Each checkpoint's files (ledger, transactions, results, buckets)
//!    are processed in parallel with concurrency controlled by a semaphore
//! 4. **Operation Delegation**: The actual work (scan/mirror) is performed by the Operation trait
//!
//! The pipeline handles checkpoint enumeration, file discovery, bucket deduplication,
//! and concurrency management. Operations define what to do with each file.
use anyhow::Result;
use lru::LruCache;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::OnceCell;

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::history_file;
use crate::utils::fetch_history_archive_state;

/// Result from attempting to get a reader for an object
pub enum ReaderResult {
    /// Successfully obtained a reader
    Ok(crate::storage::BoxedAsyncRead),
    /// Failed to get reader (file missing or inaccessible)
    Err(anyhow::Error),
}

/// Simplified operation trait - operations only define how to process objects
#[async_trait::async_trait]
pub trait Operation: Send + Sync + 'static {
    /// Process an object
    /// The Pipeline provides either a reader that streams the object content,
    /// or an error if the reader couldn't be obtained (e.g., file not found)
    /// Operations should track failures internally and not propagate errors from individual files
    async fn process_object(&self, object: &str, reader_result: ReaderResult);

    /// Called when all work is complete
    /// The pipeline passes the highest checkpoint that was processed (if any)
    /// This is where operations should check their internal failure counts and return an error if needed
    async fn finalize(&self, highest_checkpoint: Option<u32>) -> Result<()>;
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
    /// Maximum number of HTTP retry attempts
    pub max_retries: u32,
    /// Initial backoff in milliseconds for HTTP retries
    pub initial_backoff_ms: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            source: String::new(),
            concurrency: 64,
            skip_optional: false,
            low: None,
            high: None,
            max_retries: 3,
            initial_backoff_ms: 100,
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
        let total_count = ((high - low) / history_file::CHECKPOINT_FREQUENCY + 1) as usize;
        Self {
            next: Arc::new(Mutex::new(low)),
            upper_bound: high,
            total_count,
        }
    }
}

/// Represents the source of data for a file fetching task.
enum FileTaskSource {
    /// Fetch from storage backend. This is used for all non-history files.
    Storage { is_optional: bool },
    /// Use already-buffered content. We use this for history files, since both
    /// the pipeline and the archival op need to read the file.
    Buffered(Vec<u8>),
    /// Report an error. This is only used for history files, where the previous
    /// Buffered download failed and we need to propagate the error to the archival op.
    Error(anyhow::Error),
}

pub struct Pipeline<Op: Operation> {
    /// The operation to execute on archive files
    pub operation: Arc<Op>,
    config: PipelineConfig,
    source_op: crate::utils::StorageRef,
    bucket_lru: Arc<Mutex<LruCache<String, ()>>>,
    highest_checkpoint: Arc<OnceCell<u32>>,
    /// Semaphore to limit total concurrent I/O operations
    io_permits: Arc<tokio::sync::Semaphore>,
    /// Counter for outstanding (spawned but not completed) download tasks
    inflight_files: Arc<std::sync::atomic::AtomicUsize>,
    /// Cached HAS from validation to avoid re-reading
    cached_has: Arc<OnceCell<crate::history_file::HistoryFileState>>,
}

impl<Op: Operation> Pipeline<Op> {
    /// Create a new pipeline
    pub async fn new(operation: Op, config: PipelineConfig) -> Result<Self> {
        // Create source storage backend from URL with retry config
        let retry_config = crate::storage::HttpRetryConfig {
            max_retries: config.max_retries,
            initial_backoff_ms: config.initial_backoff_ms,
        };
        let source_op =
            crate::storage::StorageBackend::from_url(&config.source, Some(retry_config)).await?;
        // Create LRU cache for bucket deduplication (bounded to 1 million entries)
        // 1 million entries * ~64 bytes per hash = ~64MB memory max
        let bucket_lru = Arc::new(Mutex::new(LruCache::new(
            std::num::NonZeroUsize::new(1_000_000).unwrap(),
        )));

        // Create semaphore to limit total concurrent I/O operations
        // This ensures we never have more than 'concurrency' I/O operations at once
        let io_permits = Arc::new(tokio::sync::Semaphore::new(config.concurrency));

        Ok(Self {
            operation: Arc::new(operation),
            source_op,
            config,
            bucket_lru,
            highest_checkpoint: Arc::new(OnceCell::new()),
            io_permits,
            inflight_files: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            cached_has: Arc::new(OnceCell::new()), // Will be populated during run() if validation passes
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        // Validate source and get HAS if available
        let cached_has =
            crate::utils::validate_source_and_get_has(&self.source_op, &self.config.source).await?;

        // Store the cached HAS if we got one from validation
        if let Some(has) = cached_has {
            self.cached_has.set(has).ok(); // Ignore error if already set
        }

        // Get checkpoint upper bound from source .well-known/stellar-history.json
        let (checkpoint_range, _archive_current) = self.compute_checkpoint_bounds().await?;
        if checkpoint_range.total_count == 0 {
            info!("No checkpoints to process");
            return Ok(());
        }

        // Store the highest checkpoint we'll be processing
        self.highest_checkpoint
            .set(checkpoint_range.upper_bound)
            .expect("highest_checkpoint should only be set once");

        // Create checkpoint channel for distributing work
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel(100);
        let checkpoint_rx = Arc::new(tokio::sync::Mutex::new(checkpoint_rx));

        let progress_tracker = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Create checkpoint processors that will take checkpoints from the channel
        // and spawn file download tasks for each checkpoint's files
        let num_checkpoint_processors = self.config.concurrency;
        debug!(
            "Starting {} checkpoint processors",
            num_checkpoint_processors
        );

        // Spawn checkpoint producer
        let range = checkpoint_range.clone();
        let producer = tokio::spawn(async move {
            // Get the starting checkpoint from the range
            let start = *range.next.lock().unwrap();
            let upper = range.upper_bound;
            let mut checkpoint = start;

            while checkpoint <= upper {
                if checkpoint_tx.send(checkpoint).await.is_err() {
                    // Channel closed, workers have stopped
                    break;
                }
                checkpoint += history_file::CHECKPOINT_FREQUENCY;
            }
            drop(checkpoint_tx); // Signal no more checkpoints
            debug!("Checkpoint producer finished");
        });

        let mut checkpoint_processors = Vec::new();

        // Since a single checkpoint may produce many file fetches, limit
        // number of outstanding file fetches to prevent unbounded memory growth.
        let max_outstanding = self.config.concurrency * 3;

        for processor_id in 0..num_checkpoint_processors {
            let pipeline = self.clone();
            let rx = checkpoint_rx.clone();
            let tracker = progress_tracker.clone();
            let total_count = checkpoint_range.total_count;
            let sem = self.io_permits.clone();

            let processor = tokio::spawn(async move {
                loop {
                    // Acquire permit before taking checkpoint work
                    let _permit = sem.acquire().await.unwrap();

                    // Check if we have too many outstanding downloads
                    let outstanding = pipeline
                        .inflight_files
                        .load(std::sync::atomic::Ordering::Relaxed);
                    if outstanding > max_outstanding {
                        // Too many tasks queued, release permit and yield
                        drop(_permit);
                        debug!(
                            "Processor {} yielding, {} outstanding downloads > {} limit",
                            processor_id, outstanding, max_outstanding
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        continue;
                    }

                    // Get next checkpoint from channel
                    let checkpoint = {
                        let mut rx = rx.lock().await;
                        match rx.recv().await {
                            Some(cp) => cp,
                            None => {
                                // No more work, release permit and exit
                                drop(_permit);
                                break;
                            }
                        }
                    };

                    debug!(
                        "Processor {} processing checkpoint {:08x} (outstanding: {})",
                        processor_id, checkpoint, outstanding
                    );

                    // Process this checkpoint (permit will be held until we've downloaded
                    // the checkpoint and spawned all the file fetching tasks).
                    pipeline.spawn_checkpoint_tasks(checkpoint, _permit).await;

                    let total = tracker.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                    if total % 10 == 0 || total == total_count {
                        info!("Progress: {}/{} checkpoints processed", total, total_count);
                    }
                }
                debug!("Processor {} finished", processor_id);
            });
            checkpoint_processors.push(processor);
        }

        // Wait for producer to finish sending all checkpoints
        producer.await?;

        // Wait for all checkpoint processors to finish spawning file tasks
        for processor in checkpoint_processors {
            processor.await?;
        }

        // At this point, we've finished processing all the checkpoint files,
        // but we still have to wait for outstanding file tasks to complete.
        let mut wait_count = 0;
        loop {
            let outstanding = self
                .inflight_files
                .load(std::sync::atomic::Ordering::Relaxed);
            if outstanding == 0 {
                break;
            }
            if wait_count % 100 == 0 {
                debug!(
                    "Waiting for {} outstanding file tasks to complete",
                    outstanding
                );
            }
            wait_count += 1;
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        debug!("All file tasks completed");

        // Finalize operation with the highest checkpoint we processed
        let highest_checkpoint = self.highest_checkpoint.get().copied();
        self.operation.finalize(highest_checkpoint).await?;

        Ok(())
    }

    /// Compute checkpoint bounds from HAS (returns range and current ledger)
    async fn compute_checkpoint_bounds(&self) -> Result<(CheckpointRange, u32)> {
        // Use cached HAS if available, otherwise fetch it
        let has = if let Some(cached) = self.cached_has.get() {
            cached.clone()
        } else {
            fetch_history_archive_state(&self.source_op).await?
        };
        let current_ledger = has.current_ledger;
        let current_checkpoint = history_file::checkpoint_number(current_ledger);

        info!(
            "Archive reports current ledger: {} (checkpoint: 0x{:08x})",
            current_ledger, current_checkpoint
        );

        // Determine the low checkpoint (starting point)
        let low_checkpoint = if let Some(low) = self.config.low {
            let low_checkpoint = history_file::checkpoint_number(low);

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
            history_file::CHECKPOINT_FREQUENCY - 1
        };

        // Determine the high checkpoint (ending point)
        let high_checkpoint = if let Some(high) = self.config.high {
            let high_checkpoint = history_file::checkpoint_number(high);

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
        let first_checkpoint = if low_checkpoint < history_file::CHECKPOINT_FREQUENCY - 1 {
            history_file::CHECKPOINT_FREQUENCY - 1
        } else {
            // Round up to next checkpoint boundary if needed
            let remainder = (low_checkpoint + 1) % history_file::CHECKPOINT_FREQUENCY;
            if remainder == 0 {
                low_checkpoint
            } else {
                low_checkpoint + (history_file::CHECKPOINT_FREQUENCY - remainder)
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

    /// Unified helper to spawn a file processing task with proper concurrency control
    fn spawn_file_task(&self, path: String, source: FileTaskSource) {
        let op = self.operation.clone();
        let sem = self.io_permits.clone();
        let outstanding = self.inflight_files.clone();
        let src = self.source_op.clone();

        // Increment outstanding counter before spawning
        outstanding.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        tokio::spawn(async move {
            // Acquire semaphore permit before processing
            let _permit = sem.acquire().await.unwrap();

            // Get or create the reader based on the source
            let reader_result = match source {
                // Open a reader stream for the file from backend storage
                FileTaskSource::Storage { is_optional } => {
                    // Fetch from storage
                    match src.open_reader(&path).await {
                        Ok(reader) => ReaderResult::Ok(reader),
                        Err(e) => {
                            if is_optional {
                                debug!("Failed to get reader for optional {}: {}", path, e);
                            } else {
                                error!("Failed to get reader for {}: {}", path, e);
                            }
                            ReaderResult::Err(e.into())
                        }
                    }
                }
                // Use already-buffered content
                FileTaskSource::Buffered(buffer) => {
                    let reader =
                        Box::new(std::io::Cursor::new(buffer)) as crate::storage::BoxedAsyncRead;
                    ReaderResult::Ok(reader)
                }
                // We tried to fetch this file earlier but failed, so just pass the error to the op.
                FileTaskSource::Error(error) => ReaderResult::Err(error),
            };

            op.process_object(&path, reader_result).await;

            // Decrement outstanding counter when done
            outstanding.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    /// Spawn tasks for processing a checkpoint. This has three steps:
    /// 1. Fetch and buffer the history file for the checkpoint.
    /// 2. Parse the history file to see what else we need to process for the checkpoint.
    /// 3. Fetch and spawn workers for the remaining files.
    ///
    /// The permit ensures we don't spawn too many checkpoint processing tasks at once.
    async fn spawn_checkpoint_tasks(
        &self,
        checkpoint: u32,
        _permit: tokio::sync::SemaphorePermit<'_>,
    ) {
        let source_op = self.source_op.clone();
        let bucket_lru = self.bucket_lru.clone();

        let history_path = history_file::checkpoint_path("history", checkpoint);
        let history_path_for_fetch = history_path.clone();
        let source_op_for_history = source_op.clone();

        let history_future = async move {
            use tokio::io::AsyncReadExt;

            // Download the history file into a buffer. We'll need to process it to generate
            // file fetching work and also forward a stream iterator to the archival op. Buffer
            // so we don't have to re-download the file for the archival op.
            let mut reader = source_op_for_history
                .open_reader(&history_path_for_fetch)
                .await?;
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await?;

            let has: crate::history_file::HistoryFileState = serde_json::from_slice(&buffer)?;
            has.validate()
                .map_err(|e| anyhow::anyhow!("Invalid HAS format: {}", e))?;
            Ok::<(crate::history_file::HistoryFileState, Vec<u8>), anyhow::Error>((has, buffer))
        };

        // Start fetching other checkpoint files (ledger, transactions, results)
        for category in &["ledger", "transactions", "results"] {
            let path = history_file::checkpoint_path(category, checkpoint);
            self.spawn_file_task(path, FileTaskSource::Storage { is_optional: false });
        }

        // Add optional SCP files if not skipping
        if !self.config.skip_optional {
            let path = history_file::checkpoint_path("scp", checkpoint);
            self.spawn_file_task(path, FileTaskSource::Storage { is_optional: true });
        }

        // Wait for history JSON to be downloaded and parsed
        match history_future.await {
            Ok((checkpoint_has, buffer)) => {
                // Process the history file using the buffered content
                self.spawn_file_task(history_path.clone(), FileTaskSource::Buffered(buffer));

                // We have the parsed HAS for bucket processing
                // Now start fetching all bucket files concurrently
                let mut bucket_count = 0;
                let mut dedup_count = 0;
                for bucket_hash in checkpoint_has.buckets() {
                    if let Ok(path) = history_file::bucket_path(&bucket_hash) {
                        bucket_count += 1;
                        // Dedup buckets using cache
                        {
                            let mut cache = bucket_lru.lock().unwrap();
                            if cache.put(bucket_hash.clone(), ()).is_some() {
                                // Bucket already fetched, skip
                                dedup_count += 1;
                                continue;
                            }
                        }

                        self.spawn_file_task(path, FileTaskSource::Storage { is_optional: false });
                    }
                }

                if dedup_count > 0 {
                    debug!(
                        "Checkpoint {:08x}: {} buckets, {} deduplicated by LRU cache",
                        checkpoint, bucket_count, dedup_count
                    );
                }
            }
            Err(e) => {
                debug!(
                    "Failed to fetch history JSON for checkpoint {:08x}: {}",
                    checkpoint, e
                );

                // Forward error to op since the history file was fetched by the pipeline,
                // not the archival op.
                let err = anyhow::anyhow!("Failed to download/parse history file: {}", e);
                self.spawn_file_task(history_path, FileTaskSource::Error(err));
            }
        }
    }
}

// Re-export async_trait
pub use async_trait::async_trait;
