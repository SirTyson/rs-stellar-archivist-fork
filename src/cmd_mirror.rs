use crate::archive::new_operator;
use crate::history_archive;
use anyhow::{Context, Result};
use dashmap::DashSet;
use futures::stream::{self, StreamExt};


use moka::future::Cache;
use opendal::Operator;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// Configuration constants
const DEFAULT_WINDOW_SIZE: usize = 500; // Checkpoints per window
const DEFAULT_MAX_BUCKET_CACHE: usize = 1_600_000; // ~100MB worth of hashes (64 bytes per hash)

// Task queue sizing:
// Each checkpoint generates ~9 tasks on average:
//   - 5 checkpoint files (history, ledger, transactions, results, scp)
//   - 4 unique bucket files (after deduplication)
// For a full window of 500 checkpoints: 500 * 9 = 4500 tasks
// We want to buffer ~2 windows worth to keep pipeline flowing: 9000 tasks
const DEFAULT_TASK_QUEUE_SIZE: usize = 9000;

pub struct MirrorConfig {
    pub src: String,
    pub dst: String,
    pub concurrency: usize,
    pub skip_optional: bool,
    pub high: Option<u32>,
    pub force: bool,
    // Streaming options
    pub window_size: Option<usize>,
    pub max_bucket_cache: Option<usize>,
    pub window_workers: Option<usize>,
}

// Task types for better progress tracking
#[derive(Debug, Clone)]
enum MirrorTask {
    Bucket {
        hash: String,
        path: String,
    },
    CheckpointFile {
        checkpoint: u32,
        category: String,
        path: String,
    },
}

#[derive(Debug)]
enum CopyResult {
    Copied,
    Skipped,
}

// Progress tracking
struct ProgressTracker {
    total_windows: usize,
    completed_windows: AtomicUsize,
    total_checkpoints: usize,
    processed_checkpoints: AtomicUsize,
    copied_files: AtomicU64,
    skipped_files: AtomicU64,
    failed_files: AtomicU64,
}

impl ProgressTracker {
    fn new(total_windows: usize, total_checkpoints: usize) -> Self {
        Self {
            total_windows,
            completed_windows: AtomicUsize::new(0),
            total_checkpoints,
            processed_checkpoints: AtomicUsize::new(0),
            copied_files: AtomicU64::new(0),
            skipped_files: AtomicU64::new(0),
            failed_files: AtomicU64::new(0),
        }
    }

    fn report_window_complete(&self, _window_num: usize, window_range: Range<u32>) {
        let completed = self.completed_windows.fetch_add(1, Ordering::Relaxed) + 1;
        let percent = (completed * 100) / self.total_windows;
        info!(
            "Window {}/{} complete ({}%) - checkpoints [{:08x}..{:08x}]",
            completed, self.total_windows, percent, window_range.start, window_range.end
        );
    }

    fn report_checkpoint_progress(&self) {
        let processed = self.processed_checkpoints.load(Ordering::Relaxed);
        if processed % 100 == 0 || processed == self.total_checkpoints {
            let percent = (processed * 100) / self.total_checkpoints;
            info!(
                "Checkpoints: {}/{} ({}%)",
                processed, self.total_checkpoints, percent
            );
        }
    }
}

/// Mirror archives using a three-stage concurrent pipeline:
///
/// Stage 1: Window Generator (Single producer)
/// - Splits checkpoints into windows (chunks of ~500 checkpoints)
/// - Sends windows to channel for processing (buffer: 4 windows)
///
/// Stage 2: Window Processors (2 workers by default)  
/// - Up to 2 windows processed in parallel
/// - Parse history files to extract bucket hashes
/// - Deduplicate buckets using global cache
/// - Queue copy tasks for Stage 3 (buffer: ~9000 tasks, ~2 windows worth)
///
/// Stage 3: Copy Workers (Multiple workers, default: 16)
/// - Pull copy tasks from queue
/// - Stream files from source to destination
/// - Track progress (copied/skipped/failed)
///
/// Backpressure mechanism:
/// - Window channel (buffer=4): Stage 1 blocks if Stage 2 falls behind
/// - Task channel (buffer=9000): Stage 2 blocks if Stage 3 falls behind
/// - When task queue fills, window processors pause, preventing runaway memory use
/// - Buffer sized for ~2 windows of tasks to maintain smooth pipeline flow
///
/// This design enables:
/// - Pipeline parallelism (all stages run concurrently)
/// - Memory control via channel backpressure (not window size directly)
/// - Efficient deduplication (buckets copied only once)
/// - High throughput (many workers at each stage)
pub async fn run(config: MirrorConfig) -> Result<()> {
    let src_op = new_operator(&config.src)?;
    let dst_op = new_operator(&config.dst)?;

    if !matches!(dst_op.info().scheme(), opendal::Scheme::Fs) {
        anyhow::bail!("Destination must be a filesystem path (file://...)");
    }

    info!("Starting mirror from {} to {}", config.src, config.dst);

    // Fetch root HAS to determine range
    let has = crate::archive::fetch_history_archive_state(&src_op).await?;
    let max_checkpoint = config
        .high
        .unwrap_or_else(|| history_archive::checkpoint_number(has.current_ledger));

    // Get checkpoint range to process
    let checkpoints = has.get_checkpoint_range();
    let checkpoints: Vec<u32> = checkpoints
        .into_iter()
        .filter(|&cp| cp <= max_checkpoint)
        .collect();

    if checkpoints.is_empty() {
        info!("No checkpoints to mirror");
        return Ok(());
    }

    let num_checkpoints = checkpoints.len();
    let first_checkpoint = checkpoints.first().copied().unwrap_or(0);
    let last_checkpoint = checkpoints.last().copied().unwrap_or(0);

    info!(
        "Mirroring {} checkpoints in range [{:08x}..{:08x}]",
        num_checkpoints, first_checkpoint, last_checkpoint
    );

    // Configuration
    let window_size = config.window_size.unwrap_or(DEFAULT_WINDOW_SIZE);
    let max_bucket_cache = config.max_bucket_cache.unwrap_or(DEFAULT_MAX_BUCKET_CACHE);
    
    // Window workers should be limited by our task buffer size
    // We buffer ~2 windows worth of tasks (9000), so having more than 2-3 window
    // processors would defeat our backpressure mechanism. Use 2 workers to allow
    // for some parallelism while maintaining controlled memory usage.
    let window_workers = config
        .window_workers
        .unwrap_or(2);

    info!(
        "Configuration: window_size={}, cache_size={}, window_workers={}",
        window_size, max_bucket_cache, window_workers
    );

    // Create checkpoint windows
    let windows = create_checkpoint_windows(&checkpoints, window_size);
    let total_windows = windows.len();

    info!(
        "Processing {} windows of up to {} checkpoints each",
        total_windows, window_size
    );

    // Initialize components
    // Create a Moka cache for global bucket deduplication (lock-free, concurrent)
    let global_cache: Arc<Cache<String, ()>> = Arc::new(
        Cache::builder()
            .max_capacity(max_bucket_cache as u64)
            .build(),
    );
    let progress = Arc::new(ProgressTracker::new(total_windows, num_checkpoints));

    // Channels
    let (window_tx, window_rx) = mpsc::channel::<(usize, Vec<u32>)>(4); // Small buffer for windows
    let (task_tx, task_rx) = mpsc::channel::<MirrorTask>(DEFAULT_TASK_QUEUE_SIZE); // Buffer ~2 windows of tasks

    // Stage 1: Window generator
    let window_generator = {
        let windows = windows.clone();

        tokio::spawn(async move {
            // Send windows for processing
            // All buckets will be collected from checkpoint history files
            for (idx, window) in windows.into_iter().enumerate() {
                if window_tx.send((idx, window)).await.is_err() {
                    break; // Receiver dropped
                }
            }
            debug!("Window generator complete");
        })
    };

    // Stage 2: Window processors
    let window_rx = Arc::new(tokio::sync::Mutex::new(window_rx));
    let mut window_processors = Vec::new();

    for worker_id in 0..window_workers {
        let window_rx = window_rx.clone();
        let task_tx = task_tx.clone();
        let src_op = src_op.clone();
        let global_cache = global_cache.clone();
        let progress = progress.clone();
        let skip_optional = config.skip_optional;

        let handle = tokio::spawn(async move {
            loop {
                let window_data = {
                    let mut rx = window_rx.lock().await;
                    rx.recv().await
                };

                let Some((window_idx, window_checkpoints)) = window_data else {
                    debug!("Window processor {} exiting", worker_id);
                    break;
                };

                let window_start = *window_checkpoints.first().unwrap_or(&0);
                let window_end = *window_checkpoints.last().unwrap_or(&0);

                debug!(
                    "Worker {} processing window {} [{:08x}..{:08x}] with {} checkpoints",
                    worker_id,
                    window_idx,
                    window_start,
                    window_end,
                    window_checkpoints.len()
                );

                // Process window
                if let Err(e) = process_window(
                    window_checkpoints,
                    &src_op,
                    &task_tx,
                    &global_cache,
                    skip_optional,
                    &progress,
                )
                .await
                {
                    error!("Error processing window {}: {}", window_idx, e);
                }

                progress.report_window_complete(window_idx, window_start..window_end);
            }

            debug!("Window processor {} finished", worker_id);
        });

        window_processors.push(handle);
    }

    // Drop our task_tx so channel closes when processors finish
    drop(task_tx);

    // Stage 3: Copy workers
    let task_rx = Arc::new(tokio::sync::Mutex::new(task_rx));
    let mut copy_workers = Vec::new();

    for worker_id in 0..config.concurrency {
        let task_rx = task_rx.clone();
        let src_op = src_op.clone();
        let dst_op = dst_op.clone();
        let progress = progress.clone();
        let force = config.force;

        let handle = tokio::spawn(async move {
            loop {
                let task = {
                    let mut rx = task_rx.lock().await;
                    rx.recv().await
                };

                let Some(task) = task else {
                    debug!("Copy worker {} exiting", worker_id);
                    break;
                };

                match task {
                    MirrorTask::Bucket { hash, path } => {
                        match copy_file(&src_op, &dst_op, &path, force).await {
                            Ok(CopyResult::Copied) => {
                                progress.copied_files.fetch_add(1, Ordering::Relaxed);
                                debug!("Worker {}: Copied bucket {}", worker_id, hash);
                            }
                            Ok(CopyResult::Skipped) => {
                                progress.skipped_files.fetch_add(1, Ordering::Relaxed);
                                debug!("Worker {}: Skipped bucket {}", worker_id, hash);
                            }
                            Err(e) => {
                                progress.failed_files.fetch_add(1, Ordering::Relaxed);
                                error!(
                                    "Worker {}: Failed to copy bucket {}: {}",
                                    worker_id, hash, e
                                );
                            }
                        }
                    }

                    MirrorTask::CheckpointFile {
                        checkpoint,
                        category,
                        path,
                    } => match copy_file(&src_op, &dst_op, &path, force).await {
                        Ok(CopyResult::Copied) => {
                            progress.copied_files.fetch_add(1, Ordering::Relaxed);
                            debug!(
                                "Worker {}: Copied {} for checkpoint {:08x}",
                                worker_id, category, checkpoint
                            );
                        }
                        Ok(CopyResult::Skipped) => {
                            progress.skipped_files.fetch_add(1, Ordering::Relaxed);
                            debug!(
                                "Worker {}: Skipped {} for checkpoint {:08x}",
                                worker_id, category, checkpoint
                            );
                        }
                        Err(e) => {
                            progress.failed_files.fetch_add(1, Ordering::Relaxed);
                            if category == "scp" {
                                warn!(
                                    "Worker {}: Failed to copy optional {}: {}",
                                    worker_id, path, e
                                );
                            } else {
                                error!("Worker {}: Failed to copy {}: {}", worker_id, path, e);
                            }
                        }
                    },
                }
            }

            debug!("Copy worker {} finished", worker_id);
        });

        copy_workers.push(handle);
    }

    // Wait for all stages to complete
    window_generator.await?;
    for handle in window_processors {
        handle.await?;
    }
    for handle in copy_workers {
        handle.await?;
    }

    // Print final statistics
    let total_copied = progress.copied_files.load(Ordering::Relaxed);
    let total_skipped = progress.skipped_files.load(Ordering::Relaxed);
    let total_failed = progress.failed_files.load(Ordering::Relaxed);

    info!(
        "Mirror complete: {} files copied, {} skipped, {} failed",
        total_copied, total_skipped, total_failed
    );

    // Write the appropriate HAS file to destination (even if there were failures)
    // This ensures scan operations can still work with partially mirrored archives
    // 
    // IMPORTANT: We always use the history file at the highest checkpoint we actually mirrored,
    // never re-read the source's current HAS. This avoids race conditions where the source
    // archive advances during our mirror operation.
    let highest_mirrored_checkpoint = checkpoints.last().copied()
        .unwrap_or(0);  // We already checked checkpoints.is_empty() earlier
    
    info!(
        "Writing destination HAS from history file at checkpoint {:08x}",
        highest_mirrored_checkpoint
    );
    update_destination_has(&src_op, &dst_op, highest_mirrored_checkpoint).await?;

    // Check for failures after writing HAS
    if total_failed > 0 {
        anyhow::bail!("Mirror failed with {} errors", total_failed);
    }

    Ok(())
}

// Create windows of checkpoints
fn create_checkpoint_windows(checkpoints: &[u32], window_size: usize) -> Vec<Vec<u32>> {
    let mut windows = Vec::new();
    let mut current_window = Vec::new();

    for &checkpoint in checkpoints {
        current_window.push(checkpoint);
        if current_window.len() >= window_size {
            windows.push(std::mem::take(&mut current_window));
        }
    }

    if !current_window.is_empty() {
        windows.push(current_window);
    }

    windows
}

// Process a window of checkpoints
async fn process_window(
    window_checkpoints: Vec<u32>,
    src_op: &Operator,
    task_tx: &mpsc::Sender<MirrorTask>,
    global_cache: &Arc<Cache<String, ()>>,
    skip_optional: bool,
    progress: &Arc<ProgressTracker>,
) -> Result<()> {
    // Local set for this window only
    let window_buckets = DashSet::new();

    // Process checkpoints in this window with bounded concurrency
    let _results: Vec<_> = stream::iter(window_checkpoints.iter().copied())
        .map(|checkpoint| {
            let src_op = src_op.clone();
            let task_tx = task_tx.clone();
            let window_buckets = window_buckets.clone();
            let global_cache = global_cache.clone();
            let progress = progress.clone();

            async move {
                // Fetch history file for this checkpoint
                let has_path = history_archive::checkpoint_path("history", checkpoint);

                // Try to get buckets from this checkpoint's HAS
                if let Ok(checkpoint_has) =
                    crate::archive::fetch_history_archive_state_path(&src_op, &has_path).await
                {
                    // Process buckets
                    for bucket_hash in checkpoint_has.buckets() {
                        // Check both window-local and global cache
                        if !window_buckets.contains(&bucket_hash)
                            && global_cache.get(&bucket_hash).await.is_none()
                        {
                            window_buckets.insert(bucket_hash.clone());
                            global_cache.insert(bucket_hash.clone(), ()).await;

                            // Send bucket copy task
                            if let Ok(path) = history_archive::bucket_path(&bucket_hash) {
                                task_tx
                                    .send(MirrorTask::Bucket {
                                        hash: bucket_hash,
                                        path,
                                    })
                                    .await
                                    .ok();
                            }
                        }
                    }
                }

                // Send checkpoint file tasks
                for category in &["history", "ledger", "transactions", "results", "scp"] {
                    if *category == "scp" && skip_optional {
                        continue;
                    }

                    let path = history_archive::checkpoint_path(category, checkpoint);
                    task_tx
                        .send(MirrorTask::CheckpointFile {
                            checkpoint,
                            category: category.to_string(),
                            path,
                        })
                        .await
                        .ok();
                }

                progress
                    .processed_checkpoints
                    .fetch_add(1, Ordering::Relaxed);
                progress.report_checkpoint_progress();
            }
        })
        .buffer_unordered(16) // Limit concurrent fetches within window
        .collect()
        .await;

    // Window complete - local set is dropped, memory freed
    Ok(())
}

async fn copy_file(
    src_op: &Operator,
    dst_op: &Operator,
    path: &str,
    force: bool,
) -> Result<CopyResult> {
    // Check if destination exists
    if !force {
        match dst_op.exists(path).await {
            Ok(true) => return Ok(CopyResult::Skipped),
            Ok(false) => {}
            Err(e) => {
                debug!("Error checking destination {}: {}", path, e);
            }
        }
    }

    // Use stream/sink approach for all files - works without Content-Length headers
    copy_file_streaming(src_op, dst_op, path).await?;
    Ok(CopyResult::Copied)
}

async fn copy_file_streaming(
    src_op: &Operator,
    dst_op: &Operator,
    path: &str,
) -> Result<()> {
    use futures::{SinkExt, TryStreamExt};
    use bytes::Bytes;
    
    // Create a bytes stream from the source - uses EOF-delimited streaming
    // This works with CloudFlare's chunked responses without Content-Length
    let stream = src_op
        .reader(path)
        .await
        .with_context(|| format!("Failed to create reader for {}", path))?
        .into_bytes_stream(..)  // Unbounded range: read until EOF
        .await
        .with_context(|| format!("Failed to create stream for {}", path))?;

    // Create a bytes sink for the destination
    let sink = dst_op
        .writer(path)
        .await
        .with_context(|| format!("Failed to create writer for {}", path))?
        .into_bytes_sink();

    // Pipe stream -> sink with backpressure handling
    // This processes data in chunks without holding entire files in memory
    let mut sink = stream
        .map_ok(|buf| Bytes::from(buf))  // Convert Buffer to Bytes for the sink
        .try_fold(sink, |mut sink, bytes| async move {
            sink.send(bytes).await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            Ok(sink)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Stream error: {}", e))
        .with_context(|| format!("Failed to stream copy {}", path))?;
    
    // Close the sink to ensure all data is written
    sink.close()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to close sink: {}", e))?;

    Ok(())
}

/// Write the HAS file to destination by copying the history file at the given checkpoint.
/// This ensures the HAS accurately reflects what was actually mirrored, avoiding race
/// conditions where the source archive advances during the mirror operation.
async fn update_destination_has(
    src_op: &Operator,
    dst_op: &Operator,
    checkpoint: u32,
) -> Result<()> {
    // Copy the history file at the specified checkpoint to be our HAS file
    // This ensures the buckets in .well-known match what was actually mirrored
    let history_path = history_archive::checkpoint_path("history", checkpoint);

    // Read the history file at this checkpoint from source
    let history_content = src_op.read(&history_path).await?;

    // Write it as the .well-known/stellar-history.json
    dst_op
        .write(history_archive::ROOT_HAS_PATH, history_content)
        .await?;

    Ok(())
}
