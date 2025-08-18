# Stellar Archivist Documentation

## Project Overview

Stellar Archivist is a tool for managing Stellar History Archives - the distributed storage system used by the Stellar network to store historical ledger data. This Rust implementation provides efficient mirroring and validation of these archives.

## Architecture

### Module Structure
```rust
// Core modules
pub mod storage;           // Storage trait and implementations
pub mod fetcher;          // File fetching and optimization logic
pub mod history_file;     // History Archive State (HAS) file parsing
pub mod pipeline;         // Parallel processing pipeline

// Command implementations
pub mod mirror_operation;  // Mirror command logic
pub mod scan_operation;    // Scan/validation command logic
```

### Storage System (`src/storage.rs`)

The storage abstraction provides a unified interface for different backends:

```rust
pub trait Storage: Send + Sync {
    async fn open_reader(&self, object: &str) -> io::Result<BoxedAsyncRead>;
    async fn exists(&self, object: &str) -> io::Result<bool>;
    async fn open_writer(&self, object: &str) -> io::Result<BoxedAsyncWrite> {
        // Default: write not supported
    }
}

// Type aliases
pub type BoxedAsyncRead = Box<dyn AsyncRead + Send + Unpin>;
pub type BoxedAsyncWrite = Box<dyn AsyncWrite + Send + Unpin>;
```

Two storage implementations are provided:
- `FilesystemStore`: Direct filesystem I/O for local archives
- `HttpStore`: HTTP client with automatic retry logic and connection pooling

### HTTP Client Features

The HTTP implementation includes:
- **Automatic retry logic**: Retries transient errors with exponential backoff
- **Connection pooling**: Maintains up to 128 idle connections per host
- **HTTP/2 support**: Uses HTTP/2 when available for multiplexing
- **HEAD fallback**: Falls back to GET with Range header when HEAD is unsupported

Retry configuration:
```rust
const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 100;  // Backoff: 100ms → 200ms → 400ms

// Retries on: 408, 429, 500, 502, 503, 504, and network errors
// Does not retry on: 4xx client errors (except 408, 429)
```

### Pipeline Architecture (`src/pipeline.rs`)

The pipeline provides parallel processing for archive operations:

```rust
pub struct Pipeline {
    io_permits: Arc<Semaphore>,               // Limits concurrent I/O
    inflight_files: Arc<AtomicUsize>,         // Tracks active operations
    bucket_lru: Arc<Mutex<LruCache<String, ()>>>, // Prevents duplicate buckets
}
```

Key features:
- Producer/consumer model with multiple workers
- Per-checkpoint atomicity (each checkpoint processed by one worker)
- LRU cache for bucket deduplication across checkpoints
- Channel-based work distribution with backpressure

### Fetcher (`src/fetcher.rs`)

The fetcher optimizes file transfers based on file characteristics:
- Identifies small files to avoid unnecessary HEAD requests
- Uses streaming for most files
- Implements smart copy strategies based on file type

## Stellar History Archive Format

### Directory Structure
```
archive-root/
├── .well-known/
│   └── stellar-history.json       # Archive metadata
├── history/                       # HAS files (JSON, small)
│   └── XX/XX/XX/
│       └── history-XXXXXXXX.json
├── bucket/                        # Content-addressed data (can be large)
│   └── XX/XX/XX/
│       └── bucket-[64-char-hash].xdr.gz
├── ledger/                        # Ledger headers (small-medium)
│   └── XX/XX/XX/
│       └── ledger-XXXXXXXX.xdr.gz
├── transactions/                  # Transaction data (small-medium)
│   └── XX/XX/XX/
│       └── transactions-XXXXXXXX.xdr.gz
├── results/                       # Transaction results (small)
│   └── XX/XX/XX/
│       └── results-XXXXXXXX.xdr.gz
└── scp/                          # SCP messages (optional, small)
    └── XX/XX/XX/
        └── scp-XXXXXXXX.xdr.gz
```

### File Characteristics
- **Small (<100KB)**: history/*.json, results/*.xdr.gz, scp/*.xdr.gz
- **Small-Medium (<1MB)**: ledger/*.xdr.gz, transactions/*.xdr.gz
- **Variable (1KB-100MB+)**: bucket/*.xdr.gz (content-addressed by SHA256)

### Key Concepts
- **Checkpoints**: Occur every 64 ledgers
- **HAS Files**: JSON files describing checkpoint contents
- **Content Addressing**: Bucket filenames are their SHA256 hash
- **Optional Files**: SCP files can be skipped without breaking integrity

## Commands

### Mirror Command
Copies archives from source to destination:

```bash
stellar-archivist mirror <source> <destination> [options]

Options:
  -c, --concurrency <N>     Parallel workers (default: 16)
  --high <checkpoint>       Stop at this checkpoint
  --low <checkpoint>        Start from this checkpoint
  --skip-optional          Skip SCP files
  --force                  Overwrite existing files

Example:
stellar-archivist mirror \
  http://history.stellar.org/prd/core-testnet/core_testnet_001 \
  file:///tmp/testnet-mirror \
  --high 511 -c 32
```

### Scan Command
Validates archive integrity:

```bash
stellar-archivist scan <archive-url>

Example:
stellar-archivist scan file:///tmp/testnet-mirror
```

## Implementation Details

### Streaming Architecture
- Direct streaming from source to destination
- No full-file buffering in memory
- Write buffer size: 128KB (`WRITE_BUF_BYTES` constant)
- Constant memory usage regardless of file size

### HTTP Client Configuration
```rust
reqwest::Client::builder()
    .pool_max_idle_per_host(128)
    .pool_idle_timeout(Duration::from_secs(90))
    .http2_prior_knowledge()
    .default_headers(headers)  // Accept-Encoding: identity
    .build()
```

### URL Building
URLs are constructed using proper joining to handle edge cases:
```rust
self.base_url
    .join(object.trim_start_matches('/'))
    .expect("Failed to join URL")
```

### Error Handling
Errors are properly propagated, not swallowed:
```rust
match self.dst_storage.exists(path).await {
    Ok(true) => { /* handle existing */ }
    Ok(false) => { /* proceed */ }
    Err(e) => {
        if is_optional {
            warn!("Error checking existence: {}", e);
            return Ok(ObjectResult::Skipped);
        } else {
            return Err(e.into());
        }
    }
}
```

### History File Management
HAS files are always read from the destination during mirror operations to avoid race conditions where the source archive is being updated.

## Testing

### Test Organization
```
tests/
├── cli_validation_test.rs    # CLI argument validation
├── history_archive.rs        # Archive format tests
├── http_retry_test.rs        # HTTP retry logic
├── http_server_test.rs       # HTTP server integration
├── mirror_gap_test.rs        # Gap handling
├── mirror_resume_test.rs     # Resume functionality
├── mirror_tests.rs           # Core mirror tests
├── scan_corruption_test.rs   # Corruption detection
└── scan_range_test.rs        # Range-based scanning
```

### Running Tests
```bash
# All tests
cargo test

# Specific test suite
cargo test --test http_retry_test

# With logging
RUST_LOG=info cargo test -- --nocapture

# Sequential execution (debugging)
cargo test -- --test-threads=1
```

## Dependencies

### Core
- `tokio`: Async runtime
- `reqwest`: HTTP client with HTTP/2
- `futures-util`: Stream utilities
- `clap`: CLI parsing
- `serde`/`serde_json`: JSON handling
- `tracing`: Structured logging
- `anyhow`: Error handling
- `lru`: LRU cache
- `url`: URL parsing

### Development
- `wiremock`: HTTP mocking
- `axum`: Test HTTP server
- `tempfile`: Temporary directories
- `rstest`: Test fixtures

## Configuration

### Environment Variables
```bash
# Logging level
RUST_LOG=info  # Options: trace, debug, info, warn, error

# Module-specific logging
RUST_LOG=stellar_archivist=debug,reqwest=warn

# Component debugging
RUST_LOG=stellar_archivist::storage=trace
```

## Common Issues

### "error sending request" during mirror
Transient network errors are automatically retried with exponential backoff.

### Empty JSON files
The implementation uses streaming that doesn't require Content-Length headers.

### Slow HTTP performance
- Increase concurrency: `-c 32` or higher
- Check network latency
- Verify HTTP/2 is being used (check debug logs)

### "Permission denied" on file:// URLs
Use absolute paths: `file:///home/user/archive` not `file://archive`

## Development Guidelines

### Best Practices
- Use streaming instead of buffering entire files
- Propagate errors with context instead of swallowing them
- Define constants for magic numbers
- Use descriptive names for clarity
- Leverage Rust's type system for safety
- Test after modifications

### Adding Features
- New storage backends: Implement the `Storage` trait
- New file types: Update path classification in `fetcher.rs`
- New retry scenarios: Add to retry logic in `storage.rs`

## Important Invariants

- Bucket files are content-addressed (filename = SHA256 hash)
- Checkpoints occur every 64 ledgers
- HAS files describe all files in a checkpoint
- Archives can have gaps (missing checkpoints are allowed)
- The `.well-known/stellar-history.json` file tracks archive state
- All `.xdr.gz` files are gzip-compressed XDR data
- JSON files are never compressed

## Architecture Decisions

- **No S3 Support**: Simplified to filesystem and HTTP only
- **Streaming Only**: Never buffer entire files in memory
- **Write Support**: Only filesystem backend supports writing
- **Retry Logic**: Automatic for transient errors only
- **Connection Pooling**: Reuse HTTP connections for performance
- **LRU Caching**: Prevent redundant bucket downloads