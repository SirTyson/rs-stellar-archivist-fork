# Stellar Archivist

High-performance Stellar History Archive tools and utilities with optimized HTTP/FS-only I/O adapter.

## Features

- **Optimized I/O Modes**:
  - **Streaming**: For small files (<2MB), unknown sizes, or servers without range support
  - **Ranged Parallel**: For large files (≥2MB) with byte-range support - downloads in parallel chunks
  - **Filesystem**: Direct async file I/O

- **HTTPS Auto-Upgrade**: Automatically upgrades HTTP URLs to HTTPS when available for better performance (HTTP/2)

- **Performance Optimizations**:
  - 512KB I/O buffers throughout (no small `tokio::io::copy`)
  - Parallel ranged downloads for large files
  - Pre-sized files with no per-chunk truncation
  - Per-host concurrency limiting (default: 64 connections)
  - HTTP/2 connection pooling and keep-alive
  - Skip HEAD requests for JSON files (always small)

## Installation

```bash
cargo build --release
```

## Usage

### Mirror Archive

Mirror files from a source archive to local filesystem:

```bash
# Mirror entire archive
stellar-archivist mirror http://history.stellar.org/prd/core-testnet/core_testnet_001 file:///local/mirror

# Mirror up to specific checkpoint
stellar-archivist mirror http://history.stellar.org/prd/core-testnet/core_testnet_001 file:///local/mirror --high 1000

# Skip optional files (scp, history, results)
stellar-archivist mirror http://history.stellar.org/prd/core-testnet/core_testnet_001 file:///local/mirror --skip-optional

# Force overwrite existing files
stellar-archivist mirror http://history.stellar.org/prd/core-testnet/core_testnet_001 file:///local/mirror --force
```

### Scan Archive

Verify integrity of an archive:

```bash
# Scan remote archive
stellar-archivist scan http://history.stellar.org/prd/core-testnet/core_testnet_001

# Scan local archive
stellar-archivist scan file:///local/mirror
```

## Configuration Options

### Global Options

- `--concurrency N`: Number of concurrent workers (default: 32)
- `--skip-optional`: Skip optional files (scp, history, results)
- `--debug`: Enable debug logging
- `--trace`: Enable trace logging

### Performance Tuning

- `-c, --concurrency N`: Number of concurrent file operations (default: 32)

Example:
```bash
# Increase concurrency for fast networks
stellar-archivist -c 64 mirror ...

# Reduce for slower connections
stellar-archivist -c 8 mirror ...
```

## How It Works

### Architecture

The tool uses a three-stage pipeline architecture:

1. **Checkpoint Discovery**: Fetches the archive's state and determines which checkpoints to process
2. **Sequential Processing**: Processes checkpoints one at a time with bounded file concurrency
3. **Result Collection**: Aggregates results and handles errors

### Concurrency Control

- Checkpoints are processed sequentially to prevent runaway concurrency
- Within each checkpoint, files are downloaded concurrently
- A semaphore limits concurrent file operations to exactly the `-c` value
- This ensures predictable resource usage regardless of checkpoint size

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   main.rs   │────▶│ cmd_mirror/  │────▶│  pipeline   │
│    (CLI)    │     │  cmd_scan    │     │  (worker    │
└─────────────┘     └──────────────┘     │   pool)     │
                                         └─────────────┘
                                                 │
                                                 ▼
                    ┌────────────────────────────────────┐
                    │         fetcher.rs                 │
                    │   (storage abstraction layer)      │
                    └────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
            ┌──────────────┐              ┌──────────────┐
            │ io_backend.rs│              │ io_backend.rs│
            │   HttpStore  │              │    FsStore   │
            │ (HTTP/HTTPS) │              │ (Filesystem) │
            └──────────────┘              └──────────────┘
```

## Development

### Building

```bash
cargo build --release
```

### Testing

```bash
cargo test --release
```

### Code Style

```bash
cargo fmt
cargo clippy -- -D warnings
```

## License

Apache-2.0