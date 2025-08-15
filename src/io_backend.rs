//! Simplified HTTP/FS I/O adapter with streaming only (no range downloads).
//!
//! This implementation focuses on streaming for all files to support
//! future on-the-fly decompression capabilities.

use async_trait::async_trait;
use dashmap::DashSet;
use futures_util::TryStreamExt;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio_util::io::StreamReader;

/// Buffer size for I/O operations (1MB)
const BUFFER_SIZE: usize = 1024 * 1024;

pub type BoxReader = Pin<Box<dyn AsyncRead + Send + Sync>>;

/// Global cache of created directories to avoid repeated syscalls
static CREATED_DIRS: once_cell::sync::Lazy<DashSet<PathBuf>> =
    once_cell::sync::Lazy::new(DashSet::new);

/// Ensure parent directory exists with caching
async fn ensure_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if CREATED_DIRS.contains(parent) {
            return Ok(());
        }
        tokio::fs::create_dir_all(parent).await?;
        CREATED_DIRS.insert(parent.to_path_buf());
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct FileInfo {
    pub size: Option<u64>,
}

/// Core storage trait for filesystem and HTTP backends
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Get an async reader for the file
    async fn get_reader(&self, path: &str) -> io::Result<BoxReader>;

    /// Stream file directly to writer - optimal for HTTP backends
    async fn get_to_writer(
        &self,
        path: &str,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> io::Result<u64> {
        // Default implementation uses get_reader for compatibility
        let mut reader = self.get_reader(path).await?;
        tokio::io::copy(&mut reader, writer).await
    }

    /// Get file size if available
    async fn size(&self, path: &str) -> io::Result<Option<u64>>;

    /// Check if file exists
    async fn exists(&self, path: &str) -> io::Result<bool>;

    /// Get file info (size)
    async fn file_info(&self, path: &str) -> io::Result<FileInfo> {
        let size = self.size(path).await?;
        Ok(FileInfo { size })
    }
}

// ===== Filesystem Backend =====

/// Filesystem storage backend
pub struct FsStore {
    pub root: PathBuf,
}

impl FsStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn abs(&self, p: &str) -> PathBuf {
        self.root.join(p.trim_start_matches('/'))
    }
}

#[async_trait]
impl BlobStore for FsStore {
    async fn get_reader(&self, path: &str) -> io::Result<BoxReader> {
        let p = self.abs(path);
        let f = tokio::fs::File::open(&p).await?;
        Ok(Box::pin(f))
    }

    async fn get_to_writer(
        &self,
        path: &str,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> io::Result<u64> {
        let p = self.abs(path);
        let mut f = tokio::fs::File::open(&p).await?;
        tokio::io::copy(&mut f, writer).await
    }

    async fn size(&self, path: &str) -> io::Result<Option<u64>> {
        Ok(Some(tokio::fs::metadata(self.abs(path)).await?.len()))
    }

    async fn exists(&self, path: &str) -> io::Result<bool> {
        Ok(tokio::fs::try_exists(self.abs(path)).await?)
    }
}

// ===== HTTP Backend =====

/// Simple concurrency limiter
pub struct HttpLimiter {
    permits: Arc<tokio::sync::Semaphore>,
}

impl HttpLimiter {
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            permits: Arc::new(tokio::sync::Semaphore::new(max_concurrency)),
        }
    }

    pub async fn acquire(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.permits.clone().acquire_owned().await.unwrap()
    }
}

/// Build an optimized HTTP client
fn build_http_client(use_http2: bool) -> anyhow::Result<reqwest::Client> {
    use reqwest::header::*;

    // Set identity encoding to avoid decompression by reqwest
    let mut h = HeaderMap::new();
    h.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));

    let mut builder = reqwest::Client::builder()
        .pool_max_idle_per_host(1024)
        .pool_idle_timeout(std::time::Duration::from_secs(120))
        .tcp_nodelay(true)
        .default_headers(h);

    if use_http2 {
        // For HTTPS: allow HTTP/2 with adaptive window
        builder = builder.http2_adaptive_window(true);
    } else {
        // For HTTP: force HTTP/1.1 only
        builder = builder.http1_only();
    }

    Ok(builder.build()?)
}

/// HTTP storage backend with streaming support
pub struct HttpStore {
    base: parking_lot::RwLock<reqwest::Url>,
    client: Arc<reqwest::Client>,
    limiter: Arc<HttpLimiter>,
}

impl HttpStore {
    pub fn new(mut base: reqwest::Url) -> Self {
        // Ensure base URL ends with /
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }

        // Use HTTP/2 only for HTTPS URLs
        let use_http2 = base.scheme() == "https";
        let client = Arc::new(build_http_client(use_http2).expect("Failed to build HTTP client"));

        // Log the expected protocol
        if let Some(host) = base.host_str() {
            if use_http2 {
                tracing::info!(
                    "HTTP client created for {} (HTTPS - will use HTTP/2 if server supports it)",
                    host
                );
            } else {
                tracing::info!(
                    "HTTP client created for {} (HTTP - using HTTP/1.1 only)",
                    host
                );
            }
        }

        Self {
            base: parking_lot::RwLock::new(base),
            client,
            limiter: Arc::new(HttpLimiter::new(256)),
        }
    }

    /// Create with custom concurrency limit
    pub fn with_concurrency(base: reqwest::Url, max_concurrency: usize) -> Self {
        let mut store = Self::new(base);
        store.limiter = Arc::new(HttpLimiter::new(max_concurrency));
        store
    }

    fn url(&self, path: &str) -> reqwest::Url {
        let base = self.base.read();
        let mut u = base.clone();
        u.set_path(&format!("{}{}", base.path(), path.trim_start_matches('/')));
        u
    }
}

fn to_ioe<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

#[async_trait]
impl BlobStore for HttpStore {
    async fn get_reader(&self, path: &str) -> io::Result<BoxReader> {
        let _permit = self.limiter.acquire().await;

        let resp = self
            .client
            .get(self.url(path))
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(to_ioe)?;

        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("HTTP {}", resp.status()),
            ));
        }

        let stream = resp.bytes_stream().map_err(to_ioe);
        Ok(Box::pin(StreamReader::new(stream)))
    }

    async fn get_to_writer(
        &self,
        path: &str,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> io::Result<u64> {
        use futures_util::StreamExt;

        let _permit = self.limiter.acquire().await;

        let resp = self
            .client
            .get(self.url(path))
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(to_ioe)?;

        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("HTTP {}", resp.status()),
            ));
        }

        // Fast path for small files
        let content_length = resp.content_length().unwrap_or(0);
        const SMALL_FILE_THRESHOLD: u64 = 64 * 1024; // 64KB

        if content_length > 0 && content_length <= SMALL_FILE_THRESHOLD {
            // Buffer entire small file then single write
            let body = resp.bytes().await.map_err(to_ioe)?;
            writer.write_all(&body).await?;
            writer.flush().await?;
            return Ok(body.len() as u64);
        }

        // Stream larger files with buffering
        let mut bytes_written = 0u64;
        let mut stream = resp.bytes_stream();

        use tokio::io::BufWriter;
        let mut buffered_writer = BufWriter::with_capacity(128 * 1024, writer);

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(to_ioe)?;
            buffered_writer.write_all(&chunk).await?;
            bytes_written += chunk.len() as u64;
        }

        buffered_writer.flush().await?;
        Ok(bytes_written)
    }

    async fn size(&self, path: &str) -> io::Result<Option<u64>> {
        let _permit = self.limiter.acquire().await;

        let resp = self
            .client
            .head(self.url(path))
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(to_ioe)?;

        if let Some(len) = resp.headers().get(reqwest::header::CONTENT_LENGTH) {
            if let Some(s) = len.to_str().ok().and_then(|s| s.parse::<u64>().ok()) {
                return Ok(Some(s));
            }
        }
        Ok(None)
    }

    async fn exists(&self, path: &str) -> io::Result<bool> {
        let _permit = self.limiter.acquire().await;

        let resp = self
            .client
            .head(self.url(path))
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(to_ioe)?;

        Ok(resp.status().is_success())
    }
}

// ===== Backend Enum =====

pub enum Backend {
    Fs(FsStore),
    Http(HttpStore),
}

#[async_trait]
impl BlobStore for Backend {
    async fn get_reader(&self, path: &str) -> io::Result<BoxReader> {
        match self {
            Backend::Fs(s) => s.get_reader(path).await,
            Backend::Http(s) => s.get_reader(path).await,
        }
    }

    async fn get_to_writer(
        &self,
        path: &str,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> io::Result<u64> {
        match self {
            Backend::Fs(s) => s.get_to_writer(path, writer).await,
            Backend::Http(s) => s.get_to_writer(path, writer).await,
        }
    }

    async fn size(&self, path: &str) -> io::Result<Option<u64>> {
        match self {
            Backend::Fs(s) => s.size(path).await,
            Backend::Http(s) => s.size(path).await,
        }
    }

    async fn exists(&self, path: &str) -> io::Result<bool> {
        match self {
            Backend::Fs(s) => s.exists(path).await,
            Backend::Http(s) => s.exists(path).await,
        }
    }

    async fn file_info(&self, path: &str) -> io::Result<FileInfo> {
        let size = self.size(path).await?;
        Ok(FileInfo { size })
    }
}

// ===== Copy Functions =====

/// Stream entire file to destination
pub async fn stream_to_file<S: BlobStore + ?Sized>(
    store: &S,
    src_path: &str,
    dst: &Path,
) -> io::Result<()> {
    ensure_parent(dst).await?;

    let mut reader = store.get_reader(src_path).await?;
    let mut file = tokio::fs::File::create(dst).await?;

    let mut buffer = vec![0u8; BUFFER_SIZE];
    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        file.write_all(&buffer[..n]).await?;
    }

    Ok(())
}
