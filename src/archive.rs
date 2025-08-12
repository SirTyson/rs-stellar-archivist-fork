use anyhow::{Context, Result};
use opendal::{Operator, Scheme};
use std::collections::HashMap;
use url::Url;

pub fn new_operator(url_str: &str) -> Result<Operator> {
    // Parse the URL using the url crate for proper handling
    let url = Url::parse(url_str).with_context(|| format!("Failed to parse URL: {}", url_str))?;

    match url.scheme() {
        "file" => {
            // For file URLs, use the path component
            let path = url.path();
            let mut config = HashMap::new();
            config.insert("root".to_string(), path.to_string());

            Operator::via_iter(Scheme::Fs, config).context("Failed to create filesystem operator")
        }
        "http" | "https" => {
            let mut config = HashMap::new();

            // Build endpoint from scheme, host, and port
            let endpoint = format!(
                "{}://{}{}",
                url.scheme(),
                url.host_str().unwrap_or(""),
                url.port().map(|p| format!(":{}", p)).unwrap_or_default()
            );

            // Use the path component as root, ensuring it ends with /
            let root = {
                let path = url.path();
                if path.is_empty() || path == "/" {
                    "/".to_string()
                } else if path.ends_with('/') {
                    path.to_string()
                } else {
                    format!("{}/", path)
                }
            };

            config.insert("endpoint".to_string(), endpoint);
            config.insert("root".to_string(), root);

            Operator::via_iter(Scheme::Http, config).context("Failed to create HTTP operator")
        }
        scheme => {
            anyhow::bail!("Unsupported URL scheme: {}", scheme)
        }
    }
}

pub fn is_optional_file(path: &str) -> bool {
    path.contains("/scp/") && path.ends_with(".xdr.gz")
}

#[allow(dead_code)]
pub fn extract_hash_from_path(path: &str) -> Option<String> {
    if path.contains("/bucket/") || path.contains("bucket/") {
        path.split('/').last().and_then(|filename| {
            // Remove "bucket-" prefix and ".xdr.gz" suffix
            filename
                .strip_prefix("bucket-")
                .and_then(|s| s.strip_suffix(".xdr.gz"))
                .map(|s| s.to_string())
        })
    } else {
        None
    }
}

pub async fn fetch_history_archive_state(
    op: &Operator,
) -> Result<crate::history_archive::HistoryArchiveState> {
    use crate::history_archive::ROOT_HAS_PATH;
    fetch_history_archive_state_path(op, ROOT_HAS_PATH).await
}

pub async fn fetch_history_archive_state_path(
    op: &Operator,
    path: &str,
) -> Result<crate::history_archive::HistoryArchiveState> {
    use crate::history_archive::HistoryArchiveState;
    use tracing::debug;

    debug!("Fetching HAS from path: {}", path);
    debug!("Operator info: {:?}", op.info());
    let data = op
        .read(path)
        .await
        .with_context(|| format!("Failed to read {}", path))?;

    let has: HistoryArchiveState = serde_json::from_slice(&data.to_bytes())
        .with_context(|| format!("Failed to parse {}", path))?;

    // Validate the HAS format
    has.validate()
        .map_err(|e| anyhow::anyhow!("Invalid HAS format in {}: {}", path, e))?;

    Ok(has)
}
