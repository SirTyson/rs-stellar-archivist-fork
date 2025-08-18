//! Tests for scan command with --low and --high range parameters

use std::path::PathBuf;
use stellar_archivist::test_helpers::{run_scan, ScanConfig};

/// Get the test archive path
fn get_test_archive_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small")
}

#[tokio::test]
async fn test_scan_with_range() {
    let test_archive_path = get_test_archive_path();

    // Test scanning a specific range
    let config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 4,
        skip_optional: false,
        low: Some(1000),
        high: Some(5000),
    };

    // Should succeed - range is within archive bounds
    let result = run_scan(config).await;
    assert!(result.is_ok(), "Scan with valid range should succeed");
}

#[tokio::test]
async fn test_scan_low_beyond_current() {
    let test_archive_path = get_test_archive_path();

    // Test with low beyond the archive's current ledger
    let config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(20000),
        high: None,
    };

    // Should fail with appropriate error
    let result = run_scan(config).await;
    assert!(result.is_err(), "Scan with low beyond current should fail");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("No checkpoints above the lower bound"),
        "Error should mention no checkpoints above lower bound"
    );
}

#[tokio::test]
async fn test_scan_high_beyond_current() {
    let test_archive_path = get_test_archive_path();

    // Test with high beyond the archive's current ledger
    // This should warn but continue (not fail)
    let config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(20000), // Beyond archive's current ledger
    };

    // Should succeed (with warning logged)
    let result = run_scan(config).await;
    assert!(
        result.is_ok(),
        "Scan with high beyond current should succeed with warning"
    );
}

#[tokio::test]
async fn test_scan_empty_range() {
    let test_archive_path = get_test_archive_path();

    // Test with low > high
    let config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(5000),
        high: Some(1000), // high < low
    };

    // Should fail with appropriate error
    let result = run_scan(config).await;
    assert!(result.is_err(), "Scan with low > high should fail");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("greater than high checkpoint"),
        "Error should mention low > high"
    );
}
