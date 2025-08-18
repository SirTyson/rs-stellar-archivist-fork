//! Tests for mirror gap validation with --allow-mirror-gaps flag
//!
//! Verifies that mirror properly validates and prevents gaps in archives

use std::path::PathBuf;
use stellar_archivist::test_helpers::{run_mirror, MirrorConfig};
use tempfile::TempDir;

/// Get the test archive path
fn get_test_archive_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small")
}

#[tokio::test]
async fn test_mirror_prevents_gap_by_default() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");

    // Step 1: Mirror up to ledger 1000
    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(1000),
        overwrite: false,
        allow_mirror_gaps: false,
        max_bucket_cache: None,
    };

    run_mirror(config)
        .await
        .expect("First mirror should succeed");

    // Step 2: Try to mirror with --low that would create a gap
    // This should FAIL because allow_mirror_gaps is false
    let gap_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000), // This creates a gap!
        high: Some(3000),
        overwrite: false,
        allow_mirror_gaps: false, // Default: gaps not allowed
        max_bucket_cache: None,
    };

    let result = run_mirror(gap_config).await;
    assert!(result.is_err(), "Mirror with gap should fail by default");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Cannot mirror") && err_msg.contains("would create a gap"),
        "Error should mention gap creation: {}",
        err_msg
    );
    assert!(
        err_msg.contains("--allow-mirror-gaps"),
        "Error should suggest --allow-mirror-gaps flag"
    );
}

#[tokio::test]
async fn test_mirror_allows_gap_with_flag() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");

    // Step 1: Mirror up to ledger 1000
    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(1000),
        overwrite: false,
        allow_mirror_gaps: false,
        max_bucket_cache: None,
    };

    run_mirror(config)
        .await
        .expect("First mirror should succeed");

    // Step 2: Mirror with --low that creates a gap, but with --allow-mirror-gaps
    // This should SUCCEED with a warning
    let gap_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000), // This creates a gap
        high: Some(3000),
        overwrite: false,
        allow_mirror_gaps: true, // Allow gaps!
        max_bucket_cache: None,
    };

    // Should succeed despite the gap
    run_mirror(gap_config)
        .await
        .expect("Mirror with gap should succeed when allowed");

    // Verify that files from the new range exist
    let later_history = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("07")
        .join("history-000007bf.json");
    assert!(
        later_history.exists(),
        "History from gap range should exist"
    );

    // But files in the gap should NOT exist
    let gap_history = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("05")
        .join("history-000005ff.json"); // Ledger ~1500 in the gap
    assert!(!gap_history.exists(), "History in the gap should not exist");
}

#[tokio::test]
async fn test_mirror_no_gap_check_when_low_compatible() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");

    // Step 1: Mirror up to ledger 2000
    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(2000),
        overwrite: false,
        allow_mirror_gaps: false,
        max_bucket_cache: None,
    };

    run_mirror(config)
        .await
        .expect("First mirror should succeed");

    // Step 2: Mirror with --low that's BEFORE the destination's end
    // This should succeed without needing --allow-mirror-gaps
    let compatible_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(1500), // Before destination's end - no gap
        high: Some(3000),
        overwrite: false,
        allow_mirror_gaps: false, // Not needed, no gap created
        max_bucket_cache: None,
    };

    // Should succeed - no gap is created
    run_mirror(compatible_config)
        .await
        .expect("Mirror should succeed when low is compatible");
}

#[tokio::test]
async fn test_mirror_overwrite_does_not_bypass_gap_check() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");

    // Step 1: Mirror up to ledger 1000
    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(1000),
        overwrite: false,
        allow_mirror_gaps: false,
        max_bucket_cache: None,
    };

    run_mirror(config)
        .await
        .expect("First mirror should succeed");

    // Step 2: Try to mirror with --overwrite and --low that would create a gap
    // The --overwrite flag should NOT bypass gap checks
    let overwrite_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000), // Would create a gap
        high: Some(3000),
        overwrite: true,          // Overwrite does NOT bypass gap checks
        allow_mirror_gaps: false, // Gap not allowed
        max_bucket_cache: None,
    };

    // Should FAIL because of the gap (--overwrite doesn't bypass gap validation)
    let result = run_mirror(overwrite_config).await;
    assert!(
        result.is_err(),
        "Mirror should fail due to gap even with --overwrite"
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Cannot mirror") && err_msg.contains("would create a gap"),
        "Error should mention gap creation: {}",
        err_msg
    );

    // Step 3: Now try with both --overwrite and --allow-mirror-gaps
    let both_flags_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000), // Would create a gap
        high: Some(3000),
        overwrite: true,         // Overwrite files
        allow_mirror_gaps: true, // Allow the gap
        max_bucket_cache: None,
    };

    // Should succeed with both flags
    run_mirror(both_flags_config)
        .await
        .expect("Mirror should succeed with both --overwrite and --allow-mirror-gaps");
}

#[tokio::test]
async fn test_mirror_no_gap_check_for_empty_destination() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("empty_dest");

    // Mirror with --low to an empty destination
    // Should succeed without gap check since there's no existing archive
    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000), // High starting point
        high: Some(3000),
        overwrite: false,
        allow_mirror_gaps: false, // Not needed for empty destination
        max_bucket_cache: None,
    };

    // Should succeed - no existing archive means no gap
    run_mirror(config)
        .await
        .expect("Mirror to empty destination should not need gap check");

    // Verify the archive starts from the requested low
    let has_path = dest_path.join(".well-known").join("stellar-history.json");
    assert!(has_path.exists(), "HAS file should exist");
}
