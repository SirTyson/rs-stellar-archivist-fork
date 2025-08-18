//! Tests for mirror resume functionality
//!
//! Verifies that mirror can resume from an existing destination's checkpoint
//! by reading the destination's .well-known/stellar-history.json file.

use std::path::PathBuf;
use stellar_archivist::test_helpers::{run_mirror, run_scan, MirrorConfig, ScanConfig};
use tempfile::TempDir;

/// Get the test archive path
fn get_test_archive_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small")
}

#[tokio::test]
async fn test_mirror_resumes_from_destination_checkpoint() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");

    // Step 1: Mirror first 1000 ledgers
    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(1000), // Stop at ledger 1000
        overwrite: false,
        allow_mirror_gaps: false,
        max_bucket_cache: None,
    };

    run_mirror(config)
        .await
        .expect("First mirror should succeed");

    // Verify the first mirror created the HAS file
    let has_path = dest_path.join(".well-known").join("stellar-history.json");
    assert!(
        has_path.exists(),
        "HAS file should exist after first mirror"
    );

    // Read the current ledger from HAS
    let has_content = std::fs::read_to_string(&has_path).expect("Should read HAS");
    let has: serde_json::Value = serde_json::from_str(&has_content).expect("Should parse HAS");
    let first_ledger = has["currentLedger"]
        .as_u64()
        .expect("Should have currentLedger");

    // Should be checkpoint 0x3ff (1023) for ledger 1000
    assert!(
        first_ledger <= 1023,
        "First mirror should stop around ledger 1000"
    );

    // Step 2: Resume mirroring WITHOUT specifying --low
    // It should automatically resume from where it left off
    let resume_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,        // NOT specified - should auto-resume
        high: Some(2000), // Mirror up to 2000
        overwrite: false,
        allow_mirror_gaps: false,
        max_bucket_cache: None,
    };

    run_mirror(resume_config)
        .await
        .expect("Resume mirror should succeed");

    // Step 3: Verify that files from both ranges exist
    // Files from checkpoint 63 (first checkpoint) should exist from first mirror
    let early_history = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("00")
        .join("history-0000003f.json");
    assert!(early_history.exists(), "Early history file should exist");

    // Files from checkpoint around 1983 should exist from resume
    let later_history = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("07")
        .join("history-000007bf.json");
    assert!(
        later_history.exists(),
        "Later history file should exist from resume"
    );

    // Step 4: Read the updated HAS and verify it advanced
    let has_content = std::fs::read_to_string(&has_path).expect("Should read updated HAS");
    let has: serde_json::Value = serde_json::from_str(&has_content).expect("Should parse HAS");
    let final_ledger = has["currentLedger"]
        .as_u64()
        .expect("Should have currentLedger");

    // Should have advanced beyond the first ledger
    assert!(
        final_ledger > first_ledger,
        "Resume should have advanced the ledger"
    );
    assert!(final_ledger <= 2047, "Should stop around ledger 2000");

    // Step 5: Scan to verify integrity
    let scan_config = ScanConfig {
        archive: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(2000),
    };

    run_scan(scan_config)
        .await
        .expect("Scan should pass on resumed mirror");
}

#[tokio::test]
async fn test_mirror_overwrite_ignores_destination_checkpoint() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror");

    // Step 1: Mirror first 1000 ledgers
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

    // Step 2: Re-mirror with --overwrite flag
    // Should overwrite files in range 0-500, but preserve HAS at 1000
    let overwrite_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(500), // Less than before
        overwrite: true, // Overwrite flag set
        allow_mirror_gaps: false,
        max_bucket_cache: None,
    };

    // This should overwrite files in the specified range (0-500)
    // But should NOT downgrade the HAS file
    run_mirror(overwrite_config)
        .await
        .expect("Overwrite mirror should succeed");

    // The HAS file should still be at 1000 (not downgraded)
    let has_content =
        std::fs::read_to_string(dest_path.join(".well-known").join("stellar-history.json"))
            .expect("Should read HAS file");

    // Parse and check the current ledger
    let has: serde_json::Value = serde_json::from_str(&has_content).expect("Should parse HAS JSON");
    let current_ledger = has["currentLedger"]
        .as_u64()
        .expect("Should have currentLedger");

    // Should still be around 959 (actual checkpoint for ledger 1000) - NOT downgraded
    // Note: checkpoint_number(1000) = (1001/64)*64 - 1 = 959
    assert!(
        current_ledger >= 959 && current_ledger <= 1023,
        "HAS should remain at original checkpoint, not downgrade (was {})",
        current_ledger
    );
}

#[tokio::test]
async fn test_mirror_explicit_low_overrides_destination() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror");

    // Step 1: Mirror first 2000 ledgers
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

    // Step 2: Run mirror with explicit --low that's BEFORE destination's checkpoint
    // This should use the explicit --low, not the destination's checkpoint
    let explicit_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(500), // Explicit low, before destination's current
        high: Some(3000),
        overwrite: false,
        allow_mirror_gaps: false,
        max_bucket_cache: None,
    };

    // Should use the explicit --low value
    run_mirror(explicit_config)
        .await
        .expect("Mirror with explicit low should succeed");

    // Files from the explicit range should exist
    let expected_history = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("01")
        .join("history-000001ff.json");
    assert!(
        expected_history.exists(),
        "History from explicit low range should exist"
    );
}
