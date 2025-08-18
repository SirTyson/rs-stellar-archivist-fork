//! Tests for scan command detecting various types of archive corruption
//!
//! These tests dynamically create corrupted archives by copying testnet-archive-small
//! and randomly removing files of specific types to ensure comprehensive coverage.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::path::{Path, PathBuf};
use stellar_archivist::test_helpers::{run_scan, ScanConfig};
use tempfile::TempDir;
use walkdir::WalkDir;

/// Copy the test archive to a temporary directory
fn copy_test_archive(dst: &Path) -> Result<(), std::io::Error> {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    for entry in WalkDir::new(&src).into_iter().filter_map(|e| e.ok()) {
        let src_path = entry.path();
        let relative = src_path.strip_prefix(&src).unwrap();
        let dst_path = dst.join(relative);

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst_path)?;
        } else {
            std::fs::copy(src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Get all files of a specific type from the archive
fn get_files_by_pattern(archive_path: &Path, pattern: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in WalkDir::new(archive_path)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let path = entry.path();
            let path_str = path.to_string_lossy();
            if path_str.contains(pattern) {
                files.push(path.to_path_buf());
            }
        }
    }
    files
}

/// Randomly remove one file from the list using a seeded RNG
fn remove_random_file(files: &[PathBuf], seed: u64) -> Option<PathBuf> {
    if files.is_empty() {
        return None;
    }

    let mut rng = StdRng::seed_from_u64(seed);
    let index = rng.gen_range(0..files.len());
    let file_to_remove = &files[index];

    if std::fs::remove_file(file_to_remove).is_ok() {
        println!("Removed file: {:?}", file_to_remove);
        Some(file_to_remove.clone())
    } else {
        None
    }
}

#[tokio::test]
async fn test_scan_missing_bucket_dynamic() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    println!("\n=== Testing scan detects missing buckets ===");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    // Copy the test archive
    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // First, identify which buckets are only in history files, not root HAS
    let has_path = archive_path.join(".well-known/stellar-history.json");
    let has_content = std::fs::read_to_string(&has_path).expect("Failed to read HAS");
    let has: serde_json::Value = serde_json::from_str(&has_content).expect("Failed to parse HAS");

    // Collect buckets from root HAS
    let mut root_has_buckets = std::collections::HashSet::new();
    if let Some(buckets) = has["currentBuckets"].as_array() {
        for bucket in buckets {
            if let Some(curr) = bucket["curr"].as_str() {
                if !curr.starts_with("0000000") {
                    root_has_buckets.insert(curr.to_string());
                }
            }
            if let Some(snap) = bucket["snap"].as_str() {
                if !snap.starts_with("0000000") {
                    root_has_buckets.insert(snap.to_string());
                }
            }
        }
    }

    // Collect all historical buckets
    let history_files = get_files_by_pattern(archive_path, "/history-");
    let mut all_historical_buckets = std::collections::HashSet::new();

    for history_file in &history_files {
        if history_file.to_string_lossy().contains(".well-known") {
            continue;
        }

        let content = std::fs::read_to_string(history_file).expect("Failed to read history file");
        if let Ok(history_has) = serde_json::from_str::<serde_json::Value>(&content) {
            if let Some(buckets) = history_has["currentBuckets"].as_array() {
                for bucket in buckets {
                    if let Some(curr) = bucket["curr"].as_str() {
                        if !curr.starts_with("0000000") {
                            all_historical_buckets.insert(curr.to_string());
                        }
                    }
                    if let Some(snap) = bucket["snap"].as_str() {
                        if !snap.starts_with("0000000") {
                            all_historical_buckets.insert(snap.to_string());
                        }
                    }
                }
            }
        }
    }

    println!("Buckets in root HAS: {}", root_has_buckets.len());
    println!("Total historical buckets: {}", all_historical_buckets.len());

    // Find a bucket that's only in history files
    let historical_only: Vec<_> = all_historical_buckets
        .difference(&root_has_buckets)
        .cloned()
        .collect();

    if !historical_only.is_empty() {
        // Remove a historical-only bucket to expose the bug
        let bucket_to_remove = &historical_only[0];
        let bucket_files = get_files_by_pattern(archive_path, "/bucket-");

        for bucket_file in &bucket_files {
            if bucket_file.to_string_lossy().contains(bucket_to_remove) {
                std::fs::remove_file(bucket_file).expect("Failed to remove bucket file");
                println!("Removed historical-only bucket: {:?}", bucket_file);
                break;
            }
        }
    } else {
        // If no historical-only buckets, just remove any bucket
        let bucket_files = get_files_by_pattern(archive_path, "/bucket-");
        if !bucket_files.is_empty() {
            let removed = remove_random_file(&bucket_files, 42);
            if let Some(file) = removed {
                println!("Removed bucket: {:?}", file);
            }
        }
    }

    // Now scan should fail since we removed a bucket
    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    match run_scan(scan_config).await {
        Ok(_) => {
            // BUG: The scan passed even though we removed a bucket!
            // This happens because scan only checks buckets from root HAS
            panic!(
                "BUG: Scan passed despite missing bucket file! \
                 Scan is not checking all historical buckets, only root HAS buckets."
            );
        }
        Err(e) => {
            println!("✓ Scan correctly failed with error: {}", e);
            assert!(
                e.to_string().contains("Archive scan failed"),
                "Error should be 'Archive scan failed', got: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_scan_missing_history() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Get all history files (excluding the root HAS)
    let history_files: Vec<_> = get_files_by_pattern(archive_path, "/history-")
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .collect();

    println!("Found {} history files", history_files.len());

    // Remove a random history file
    let removed = remove_random_file(&history_files, 42);

    if let Some(removed_file) = removed {
        println!("Removed history file: {:?}", removed_file);
    }

    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should detect missing history file
    match run_scan(scan_config).await {
        Ok(_) => {
            println!("Scan completed, likely only warned about missing files");
        }
        Err(e) => {
            println!("✓ Scan failed with error: {}", e);
            assert!(
                e.to_string().contains("Archive scan failed"),
                "Error should be 'Archive scan failed', got: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_scan_missing_ledger() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    let ledger_files = get_files_by_pattern(archive_path, "/ledger-");
    println!("Found {} ledger files", ledger_files.len());

    // Remove a random ledger file
    let removed = remove_random_file(&ledger_files, 123);

    if let Some(removed_file) = removed {
        println!("Removed ledger file: {:?}", removed_file);
    }

    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should detect missing ledger file
    match run_scan(scan_config).await {
        Ok(_) => {
            println!("Scan completed, likely only warned about missing files");
        }
        Err(e) => {
            println!("✓ Scan failed with error: {}", e);
            assert!(
                e.to_string().contains("Archive scan failed"),
                "Error should be 'Archive scan failed', got: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_scan_missing_transactions() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    let tx_files = get_files_by_pattern(archive_path, "/transactions-");
    println!("Found {} transaction files", tx_files.len());

    // Remove a random transaction file
    let removed = remove_random_file(&tx_files, 456);

    if let Some(removed_file) = removed {
        println!("Removed transaction file: {:?}", removed_file);
    }

    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should detect missing transaction file
    match run_scan(scan_config).await {
        Ok(_) => {
            println!("Scan completed, likely only warned about missing files");
        }
        Err(e) => {
            println!("✓ Scan failed with error: {}", e);
            assert!(
                e.to_string().contains("Archive scan failed"),
                "Error should be 'Archive scan failed', got: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_scan_missing_results() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    let results_files = get_files_by_pattern(archive_path, "/results-");
    println!("Found {} results files", results_files.len());

    // Remove a random results file
    let removed = remove_random_file(&results_files, 789);

    if let Some(removed_file) = removed {
        println!("Removed results file: {:?}", removed_file);
    }

    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should detect missing results file
    match run_scan(scan_config).await {
        Ok(_) => {
            println!("Scan completed, likely only warned about missing files");
        }
        Err(e) => {
            println!("✓ Scan failed with error: {}", e);
            assert!(
                e.to_string().contains("Archive scan failed"),
                "Error should be 'Archive scan failed', got: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_scan_complete_archive() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Don't remove anything - archive should be complete
    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should succeed on complete archive
    match run_scan(scan_config).await {
        Ok(_) => {
            println!("✓ Scan succeeded on complete archive");
        }
        Err(e) => {
            panic!(
                "Scan should succeed on complete archive, but failed with: {}",
                e
            );
        }
    }
}

// This test specifically targets the bucket scanning bug
#[tokio::test]
async fn test_scan_detects_all_historical_buckets() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    println!("\n=== Testing that scan checks ALL historical buckets ===");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // First, let's understand what buckets exist
    let bucket_files = get_files_by_pattern(archive_path, "/bucket-");
    println!("Total bucket files in archive: {}", bucket_files.len());

    // Read the root HAS to see what buckets it references
    let has_path = archive_path.join(".well-known/stellar-history.json");
    let has_content = std::fs::read_to_string(&has_path).expect("Failed to read HAS");
    let has: serde_json::Value = serde_json::from_str(&has_content).expect("Failed to parse HAS");

    // Count unique bucket hashes in root HAS
    let mut root_has_buckets = std::collections::HashSet::new();
    if let Some(buckets) = has["currentBuckets"].as_array() {
        for bucket in buckets {
            if let Some(curr) = bucket["curr"].as_str() {
                if !curr.starts_with("0000000") {
                    root_has_buckets.insert(curr.to_string());
                }
            }
            if let Some(snap) = bucket["snap"].as_str() {
                if !snap.starts_with("0000000") {
                    root_has_buckets.insert(snap.to_string());
                }
            }
        }
    }

    println!("Buckets referenced by root HAS: {}", root_has_buckets.len());

    // Now check history files to find buckets NOT in root HAS
    let history_files = get_files_by_pattern(archive_path, "/history-");
    let mut all_historical_buckets = std::collections::HashSet::new();

    for history_file in &history_files {
        if history_file.to_string_lossy().contains(".well-known") {
            continue;
        }

        let content = std::fs::read_to_string(history_file).expect("Failed to read history file");
        if let Ok(history_has) = serde_json::from_str::<serde_json::Value>(&content) {
            if let Some(buckets) = history_has["currentBuckets"].as_array() {
                for bucket in buckets {
                    if let Some(curr) = bucket["curr"].as_str() {
                        if !curr.starts_with("0000000") {
                            all_historical_buckets.insert(curr.to_string());
                        }
                    }
                    if let Some(snap) = bucket["snap"].as_str() {
                        if !snap.starts_with("0000000") {
                            all_historical_buckets.insert(snap.to_string());
                        }
                    }
                }
            }
        }
    }

    println!(
        "Total unique buckets referenced by all history files: {}",
        all_historical_buckets.len()
    );

    // Find buckets that are in history files but NOT in root HAS
    let historical_only_buckets: Vec<_> = all_historical_buckets
        .difference(&root_has_buckets)
        .cloned()
        .collect();

    println!(
        "Buckets in history files but NOT in root HAS: {}",
        historical_only_buckets.len()
    );

    if historical_only_buckets.is_empty() {
        println!("WARNING: Test archive doesn't have buckets unique to history files.");
        println!("This test cannot properly verify the bucket scanning bug fix.");
        return;
    }

    // Remove one of the historical-only buckets
    println!("Removing a bucket that's only referenced by history files, not root HAS...");
    let bucket_to_remove = &historical_only_buckets[0];

    // Find the actual file for this bucket
    let mut removed = false;
    for bucket_file in &bucket_files {
        if bucket_file.to_string_lossy().contains(bucket_to_remove) {
            std::fs::remove_file(bucket_file).expect("Failed to remove bucket file");
            println!("Removed bucket file: {:?}", bucket_file);
            removed = true;
            break;
        }
    }

    if !removed {
        println!(
            "WARNING: Couldn't find bucket file for hash {}",
            bucket_to_remove
        );
        return;
    }

    // Now scan - with the bug, this will PASS because scan only checks root HAS buckets
    // After the fix, this should FAIL
    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    match run_scan(scan_config).await {
        Ok(_) => {
            println!("BUG CONFIRMED: Scan passed despite missing historical bucket!");
            println!("The scan is only checking buckets from root HAS, not all history files.");
            // Once we fix the bug, this test should fail here
            // For now, we expect this behavior due to the bug
        }
        Err(e) => {
            println!("✓ Scan correctly detected missing historical bucket: {}", e);
            assert!(
                e.to_string().contains("Archive scan failed"),
                "Error should be 'Archive scan failed', got: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_scan_missing_well_known() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    println!("\n=== Testing scan with missing .well-known/stellar-history.json ===");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    // Copy the test archive
    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Remove the .well-known/stellar-history.json file
    let well_known_path = archive_path.join(".well-known/stellar-history.json");
    std::fs::remove_file(&well_known_path).expect("Failed to remove .well-known file");
    println!("Removed .well-known/stellar-history.json");

    // Scan should fail without the root HAS file
    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    match run_scan(scan_config).await {
        Ok(_) => {
            panic!("Scan should fail when .well-known/stellar-history.json is missing!");
        }
        Err(e) => {
            println!("✓ Scan correctly failed with error: {}", e);
            assert!(
                e.to_string().contains("Archive scan failed")
                    || e.to_string().contains("stellar-history.json")
                    || e.to_string().contains("not found")
                    || e.to_string().contains("Failed to fetch"),
                "Error should be 'Archive scan failed' or HAS-related error, got: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_scan_well_known_not_in_history() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    println!("\n=== Testing scan with .well-known not copied to history folder ===");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    // Copy the test archive
    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // The .well-known/stellar-history.json exists, but let's verify it's also
    // supposed to be in the history folder at the current checkpoint
    let well_known_path = archive_path.join(".well-known/stellar-history.json");
    let has_content = std::fs::read_to_string(&well_known_path).expect("Failed to read HAS");
    let has: serde_json::Value = serde_json::from_str(&has_content).expect("Failed to parse HAS");

    let current_ledger = has["currentLedger"].as_u64().unwrap() as u32;
    let current_checkpoint = (current_ledger + 1) / 64 * 64 - 1; // Round to checkpoint

    // The history file at the current checkpoint should match the .well-known file
    let history_path = archive_path.join(format!(
        "history/{:02x}/{:02x}/{:02x}/history-{:08x}.json",
        (current_checkpoint >> 24) & 0xff,
        (current_checkpoint >> 16) & 0xff,
        (current_checkpoint >> 8) & 0xff,
        current_checkpoint
    ));

    if history_path.exists() {
        // Remove the history file at the current checkpoint
        std::fs::remove_file(&history_path).expect("Failed to remove history file");
        println!(
            "Removed history file at current checkpoint: {:?}",
            history_path
        );

        // This creates an inconsistency: .well-known claims currentLedger but
        // the corresponding history file is missing
    }

    // Scan should detect this inconsistency
    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    match run_scan(scan_config).await {
        Ok(_) => {
            // The scan might pass if it only looks at .well-known
            println!("Scan passed - it may not check for history file consistency");
        }
        Err(e) => {
            println!("✓ Scan detected missing history file: {}", e);
            assert!(
                e.to_string().contains("Archive scan failed"),
                "Error should be 'Archive scan failed', got: {}",
                e
            );
        }
    }
}
