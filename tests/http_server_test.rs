//! Test HTTP via local server

use axum::{routing::get_service, Router};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use stellar_archivist::{cmd_mirror, cmd_scan};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower_http::services::ServeDir;

/// Start an HTTP server serving the specified archive path
async fn start_test_http_server(archive_path: &std::path::Path) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().fallback(get_service(ServeDir::new(archive_path.to_path_buf())));
    start_http_server_with_app(app).await
}

async fn start_http_server_with_app(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind to address");

    let addr = listener.local_addr().expect("Failed to get local address");
    let url = format!("http://{}", addr);

    println!("Test HTTP server listening on {}", url);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("HTTP server failed");
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (url, handle)
}

#[tokio::test]
async fn test_scan_http_archive() {
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");
    let (server_url, server_handle) = start_test_http_server(&test_archive_path).await;

    // Scan the archive via HTTP
    let scan_config = cmd_scan::ScanConfig {
        archive: server_url.clone(),
        concurrency: 4,
        skip_optional: false
    };

    match cmd_scan::run(scan_config).await {
        Ok(_) => (),
        Err(e) => {
            server_handle.abort();
            panic!("HTTP scan failed: {}", e);
        }
    }

    server_handle.abort();
    println!("✓ test_scan_http_archive passed");
}

#[tokio::test]
async fn test_mirror_http_to_filesystem() {
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");
    let (server_url, server_handle) = start_test_http_server(&test_archive_path).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dir.path().to_str().unwrap();

    // Mirror from HTTP to filesystem
    let mirror_config = cmd_mirror::MirrorConfig {
        src: server_url.clone(),
        dst: format!("file://{}", mirror_dest),
        concurrency: 4,
        high: None,
        skip_optional: false,
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };

    println!(
        "Mirroring from HTTP {} to filesystem {}",
        server_url, mirror_dest
    );

    match cmd_mirror::run(mirror_config).await {
        Ok(_) => (),
        Err(e) => {
            server_handle.abort();
            panic!("HTTP mirror failed: {}", e);
        }
    }

    // Verify the mirrored archive
    let scan_config = cmd_scan::ScanConfig {
        archive: format!("file://{}", mirror_dest),
        concurrency: 4,
        skip_optional: false,
    };

    println!("Scanning mirrored archive...");

    match cmd_scan::run(scan_config).await {
        Ok(_) => (),
        Err(e) => {
            server_handle.abort();
            panic!("Scan of mirrored archive failed: {}", e);
        }
    }

    server_handle.abort();
    println!("✓ test_mirror_http_to_filesystem passed");
}

#[tokio::test]
async fn test_http_server_with_missing_files() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    println!("Starting test_http_server_with_missing_files");

    // Create a temporary archive with some missing files
    let temp_archive = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_archive.path();

    // Copy archive but skip some required files
    copy_partial_archive(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("testnet-archive-small"),
        archive_path,
    );

    // Start HTTP server with the partial archive
    let (server_url, server_handle) = start_test_http_server(archive_path).await;

    // Try to scan the partial archive via HTTP
    let scan_config = cmd_scan::ScanConfig {
        archive: server_url.clone(),
        concurrency: 4,
        skip_optional: true,
    };

    println!("Scanning partial archive via HTTP from {}", server_url);

    match cmd_scan::run(scan_config).await {
        Ok(_) => {
            server_handle.abort();
            panic!("Scan should have failed due to missing files");
        }
        Err(e) => {
            println!("✓ Scan correctly failed with missing files: {}", e);
        }
    }

    // Clean shutdown
    server_handle.abort();

    println!("✓ test_http_server_with_missing_files passed");
}

// Test that HAS file is written even when some files fail to download
#[tokio::test]
async fn test_mirror_writes_has_despite_failures() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    println!("Starting test_mirror_writes_has_despite_failures");

    // Create a temp archive with some files that will fail to serve
    let temp_archive = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_archive.path();

    // Copy archive but make some files unreadable (to simulate download failures)
    copy_archive_with_unreadable_files(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("testnet-archive-small"),
        archive_path,
    );

    // Start HTTP server with the modified archive
    let (server_url, server_handle) = start_test_http_server(archive_path).await;

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create temp directory for mirror destination
    let temp_dest = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dest.path().to_str().unwrap();

    // Mirror from HTTP to filesystem (should have some failures)
    let mirror_config = cmd_mirror::MirrorConfig {
        src: server_url.clone(),
        dst: format!("file://{}", mirror_dest),
        concurrency: 4,
        high: Some(255), // Small range for faster test
        skip_optional: true,
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };

    println!(
        "Mirroring from HTTP {} to filesystem {} (expecting some failures)",
        server_url, mirror_dest
    );

    // Mirror should fail due to unreadable files
    match cmd_mirror::run(mirror_config).await {
        Ok(_) => {
            server_handle.abort();
            panic!("Mirror should have reported failures");
        }
        Err(e) => {
            println!("Mirror failed as expected: {}", e);
        }
    }

    // CRITICAL CHECK: HAS file should still exist despite failures
    let has_path = std::path::Path::new(mirror_dest).join(".well-known/stellar-history.json");
    assert!(
        has_path.exists(),
        "BUG EXPOSED: .well-known/stellar-history.json was not written when mirror had failures"
    );

    // The HAS file should be readable and valid
    let has_content = std::fs::read_to_string(&has_path)
        .expect("HAS file should be readable");
    let has: serde_json::Value = serde_json::from_str(&has_content)
        .expect("HAS file should be valid JSON");
    
    // Should have the bounded ledger value
    assert_eq!(
        has["currentLedger"], 
        serde_json::json!(255),
        "HAS file should reflect the bounded mirror range"
    );

    server_handle.abort();
    println!("✓ test_mirror_writes_has_despite_failures passed");
}

// Test that JSON history files are not empty after mirroring (file to file)
#[tokio::test]
async fn test_mirror_copies_json_content_file_to_file() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    println!("Starting test_mirror_copies_json_content_file_to_file");

    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    // Create temp directory for mirror destination
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dir.path().to_str().unwrap();

    // Mirror from file to file
    let mirror_config = cmd_mirror::MirrorConfig {
        src: format!("file://{}", test_archive_path.to_str().unwrap()),
        dst: format!("file://{}", mirror_dest),
        concurrency: 4,
        high: Some(127), // Small range for faster test
        skip_optional: true,
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };

    println!(
        "Mirroring from file {} to filesystem {}",
        test_archive_path.display(), mirror_dest
    );

    match cmd_mirror::run(mirror_config).await {
        Ok(_) => println!("Mirror completed successfully"),
        Err(e) => {
            panic!("Mirror failed: {}", e);
        }
    }

    // CRITICAL CHECK: Verify JSON history files are not empty
    let history_path = std::path::Path::new(mirror_dest)
        .join("history/00/00/00/history-0000003f.json");
    
    assert!(
        history_path.exists(),
        "History file should exist: {:?}",
        history_path
    );

    let history_metadata = std::fs::metadata(&history_path)
        .expect("Should be able to get file metadata");
    
    assert!(
        history_metadata.len() > 0,
        "BUG EXPOSED: History JSON file is empty (0 bytes) at {:?}",
        history_path
    );

    // Also verify the content is valid JSON
    let history_content = std::fs::read_to_string(&history_path)
        .expect("Should be able to read history file");
    
    assert!(
        !history_content.is_empty(),
        "BUG EXPOSED: History JSON file has no content"
    );

    let history_json: serde_json::Value = serde_json::from_str(&history_content)
        .expect("History file should contain valid JSON");
    
    // Verify it has expected fields
    assert!(
        history_json.get("currentLedger").is_some(),
        "History file should have currentLedger field"
    );
    assert!(
        history_json.get("currentBuckets").is_some(),
        "History file should have currentBuckets field"
    );

    // Check multiple history files to ensure it's not just one
    let history_path_2 = std::path::Path::new(mirror_dest)
        .join("history/00/00/00/history-0000007f.json");
    
    if history_path_2.exists() {
        let metadata_2 = std::fs::metadata(&history_path_2)
            .expect("Should be able to get file metadata");
        assert!(
            metadata_2.len() > 0,
            "BUG EXPOSED: Second history JSON file is also empty at {:?}",
            history_path_2
        );
    }

    println!("✓ test_mirror_copies_json_content_file_to_file passed");
}

#[tokio::test]
async fn test_mirror_race_condition_with_advancing_has() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    println!("Starting test_mirror_race_condition_with_advancing_has");

    // Start with the real testnet-archive-small as our base
    let source_archive = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");
    
    // Create a temp archive that simulates a live archive that advances during mirror
    let temp_archive = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_archive.path();
    
    // Copy the real testnet-archive-small to our temp location
    // This gives us realistic data with proper history files
    use walkdir::WalkDir;
    for entry in WalkDir::new(&source_archive).into_iter().filter_map(|e| e.ok()) {
        let src_path = entry.path();
        let relative = src_path.strip_prefix(&source_archive).unwrap();
        let dst_path = archive_path.join(relative);
        
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst_path).ok();
        } else if entry.file_type().is_file() {
            if let Some(parent) = dst_path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::copy(src_path, &dst_path).ok();
        }
    }
    
    // Read the history files for checkpoint 127 and 191 to use as our HAS files
    let history_7f_path = archive_path.join("history/00/00/00/history-0000007f.json");
    let history_bf_path = archive_path.join("history/00/00/00/history-000000bf.json");
    
    // Use the history file at checkpoint 127 as our initial HAS
    let initial_has = std::fs::read_to_string(&history_7f_path)
        .expect("Test data is broken: history-0000007f.json must exist in testnet-archive-small");
    
    // For checkpoint 191, we need to create it since testnet-archive-small only goes to 127
    // Use the history from checkpoint 127 as a base and update currentLedger
    let mut has_json: serde_json::Value = serde_json::from_str(&initial_has)
        .expect("Failed to parse history-0000007f.json");
    has_json["currentLedger"] = serde_json::json!(191);
    let advanced_has = serde_json::to_string_pretty(&has_json)
        .expect("Failed to serialize advanced HAS");
    
    // Write the initial HAS (from checkpoint 127) to .well-known
    let has_path = archive_path.join(".well-known/stellar-history.json");
    std::fs::write(&has_path, &initial_has).expect("Failed to write initial HAS");
    
    // Now add files for the "new" checkpoint 191 that will appear after the mirror starts
    // These files should NOT be downloaded by the mirror
    std::fs::write(&history_bf_path, &advanced_has)
        .expect("Failed to write history-000000bf.json");
    
    let ledger_bf_path = archive_path.join("ledger/00/00/00/ledger-000000bf.xdr.gz");
    std::fs::write(&ledger_bf_path, b"ledger data for 191 - should not be mirrored")
        .expect("Failed to write ledger-000000bf.xdr.gz");
    
    let tx_bf_path = archive_path.join("transactions/00/00/00/transactions-000000bf.xdr.gz");
    std::fs::write(&tx_bf_path, b"tx data for 191 - should not be mirrored")
        .expect("Failed to write transactions-000000bf.xdr.gz");
    
    let results_bf_path = archive_path.join("results/00/00/00/results-000000bf.xdr.gz");
    std::fs::write(&results_bf_path, b"results data for 191 - should not be mirrored")
        .expect("Failed to write results-000000bf.xdr.gz");
    
    let scp_bf_path = archive_path.join("scp/00/00/00/scp-000000bf.xdr.gz");
    std::fs::create_dir_all(scp_bf_path.parent().unwrap()).ok();
    std::fs::write(&scp_bf_path, b"scp data for 191 - should not be mirrored")
        .expect("Failed to write scp-000000bf.xdr.gz");
    
    // Set up flag to simulate HAS advancement
    let should_advance = Arc::new(AtomicBool::new(false));
    let should_advance_clone = should_advance.clone();
    
    // Start HTTP server with special handler that advances HAS after initial reads
    let app = Router::new()
        .route("/.well-known/stellar-history.json", 
            axum::routing::get(move || {
                let should_advance = should_advance_clone.clone();
                let has_content = if should_advance.load(Ordering::Relaxed) {
                    // Return advanced HAS
                    advanced_has.to_string()
                } else {
                    // First few reads get initial HAS, then we advance
                    should_advance.store(true, Ordering::Relaxed);
                    initial_has.to_string()
                };
                async move {
                    has_content
                }
            })
        )
        .fallback(get_service(ServeDir::new(archive_path.to_path_buf())));
    
    let (server_url, server_handle) = start_http_server_with_app(app).await;
    
    // Mirror the archive - it will get initial HAS first, then advanced HAS at the end
    let temp_dest = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dest.path().to_str().unwrap();
    
    let mirror_config = cmd_mirror::MirrorConfig {
        src: server_url.clone(),
        dst: format!("file://{}", mirror_dest),
        concurrency: 4,
        high: None,  // Unbounded mirror
        skip_optional: true,
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };
    
    println!("Mirroring from {} to {}", server_url, mirror_dest);
    
    match cmd_mirror::run(mirror_config).await {
        Ok(_) => println!("Mirror completed"),
        Err(e) => {
            server_handle.abort();
            panic!("Mirror failed: {}", e);
        }
    }
    
    // Check what HAS was written
    let dest_has_path = std::path::Path::new(mirror_dest).join(".well-known/stellar-history.json");
    let dest_has_content = std::fs::read_to_string(&dest_has_path)
        .expect("Failed to read destination HAS");
    let dest_has: serde_json::Value = serde_json::from_str(&dest_has_content)
        .expect("Failed to parse destination HAS");
    
    let claimed_ledger = dest_has["currentLedger"].as_u64().unwrap() as u32;
    println!("Destination HAS claims currentLedger: {}", claimed_ledger);
    
    // BUG: The HAS should reflect what we actually mirrored (127), not what the source
    // has advanced to (191) during our mirror operation
    assert_eq!(
        claimed_ledger, 127,
        "BUG EXPOSED: HAS claims currentLedger={} but we only mirrored up to 127! \
         The HAS file was re-read from source AFTER it advanced.",
        claimed_ledger
    );
    
    // Verify that NONE of the checkpoint 191 files were downloaded
    let checkpoint_bf_history = std::path::Path::new(mirror_dest)
        .join("history/00/00/00/history-000000bf.json");
    let checkpoint_bf_ledger = std::path::Path::new(mirror_dest)
        .join("ledger/00/00/00/ledger-000000bf.xdr.gz");
    let checkpoint_bf_tx = std::path::Path::new(mirror_dest)
        .join("transactions/00/00/00/transactions-000000bf.xdr.gz");
    let checkpoint_bf_results = std::path::Path::new(mirror_dest)
        .join("results/00/00/00/results-000000bf.xdr.gz");
    let checkpoint_bf_scp = std::path::Path::new(mirror_dest)
        .join("scp/00/00/00/scp-000000bf.xdr.gz");
    
    assert!(
        !checkpoint_bf_history.exists(),
        "history-000000bf.json should not exist since we only mirrored up to 127"
    );
    assert!(
        !checkpoint_bf_ledger.exists(),
        "ledger-000000bf.xdr.gz should not exist since we only mirrored up to 127"
    );
    assert!(
        !checkpoint_bf_tx.exists(),
        "transactions-000000bf.xdr.gz should not exist since we only mirrored up to 127"
    );
    assert!(
        !checkpoint_bf_results.exists(),
        "results-000000bf.xdr.gz should not exist since we only mirrored up to 127"
    );
    assert!(
        !checkpoint_bf_scp.exists(),
        "scp-000000bf.xdr.gz should not exist since we only mirrored up to 127"
    );
    
    // Verify that checkpoint 127 files DO exist (they should have been mirrored)
    let checkpoint_7f_history = std::path::Path::new(mirror_dest)
        .join("history/00/00/00/history-0000007f.json");
    let checkpoint_7f_ledger = std::path::Path::new(mirror_dest)
        .join("ledger/00/00/00/ledger-0000007f.xdr.gz");
    
    assert!(
        checkpoint_7f_history.exists(),
        "history-0000007f.json should exist as it was within our mirror range"
    );
    assert!(
        checkpoint_7f_ledger.exists(),
        "ledger-0000007f.xdr.gz should exist as it was within our mirror range"
    );
    
    server_handle.abort();
    println!("✓ test_mirror_race_condition_with_advancing_has passed");
}

#[tokio::test]
#[ignore] // Ignore by default as it requires network access
async fn test_diagnose_real_testnet_json_bug() {
    // Initialize tracing for debugging at DEBUG level
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    println!("\n=== DIAGNOSTIC TEST: Real Testnet JSON Bug ===\n");

    // First, let's directly test downloading a JSON file with different methods
    let json_url = "http://history.stellar.org/prd/core-testnet/core_testnet_001/history/00/00/00/history-0000003f.json";
    
    println!("1. Testing direct download with standard library HTTP client...");
    // Use std::process::Command to test with curl
    let curl_output = std::process::Command::new("curl")
        .arg("-s")
        .arg("-I")  // Headers only
        .arg(json_url)
        .output();
    
    match curl_output {
        Ok(output) => {
            let headers = String::from_utf8_lossy(&output.stdout);
            println!("   - HTTP Headers from curl:");
            for line in headers.lines().take(10) {
                println!("     {}", line);
            }
        }
        Err(e) => println!("   - curl failed: {}", e),
    }
    
    // Now get the actual content
    let curl_content = std::process::Command::new("curl")
        .arg("-s")
        .arg(json_url)
        .output();
    
    match curl_content {
        Ok(output) => {
            println!("   - Content length from curl: {} bytes", output.stdout.len());
            if output.stdout.len() > 0 {
                let preview = String::from_utf8_lossy(&output.stdout[..output.stdout.len().min(100)]);
                println!("   - First 100 chars: {}", preview);
            }
        }
        Err(e) => println!("   - curl content fetch failed: {}", e),
    }

    println!("\n2. Testing with OpenDAL (what mirror uses)...");
    use opendal::{Operator, services::Http};
    
    let mut http_builder = Http::default();
    http_builder = http_builder.endpoint("http://history.stellar.org");
    http_builder = http_builder.root("/prd/core-testnet/core_testnet_001");
    
    let op = Operator::new(http_builder)
        .expect("Failed to create operator")
        .finish();
    
    let history_path = "history/00/00/00/history-0000003f.json";
    
    println!("   - Reading file metadata...");
    match op.stat(history_path).await {
        Ok(metadata) => {
            println!("   - File exists: true");
            println!("   - Content length from metadata: {:?}", metadata.content_length());
            println!("   - Content type from metadata: {:?}", metadata.content_type());
        }
        Err(e) => println!("   - Failed to stat file: {}", e),
    }
    
    println!("   - Attempting to read file content...");
    match op.read(history_path).await {
        Ok(bytes) => {
            println!("   - Read {} bytes", bytes.len());
            if bytes.len() > 0 {
                let bytes_vec = bytes.to_bytes();
                let preview = String::from_utf8_lossy(&bytes_vec[..bytes_vec.len().min(100)]);
                println!("   - First 100 chars: {}", preview);
            } else {
                println!("   - WARNING: OpenDAL read returned empty bytes!");
            }
        }
        Err(e) => println!("   - Failed to read file: {}", e),
    }

    // Also test with buffer reading approach that mirror uses
    println!("\n   - Testing with buffer/reader approach (as used in mirror)...");
    match op.reader(history_path).await {
        Ok(reader) => {
            let mut collected_bytes = Vec::new();
            let mut offset = 0u64;
            loop {
                match reader.read(offset..offset+4096).await {
                    Ok(buf) if buf.is_empty() => break,
                    Ok(buf) => {
                        // Convert Buffer to bytes properly
                        let bytes = buf.to_bytes();
                        collected_bytes.extend_from_slice(&bytes);
                        offset += bytes.len() as u64;
                    }
                    Err(e) => {
                        println!("   - Error reading buffer: {}", e);
                        break;
                    }
                }
            }
            println!("   - Collected {} bytes via reader", collected_bytes.len());
            if collected_bytes.len() > 0 {
                let preview = String::from_utf8_lossy(&collected_bytes[..collected_bytes.len().min(100)]);
                println!("   - First 100 chars: {}", preview);
            } else {
                println!("   - WARNING: Reader approach collected 0 bytes!");
            }
        }
        Err(e) => println!("   - Failed to create reader: {}", e),
    }

    println!("\n3. Testing mirror operation with debug logging...");
    
    // Create temp directory for mirror destination
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dir.path().to_str().unwrap();

    // Mirror a tiny range from real testnet
    let mirror_config = cmd_mirror::MirrorConfig {
        src: "http://history.stellar.org/prd/core-testnet/core_testnet_001".to_string(),
        dst: format!("file://{}", mirror_dest),
        concurrency: 1, // Use single thread to make debugging easier
        high: Some(63), // Just the first checkpoint
        skip_optional: true,
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };

    println!("   - Mirroring from real testnet to {}", mirror_dest);

    match cmd_mirror::run(mirror_config).await {
        Ok(_) => println!("   - Mirror completed successfully"),
        Err(e) => {
            println!("   - Mirror had errors: {}", e);
        }
    }

    // Check the mirrored file
    let history_path = std::path::Path::new(mirror_dest)
        .join("history/00/00/00/history-0000003f.json");
    
    println!("\n4. Checking mirrored file...");
    if history_path.exists() {
        let history_metadata = std::fs::metadata(&history_path)
            .expect("Should be able to get file metadata");
        
        println!("   - File exists: true");
        println!("   - File size: {} bytes", history_metadata.len());
        
        if history_metadata.len() == 0 {
            println!("   ❌ BUG CONFIRMED: JSON file is empty after mirroring!");
            
            // Let's check other file types to see if they're affected
            let ledger_path = std::path::Path::new(mirror_dest)
                .join("ledger/00/00/00/ledger-0000003f.xdr.gz");
            if ledger_path.exists() {
                let ledger_size = std::fs::metadata(&ledger_path)
                    .map(|m| m.len())
                    .unwrap_or(0);
                println!("   - Ledger file size: {} bytes (compressed XDR)", ledger_size);
            }
            
            // Try to understand why - check if it's a write issue
            println!("\n   - Checking if this is a write issue...");
            let test_content = b"test content";
            let test_path = std::path::Path::new(mirror_dest).join("test.json");
            match std::fs::write(&test_path, test_content) {
                Ok(_) => {
                    let size = std::fs::metadata(&test_path).map(|m| m.len()).unwrap_or(0);
                    println!("   - Test write succeeded, file size: {} bytes", size);
                }
                Err(e) => println!("   - Test write failed: {}", e),
            }
        } else {
            println!("   ✓ JSON file has content!");
        }
    } else {
        println!("   - History file not found at {:?}", history_path);
    }

    println!("\n=== END DIAGNOSTIC TEST ===\n");
}

// Helper function to copy only partial archive (missing some ledger files)
fn copy_partial_archive(src: &std::path::Path, dst: &std::path::Path) {
    use std::fs;
    use walkdir::WalkDir;

    let mut skip_count = 0;

    for entry in WalkDir::new(src).into_iter().filter_map(|e| e.ok()) {
        let src_path = entry.path();
        let relative = src_path.strip_prefix(src).unwrap();
        let dst_path = dst.join(relative);

        if entry.file_type().is_dir() {
            fs::create_dir_all(&dst_path).ok();
        } else if entry.file_type().is_file() {
            // Skip every 3rd ledger file to create gaps
            if src_path.to_string_lossy().contains("/ledger/") && skip_count % 3 == 0 {
                skip_count += 1;
                continue; // Skip this file
            }
            skip_count += 1;

            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent).ok();
            }
            fs::copy(src_path, dst_path).ok();
        }
    }
}

// Helper function to copy archive but omit some files to cause download failures
fn copy_archive_with_unreadable_files(src: &std::path::Path, dst: &std::path::Path) {
    use std::fs;
    use walkdir::WalkDir;

    let mut file_count = 0;

    for entry in WalkDir::new(src).into_iter().filter_map(|e| e.ok()) {
        let src_path = entry.path();
        let relative = src_path.strip_prefix(src).unwrap();
        let dst_path = dst.join(relative);

        if entry.file_type().is_dir() {
            fs::create_dir_all(&dst_path).ok();
        } else if entry.file_type().is_file() {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent).ok();
            }
            
            // Skip specific files to cause 404 errors
            let path_str = src_path.to_string_lossy();
            if path_str.contains("transactions-000000bf.xdr.gz") || 
               path_str.contains("bucket-") && file_count % 15 == 0 {
                // Don't copy these files - they'll 404 when mirror tries to download
                println!("Skipping file to cause failure: {}", relative.display());
                file_count += 1;
                continue;
            }
            
            fs::copy(src_path, &dst_path).ok();
            file_count += 1;
        }
    }
}
