//! Tests for command-line interface validation and error handling
//! 
//! These tests ensure that malformed inputs and invalid configurations
//! are rejected early at the command interface level.

use stellar_archivist::cmd_mirror;

#[tokio::test]
async fn test_mirror_rejects_http_destination() {
    // HTTP destinations are read-only and should be rejected immediately
    let config = cmd_mirror::MirrorConfig {
        src: "file:///tmp/test-source".to_string(),
        dst: "http://example.com/archive".to_string(),
        concurrency: 4,
        skip_optional: false,
        high: None,
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };
    
    let result = cmd_mirror::run(config).await;
    assert!(result.is_err(), "Should reject HTTP destination");
    
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Destination must be a filesystem path"),
        "Error should mention filesystem requirement, got: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_mirror_rejects_https_destination() {
    // HTTPS destinations are also read-only
    let config = cmd_mirror::MirrorConfig {
        src: "file:///tmp/test-source".to_string(),
        dst: "https://example.com/archive".to_string(),
        concurrency: 4,
        skip_optional: false,
        high: None,
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };
    
    let result = cmd_mirror::run(config).await;
    assert!(result.is_err(), "Should reject HTTPS destination");
    
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Destination must be a filesystem path"),
        "Error should mention filesystem requirement, got: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_mirror_rejects_s3_destination() {
    // S3 destinations are not currently supported for writing
    let config = cmd_mirror::MirrorConfig {
        src: "file:///tmp/test-source".to_string(),
        dst: "s3://my-bucket/archive".to_string(),
        concurrency: 4,
        skip_optional: false,
        high: None,
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };
    
    let result = cmd_mirror::run(config).await;
    assert!(result.is_err(), "Should reject S3 destination");
    
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Destination must be a filesystem path") || 
        err_msg.contains("Unsupported URL scheme"),
        "Error should reject S3 destination, got: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_mirror_rejects_malformed_source_url() {
    // Test various malformed source URLs
    let test_cases = vec![
        "not-a-url",
        "://missing-scheme",
        "file:/missing-slash",
        "http//missing-colon",
        "",
    ];
    
    for bad_src in test_cases {
        let config = cmd_mirror::MirrorConfig {
            src: bad_src.to_string(),
            dst: "file:///tmp/test-dest".to_string(),
            concurrency: 4,
            skip_optional: false,
            high: None,
            force: false,
            window_size: None,
            max_bucket_cache: None,
            window_workers: None,
        };
        
        let result = cmd_mirror::run(config).await;
        assert!(
            result.is_err(),
            "Should reject malformed source URL: '{}'",
            bad_src
        );
    }
}

#[tokio::test]
async fn test_mirror_rejects_malformed_destination_url() {
    // Test various malformed destination URLs
    let test_cases = vec![
        "not-a-url",
        "://missing-scheme",
        "file:/missing-slash",
        "",
    ];
    
    for bad_dst in test_cases {
        let config = cmd_mirror::MirrorConfig {
            src: "file:///tmp/test-source".to_string(),
            dst: bad_dst.to_string(),
            concurrency: 4,
            skip_optional: false,
            high: None,
            force: false,
            window_size: None,
            max_bucket_cache: None,
            window_workers: None,
        };
        
        let result = cmd_mirror::run(config).await;
        assert!(
            result.is_err(),
            "Should reject malformed destination URL: '{}'",
            bad_dst
        );
    }
}

#[tokio::test]
async fn test_mirror_accepts_valid_file_destination() {
    use std::path::PathBuf;
    
    // Use the existing test archive that we know is valid
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");
    
    // Only test if the test archive exists
    if !test_archive_path.exists() {
        eprintln!("Skipping test: test archive not found");
        return;
    }
    
    use tempfile::TempDir;
    let temp_dest = TempDir::new().expect("Failed to create temp dest");
    
    // Test with file:// scheme
    let config = cmd_mirror::MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", temp_dest.path().display()),
        concurrency: 4,
        skip_optional: true,  // Skip optional to speed up test
        high: Some(63),  // Only mirror first checkpoint to speed up test
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };
    
    let result = cmd_mirror::run(config).await;
    assert!(
        result.is_ok(),
        "Should accept valid file:// destination, got error: {:?}",
        result
    );
    
    // Verify that the HAS file was created
    assert!(
        temp_dest.path().join(".well-known/stellar-history.json").exists(),
        "HAS file should exist in destination"
    );
}

#[tokio::test]
async fn test_mirror_rejects_nonexistent_source() {
    // Source that doesn't exist should fail gracefully
    let config = cmd_mirror::MirrorConfig {
        src: "file:///this/path/does/not/exist/at/all".to_string(),
        dst: "file:///tmp/test-dest".to_string(),
        concurrency: 4,
        skip_optional: false,
        high: None,
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };
    
    let result = cmd_mirror::run(config).await;
    assert!(result.is_err(), "Should reject nonexistent source");
}

#[tokio::test]
async fn test_mirror_creates_destination_if_not_exists() {
    use std::path::PathBuf;
    use tempfile::TempDir;
    
    // Use the existing test archive that we know is valid
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");
    
    // Only test if the test archive exists
    if !test_archive_path.exists() {
        eprintln!("Skipping test: test archive not found");
        return;
    }
    
    // Create a temp directory, then use a subdirectory that doesn't exist yet
    let temp_base = TempDir::new().expect("Failed to create temp base");
    let dest_path = temp_base.path().join("new-dest-directory");
    
    let config = cmd_mirror::MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,  // Skip optional to speed up test
        high: Some(63),  // Only mirror first checkpoint to speed up test
        force: false,
        window_size: None,
        max_bucket_cache: None,
        window_workers: None,
    };
    
    let result = cmd_mirror::run(config).await;
    assert!(
        result.is_ok(),
        "Should create destination directory if it doesn't exist, got error: {:?}",
        result
    );
    
    // Verify the destination was created
    assert!(dest_path.exists(), "Destination directory should have been created");
    assert!(dest_path.join(".well-known/stellar-history.json").exists(), 
            "HAS file should exist in destination");
}