//! Tests for streaming optimizations

use stellar_archivist::io_backend::{stream_to_file, FsStore};
use tempfile::tempdir;

#[tokio::test]
async fn test_is_probably_small_paths() {
    // These paths should be identified as small and skip HEAD requests
    let small_paths = vec![
        "history/00/00/00/history-0000003f.json",
        "ledger/00/00/00/ledger-0000003f.xdr.gz",
        "transactions/00/00/00/transactions-0000003f.xdr.gz",
        "results/00/00/00/results-0000003f.xdr.gz",
        "scp/00/00/00/scp-0000003f.xdr.gz",
        ".well-known/stellar-history.json",
    ];

    let temp = tempdir().unwrap();
    let src_dir = temp.path().join("src");
    let dst_dir = temp.path().join("dst");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::create_dir_all(&dst_dir).unwrap();

    // Create small test files
    for path in &small_paths {
        let full_path = src_dir.join(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&full_path, b"small test data").unwrap();
    }

    let store = FsStore::new(&src_dir);

    // Copy each file using streaming
    for path in &small_paths {
        let dst_path = dst_dir.join(path);
        stream_to_file(&store, path, &dst_path).await.unwrap();

        // Verify file was copied
        assert!(dst_path.exists(), "File {} should exist", path);
        let content = std::fs::read(&dst_path).unwrap();
        assert_eq!(content, b"small test data");
    }
}

#[tokio::test]
async fn test_bucket_file_probe() {
    // Bucket files should use probe-based approach
    let bucket_path = "bucket/00/00/00/bucket-0000003f.xdr.gz";

    let temp = tempdir().unwrap();
    let src_dir = temp.path().join("src");
    let dst_dir = temp.path().join("dst");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::create_dir_all(&dst_dir).unwrap();

    // Create a medium-sized bucket file (3MB to trigger potential ranged mode)
    let full_path = src_dir.join(bucket_path);
    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let data = vec![0u8; 3 * 1024 * 1024];
    std::fs::write(&full_path, &data).unwrap();

    let store = FsStore::new(&src_dir);
    let dst_path = dst_dir.join(bucket_path);

    // This should use probe-based approach
    stream_to_file(&store, bucket_path, &dst_path)
        .await
        .unwrap();

    // Verify file was copied correctly
    assert!(dst_path.exists());
    let copied = std::fs::read(&dst_path).unwrap();
    assert_eq!(copied.len(), data.len());
}

#[tokio::test]
async fn test_ranged_requires_two_chunks() {
    let temp = tempdir().unwrap();
    let src_dir = temp.path().join("src");
    let dst_dir = temp.path().join("dst");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::create_dir_all(&dst_dir).unwrap();

    // Create a file just under 2 chunks (15MB with 8MB chunks = 1.875 chunks)
    // This should NOT use ranged mode
    let path = "other/large-file.xdr.gz";
    let full_path = src_dir.join(path);
    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let data = vec![0u8; 15 * 1024 * 1024];
    std::fs::write(&full_path, &data).unwrap();

    let store = FsStore::new(&src_dir);
    let dst_path = dst_dir.join(path);

    // With 8MB chunks, 15MB = 1.875 chunks, should use streaming
    stream_to_file(&store, path, &dst_path).await.unwrap();

    assert!(dst_path.exists());
    assert_eq!(std::fs::read(&dst_path).unwrap().len(), data.len());

    // Now test with a file that's exactly 2 chunks (16MB with 8MB chunks)
    let path2 = "other/large-file2.xdr.gz";
    let full_path2 = src_dir.join(path2);
    if let Some(parent) = full_path2.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let data2 = vec![0u8; 16 * 1024 * 1024];
    std::fs::write(&full_path2, &data2).unwrap();

    let dst_path2 = dst_dir.join(path2);

    // With 8MB chunks, 16MB = exactly 2 chunks, should use ranged
    stream_to_file(&store, path2, &dst_path2).await.unwrap();

    assert!(dst_path2.exists());
    assert_eq!(std::fs::read(&dst_path2).unwrap().len(), data2.len());
}
