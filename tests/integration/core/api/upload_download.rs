use crate::common::helpers::terminate_process;
use crate::common::TestServiceConfig;
use crate::core::api::ApiClient;

use std::time::Duration;

/// Integration test for upload/download APIs:
/// - POST /upload
/// - GET  /check_exists
/// - GET  /download
#[tokio::test]
async fn test_upload_download_api() {
    // Spin up service
    let config = TestServiceConfig::new();
    let (_tempfile, mut cmd) = config.serve();
    let mut child = cmd.spawn().expect("failed to start taosx server");

    // wait for server to be ready
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = ApiClient::builder(&config.api_base_url())
        .build()
        .expect("build api client");

    // Prepare a small file payload
    let filename = "hello.txt";
    let file_contents = b"hello taosx upload/download test".to_vec();

    // Upload file
    let upload_result = client
        .upload_files(vec![(filename, file_contents.clone())])
        .await;
    assert!(
        upload_result.is_ok(),
        "upload failed: {:?}",
        upload_result.err()
    );
    let uploaded_paths: Vec<String> = upload_result.unwrap();
    let first_path = uploaded_paths
        .first()
        .expect("upload response should contain path string");

    // Check existence
    let exists = client
        .check_file_exists(first_path)
        .await
        .expect("check file exists failed");
    assert!(exists, "uploaded file should exist via check_exists");

    // Download and verify content
    let downloaded = client
        .download_file(first_path)
        .await
        .expect("download failed for uploaded file");
    assert_eq!(
        downloaded, file_contents,
        "downloaded contents mismatch original upload"
    );

    // Cleanup server
    if let Some(pid) = child.id() {
        terminate_process(pid);
    }
    let _ = child.wait().await;
}
