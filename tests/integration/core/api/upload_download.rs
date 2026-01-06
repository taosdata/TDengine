use crate::common::helpers::terminate_process;
use crate::common::TestServiceConfig;
use crate::core::api::ApiClient;

use std::time::Duration;

/// Integration test for upload/download APIs:
/// - POST /upload
/// - GET  /check_exists
/// - GET  /download
#[test]
fn test_upload_download_api() {
    // Spin up service
    let config = TestServiceConfig::new();
    let (_tempfile, mut cmd) = config.serve();
    let mut child = cmd.spawn().expect("failed to start taosx server");

    // wait for server to be ready
    std::thread::sleep(Duration::from_secs(5));

    let client = ApiClient::new(&config.api_base_url());

    // Prepare a small file payload
    let mut rng = fastrand::Rng::new();
    let req_id: String = (0..12).map(|_| rng.alphanumeric()).collect();
    let filename = "hello.txt";
    let file_contents = b"hello taosx upload/download test".to_vec();

    // Upload file
    let upload_result = client.upload_files(&req_id, vec![(filename, file_contents.clone())]);
    assert!(
        upload_result.is_ok(),
        "upload failed: {:?}",
        upload_result.err()
    );
    let uploaded_paths: serde_json::Value = upload_result.unwrap();
    let first_path = uploaded_paths
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("upload response should contain path string");

    // Check existence
    let exists = client
        .check_file_exists(first_path)
        .expect("check file exists failed");
    assert!(exists, "uploaded file should exist via check_exists");

    // Download and verify content
    let downloaded = client
        .download_file(first_path)
        .expect("download failed for uploaded file");
    assert_eq!(
        downloaded, file_contents,
        "downloaded contents mismatch original upload"
    );

    // Cleanup server
    terminate_process(child.id());
    let _ = child.wait();
}
