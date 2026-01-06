use crate::common::helpers::terminate_process;
use crate::common::TestServiceConfig;
use crate::core::api::ApiClient;

use serde_json::Value;
use std::time::Duration;

/// Integration coverage for data source listing/detail and point template download.
/// The assertions are intentionally tolerant: they ensure endpoints are reachable
/// and return plausible payloads without requiring specific fixture data.
#[test]
fn test_data_sources_and_points_api() {
    // Spin up service
    let config = TestServiceConfig::new();
    let (_tempfile, mut cmd) = config.serve();
    let mut child = cmd.spawn().expect("failed to start taosx server");

    // Wait for server to be ready
    std::thread::sleep(Duration::from_secs(5));

    let client = ApiClient::new(&config.api_base_url());

    // List data sources (no language)
    let list_default = client.list_data_sources(None);
    assert!(
        list_default.is_ok(),
        "list_data_sources (default) should succeed: {:?}",
        list_default.err()
    );
    let sources_default: Value = list_default.unwrap();
    println!("Listed data sources (default): {:?}", sources_default);

    // List data sources (English)
    let list_en = client.list_data_sources(Some("en"));
    assert!(
        list_en.is_ok(),
        "list_data_sources(en) should succeed: {:?}",
        list_en.err()
    );
    let sources_en: Value = list_en.unwrap();
    println!("Listed data sources (en): {:?}", sources_en);

    // Try fetching a specific data source if any are returned
    if let Some(first_id) = sources_default
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.get("id").or_else(|| v.get("name")))
        .and_then(|v| v.as_str())
    {
        let ds = client.get_data_source(first_id, None);
        assert!(
            ds.is_ok(),
            "get_data_source({first_id}) should succeed: {:?}",
            ds.err()
        );
        println!("Fetched data source {first_id}: {:?}", ds.unwrap());
    } else {
        println!("No data source definitions available to fetch individually");
    }

    // Download point template for a known driver (opcua)
    let template = client.download_point_template("opcua", None);
    assert!(
        template.is_ok(),
        "download_point_template(opcua) should succeed: {:?}",
        template.err()
    );
    let template_bytes = template.unwrap();
    assert!(
        !template_bytes.is_empty(),
        "point template should not be empty"
    );
    println!(
        "Downloaded point template (opcua), size: {} bytes",
        template_bytes.len()
    );

    // Cleanup server
    terminate_process(child.id());
    let _ = child.wait();
}
