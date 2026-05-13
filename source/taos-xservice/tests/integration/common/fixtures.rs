/// Test fixtures and data generators
///
/// Provides reusable test data and setup/teardown utilities
use std::path::PathBuf;

use anyhow::{Context, Result};
use uuid::Uuid;

/// Sample data for testing
#[derive(Debug, Clone)]
pub struct SampleData {
    pub records: Vec<TestRecord>,
}

#[derive(Debug, Clone)]
pub struct TestRecord {
    pub id: String,
    pub timestamp: i64,
    pub value: f64,
    pub tags: std::collections::BTreeMap<String, String>,
}

impl SampleData {
    /// Generate sample records for testing
    pub fn generate(count: usize) -> Self {
        let mut records = Vec::new();
        let base_time = chrono::Utc::now().timestamp_millis();

        for i in 0..count {
            let mut tags = std::collections::BTreeMap::new();
            tags.insert("location".to_string(), format!("loc_{}", i % 5));
            tags.insert("device".to_string(), format!("dev_{}", i % 3));

            records.push(TestRecord {
                id: format!("record_{}", i),
                timestamp: base_time + (i as i64 * 1000),
                value: 10.0 + (i as f64 * 0.5),
                tags,
            });
        }

        Self { records }
    }

    /// Generate with specific timestamp
    pub fn generate_with_time(count: usize, start_time: i64) -> Self {
        let mut records = Vec::new();

        for i in 0..count {
            let mut tags = std::collections::BTreeMap::new();
            tags.insert("location".to_string(), format!("loc_{}", i % 5));
            tags.insert("device".to_string(), format!("dev_{}", i % 3));

            records.push(TestRecord {
                id: format!("record_{}", i),
                timestamp: start_time + (i as i64 * 1000),
                value: 10.0 + (i as f64 * 0.5),
                tags,
            });
        }

        Self { records }
    }
}

/// Test table configuration
#[derive(Debug, Clone)]
pub struct TestTableConfig {
    pub name: String,
    pub db: String,
    pub columns: Vec<ColumnDef>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: String,
}

impl TestTableConfig {
    /// Create a default test table configuration
    pub fn default_with_db(db_name: &str) -> Self {
        let uuid = Uuid::new_v4().simple().to_string();
        Self {
            name: format!("test_table_{}", &uuid[..8]),
            db: db_name.to_string(),
            columns: vec![
                ColumnDef {
                    name: "ts".to_string(),
                    col_type: "TIMESTAMP".to_string(),
                },
                ColumnDef {
                    name: "value".to_string(),
                    col_type: "DOUBLE".to_string(),
                },
                ColumnDef {
                    name: "status".to_string(),
                    col_type: "INT".to_string(),
                },
            ],
            tags: vec!["location".to_string(), "device".to_string()],
        }
    }

    /// Get CREATE TABLE statement for TDengine
    pub fn create_table_sql(&self) -> String {
        let columns = self
            .columns
            .iter()
            .map(|c| format!("{} {}", c.name, c.col_type))
            .collect::<Vec<_>>()
            .join(", ");

        let tags = self
            .tags
            .iter()
            .map(|t| format!("{} VARCHAR(256)", t))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "CREATE TABLE IF NOT EXISTS {}.{} ({}) TAGS ({})",
            self.db, self.name, columns, tags
        )
    }
}

/// Test context that handles setup and teardown
pub struct TestContext {
    pub env: crate::common::TestEnv,
    pub db_name: String,
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

impl TestContext {
    /// Create a new test context with a unique database
    pub fn new() -> Self {
        let env = crate::common::TestEnv::default();
        let uuid = Uuid::new_v4().simple().to_string();
        Self {
            db_name: format!("test_db_{}", uuid),
            env,
        }
    }

    /// Get the DSN for this test context
    pub fn dsn(&self) -> String {
        format!(
            "taos://{}:{}@{}:{}/{}",
            self.env.taos_user,
            self.env.taos_password,
            self.env.taos_host,
            self.env.taos_port,
            self.db_name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies fixture generation returns expected record content for sample data.
    #[test]
    fn test_sample_data_generation() {
        let data = SampleData::generate(10);
        assert_eq!(data.records.len(), 10);
        assert_eq!(data.records[0].id, "record_0");
        assert!(data.records[0].value > 0.0);
        assert_eq!(
            data.records[0].tags.get("location"),
            Some(&"loc_0".to_string())
        );
    }

    /// Ensures timestamp-seeded fixture generation starts from the requested base time.
    #[test]
    fn test_sample_data_with_time() {
        let start_time = 1000000i64;
        let data = SampleData::generate_with_time(5, start_time);
        assert_eq!(data.records.len(), 5);
        assert_eq!(data.records[0].timestamp, start_time);
    }

    /// Verifies default table config contains expected database binding and schema fields.
    #[test]
    fn test_table_config_creation() {
        let config = TestTableConfig::default_with_db("test");
        assert_eq!(config.db, "test");
        assert!(!config.name.is_empty());
        assert_eq!(config.columns.len(), 3);
    }

    /// Ensures generated CREATE TABLE SQL includes core table and tag definitions.
    #[test]
    fn test_create_table_sql() {
        let config = TestTableConfig::default_with_db("test");
        let sql = config.create_table_sql();
        assert!(sql.contains("CREATE TABLE"));
        assert!(sql.contains("test."));
        assert!(sql.contains("TAGS"));
    }

    /// Verifies test context creates a unique database name and valid DSN.
    #[test]
    fn test_context_creation() {
        let ctx = TestContext::new();
        assert!(!ctx.db_name.is_empty());
        let dsn = ctx.dsn();
        assert!(dsn.contains("taos://"));
    }
}

// ── Import/export fixture helpers ────────────────────────────────────────────

/// Returns the absolute path to the integration test fixtures root directory.
///
/// Resolved relative to `CARGO_MANIFEST_DIR` so it works regardless of the
/// working directory the test runner uses.
pub fn fixtures_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR is always set by cargo when running tests.
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set");
    PathBuf::from(manifest_dir).join("fixtures")
}

/// Returns the path to a fixture file located under `fixtures/<relative>`.
pub fn fixture_path(relative: &str) -> PathBuf {
    fixtures_dir().join(relative)
}

/// Returns the path to an import-export fixture file by bare name.
///
/// The file must exist under `fixtures/import-export/<name>`.
pub fn import_export_fixture_path(name: &str) -> PathBuf {
    fixture_path(&format!("import-export/{name}"))
}

/// Load an import-export fixture as raw bytes.
pub fn load_import_export_fixture(name: &str) -> Result<Vec<u8>> {
    let path = import_export_fixture_path(name);
    std::fs::read(&path).with_context(|| format!("load fixture {}", path.display()))
}

/// Extract and parse `tasks.json` from an in-memory ZIP archive produced by
/// the task-export endpoint.
pub fn extract_tasks_json_from_zip(zip_bytes: &[u8]) -> Result<serde_json::Value> {
    use std::io::{Cursor, Read};

    let cursor = Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(cursor).context("open zip archive")?;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).context("read zip entry")?;
        if entry.name() == "tasks.json" {
            let mut buf = String::new();
            entry.read_to_string(&mut buf).context("read tasks.json")?;
            return serde_json::from_str(&buf).context("parse tasks.json");
        }
    }
    anyhow::bail!("tasks.json not found in ZIP")
}

/// List all file entry names inside a ZIP archive.
pub fn list_zip_entries(zip_bytes: &[u8]) -> Result<Vec<String>> {
    use std::io::Cursor;

    let cursor = Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(cursor).context("open zip archive")?;
    let names = (0..archive.len())
        .map(|i| archive.by_index(i).map(|e| e.name().to_string()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("read zip entry names")?;
    Ok(names)
}

#[cfg(test)]
mod import_export_tests {
    use super::*;

    /// Verifies the mqtt-legacy.json fixture exists and contains valid JSON with a tasks array.
    #[test]
    fn test_mqtt_legacy_fixture_is_valid_json() {
        let bytes = load_import_export_fixture("mqtt-legacy.json")
            .expect("mqtt-legacy.json fixture must exist");
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).expect("mqtt-legacy.json must be valid JSON");
        assert!(
            json.get("tasks").is_some(),
            "mqtt-legacy.json must have a 'tasks' field"
        );
        let tasks = json["tasks"].as_array().expect("'tasks' must be an array");
        assert!(!tasks.is_empty(), "tasks array must not be empty");
        // MQTT task should have a 'from' with type 'mqtt'.
        let task = &tasks[0];
        assert_eq!(
            task["from"]["type"].as_str(),
            Some("mqtt"),
            "first task source type must be mqtt"
        );
    }

    /// Verifies the opcua-legacy.json fixture exists and is a valid JSON export.
    #[test]
    fn test_opcua_legacy_fixture_is_valid_json() {
        let bytes = load_import_export_fixture("opcua-legacy.json")
            .expect("opcua-legacy.json fixture must exist");
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).expect("opcua-legacy.json must be valid JSON");
        assert!(
            json.get("tasks").is_some(),
            "opcua-legacy.json must have a 'tasks' field"
        );
        let tasks = json["tasks"].as_array().expect("'tasks' must be an array");
        assert!(!tasks.is_empty(), "tasks array must not be empty");
        assert_eq!(
            tasks[0]["from"]["type"].as_str(),
            Some("opcua"),
            "first task source type must be opcua"
        );
        let csv_config_file = tasks[0]["from"]["data"]["csv_config_file"]
            .as_str()
            .expect("opcua fixture csv_config_file must be a string");
        assert!(
            csv_config_file.starts_with("@resources/"),
            "opcua legacy fixture must point at a repo-owned resource path: {csv_config_file}"
        );
    }

    #[test]
    fn test_opcua_legacy_resource_file_exists() {
        let path = import_export_fixture_path("resources/opcua/opcua-points.csv");
        assert!(
            path.exists(),
            "opcua legacy CSV resource must exist at {}",
            path.display()
        );
    }

    /// Verifies the opcua-with-files.zip fixture exists and contains tasks.json plus a csv file.
    #[test]
    fn test_opcua_zip_fixture_contains_expected_entries() {
        let bytes = load_import_export_fixture("opcua-with-files.zip")
            .expect("opcua-with-files.zip fixture must exist");
        let entries = list_zip_entries(&bytes).expect("must be a valid ZIP");
        assert!(
            entries.contains(&"tasks.json".to_string()),
            "ZIP must contain tasks.json; entries: {entries:?}"
        );
        let has_csv = entries.iter().any(|e| e.ends_with(".csv"));
        assert!(
            has_csv,
            "ZIP must contain at least one .csv entry; entries: {entries:?}"
        );
    }

    /// Verifies tasks.json can be extracted from the opcua ZIP and parsed correctly.
    #[test]
    fn test_extract_tasks_json_from_opcua_zip() {
        let bytes = load_import_export_fixture("opcua-with-files.zip")
            .expect("opcua-with-files.zip fixture must exist");
        let tasks_json =
            extract_tasks_json_from_zip(&bytes).expect("must extract tasks.json from ZIP");
        assert!(
            tasks_json.get("tasks").is_some(),
            "extracted tasks.json must have a 'tasks' field"
        );
    }

    /// Verifies the fixture_path helper returns an existing path for known fixtures.
    #[test]
    fn test_fixture_path_resolves_to_existing_file() {
        let path = import_export_fixture_path("mqtt-legacy.json");
        assert!(
            path.exists(),
            "fixture path must exist on disk: {}",
            path.display()
        );
    }

    /// Verifies the mqtt-with-tls.zip fixture contains tasks.json and all required TLS cert files.
    #[test]
    fn test_mqtt_tls_zip_fixture_contains_expected_entries() {
        let bytes = load_import_export_fixture("mqtt-with-tls.zip")
            .expect("mqtt-with-tls.zip fixture must exist");
        let entries = list_zip_entries(&bytes).expect("must be a valid ZIP");

        assert!(
            entries.contains(&"tasks.json".to_string()),
            "ZIP must contain tasks.json; entries: {entries:?}"
        );

        let expected_tls_files = [
            "files/tls/ca.pem",
            "files/tls/client.pem",
            "files/tls/client-key.pem",
        ];

        for expected in &expected_tls_files {
            assert!(
                entries.contains(&expected.to_string()),
                "ZIP must contain {expected}; entries: {entries:?}"
            );
        }
    }

    /// Verifies tasks.json in mqtt-with-tls.zip references bundled TLS files via @files/ paths.
    #[test]
    fn test_mqtt_tls_zip_tasks_json_references_bundled_files() {
        let bytes = load_import_export_fixture("mqtt-with-tls.zip")
            .expect("mqtt-with-tls.zip fixture must exist");
        let tasks_json =
            extract_tasks_json_from_zip(&bytes).expect("must extract tasks.json from ZIP");

        let tasks = tasks_json["tasks"]
            .as_array()
            .expect("tasks.json must have tasks array");
        assert!(!tasks.is_empty(), "tasks array must not be empty");

        let task = &tasks[0];
        assert_eq!(
            task["from"]["type"].as_str(),
            Some("mqtt"),
            "task source type must be mqtt"
        );

        let data = &task["from"]["data"];
        assert_eq!(
            data["tls_ca_file"].as_str(),
            Some("@files/tls/ca.pem"),
            "tls_ca_file must reference bundled file"
        );
        assert_eq!(
            data["tls_cert_file"].as_str(),
            Some("@files/tls/client.pem"),
            "tls_cert_file must reference bundled file"
        );
        assert_eq!(
            data["tls_key_file"].as_str(),
            Some("@files/tls/client-key.pem"),
            "tls_key_file must reference bundled file"
        );
    }
}
