/// Test fixtures and data generators
///
/// Provides reusable test data and setup/teardown utilities
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

    #[test]
    fn test_sample_data_with_time() {
        let start_time = 1000000i64;
        let data = SampleData::generate_with_time(5, start_time);
        assert_eq!(data.records.len(), 5);
        assert_eq!(data.records[0].timestamp, start_time);
    }

    #[test]
    fn test_table_config_creation() {
        let config = TestTableConfig::default_with_db("test");
        assert_eq!(config.db, "test");
        assert!(!config.name.is_empty());
        assert_eq!(config.columns.len(), 3);
    }

    #[test]
    fn test_create_table_sql() {
        let config = TestTableConfig::default_with_db("test");
        let sql = config.create_table_sql();
        assert!(sql.contains("CREATE TABLE"));
        assert!(sql.contains("test."));
        assert!(sql.contains("TAGS"));
    }

    #[test]
    fn test_context_creation() {
        let ctx = TestContext::new();
        assert!(!ctx.db_name.is_empty());
        let dsn = ctx.dsn();
        assert!(dsn.contains("taos://"));
    }
}
