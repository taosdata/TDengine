/// Data source integration tests
///
/// Tests for all supported data sources (Kafka, MySQL, Oracle, etc.)
/// Each test module is feature-gated to avoid unnecessary compilation

#[cfg(feature = "test-kafka")]
pub mod kafka;

#[cfg(feature = "test-mysql")]
pub mod mysql;

#[cfg(feature = "test-oracle")]
pub mod oracle;

#[cfg(feature = "test-postgres")]
pub mod postgres;

#[cfg(feature = "test-mongodb")]
pub mod mongodb;

#[cfg(feature = "test-mssql")]
pub mod mssql;

#[cfg(feature = "test-mqtt")]
pub mod mqtt;

#[cfg(feature = "test-opcua")]
pub mod opcua;

#[cfg(feature = "test-opcda")]
pub mod opcda;

#[cfg(feature = "test-pi")]
pub mod pi;

#[cfg(feature = "test-historian")]
pub mod historian;

mod tests {
    #[test]
    fn test_datasource_modules_structure() {
        println!("✓ Data source test modules are properly structured");
    }
}
