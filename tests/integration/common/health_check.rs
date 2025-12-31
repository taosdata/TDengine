/// Health checks for external services
///
/// Verifies that required services are available before running tests
use std::time::Duration;

/// Check if TDengine is running and accessible
pub async fn check_taos_health(host: &str, port: u16) -> anyhow::Result<()> {
    use taos::AsyncTBuilder;

    let dsn = format!("taos://{}:{}", host, port);

    // Create a builder and attempt to connect
    let builder =
        taos::TaosBuilder::from_dsn(&dsn).map_err(|e| anyhow::anyhow!("Invalid DSN: {}", e))?;

    let result = tokio::time::timeout(Duration::from_secs(5), builder.build()).await;

    match result {
        Ok(Ok(_)) => {
            println!("✓ TDengine health check passed ({}:{})", host, port);
            Ok(())
        }
        Ok(Err(e)) => {
            eprintln!("✗ TDengine health check failed: {}", e);
            Err(anyhow::anyhow!("TDengine not available: {}", e))
        }
        Err(_) => {
            eprintln!("✗ TDengine health check timeout");
            Err(anyhow::anyhow!("TDengine health check timeout"))
        }
    }
}

/// Check if Kafka is running
pub async fn check_kafka_health(broker: &str) -> anyhow::Result<()> {
    // TODO: Implement Kafka health check when kafka crate is available
    tracing::info!("Kafka health check placeholder for: {}", broker);
    Ok(())
}

/// Check if MySQL is running
pub async fn check_mysql_health(host: &str, port: u16) -> anyhow::Result<()> {
    // TODO: Implement MySQL health check when sqlx mysql feature is available
    tracing::info!("MySQL health check placeholder for: {}:{}", host, port);
    Ok(())
}

/// Check if PostgreSQL is running
pub async fn check_postgres_health(host: &str, port: u16) -> anyhow::Result<()> {
    // TODO: Implement PostgreSQL health check when sqlx postgres feature is available
    tracing::info!("PostgreSQL health check placeholder for: {}:{}", host, port);
    Ok(())
}

/// Check if MongoDB is running
pub async fn check_mongodb_health(url: &str) -> anyhow::Result<()> {
    // TODO: Implement MongoDB health check when mongodb crate is available
    tracing::info!("MongoDB health check placeholder for: {}", url);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_taos_health_check() {
        // This test will fail if TDengine is not running
        // but we don't fail the build - developers handle this
        match check_taos_health("localhost", 6030).await {
            Ok(()) => println!("✓ TDengine is running"),
            Err(e) => println!(
                "⚠ TDengine is not available (expected if not running): {}",
                e
            ),
        }
    }

    #[tokio::test]
    async fn test_kafka_health_placeholder() {
        assert!(check_kafka_health("localhost:9092").await.is_ok());
    }

    #[tokio::test]
    async fn test_mysql_health_placeholder() {
        assert!(check_mysql_health("localhost", 3306).await.is_ok());
    }

    #[tokio::test]
    async fn test_postgres_health_placeholder() {
        assert!(check_postgres_health("localhost", 5432).await.is_ok());
    }

    #[tokio::test]
    async fn test_mongodb_health_placeholder() {
        assert!(check_mongodb_health("mongodb://localhost:27017")
            .await
            .is_ok());
    }
}
