/// Common utilities for all integration tests
///
/// Provides:
/// - Health checks for external services
/// - Test fixtures and data generators
/// - Helper functions for DSN construction
/// - Common test assertions
pub mod fixtures;
pub mod health_check;
pub mod helpers;

/// Test environment configuration
#[derive(Debug, Clone)]
pub struct TestEnv {
    pub taos_host: String,
    pub taos_port: u16,
    pub taos_user: String,
    pub taos_password: String,
    pub test_db: String,
}

impl Default for TestEnv {
    fn default() -> Self {
        Self {
            taos_host: std::env::var("TAOS_HOST").unwrap_or("localhost".to_string()),
            taos_port: std::env::var("TAOS_PORT")
                .unwrap_or("6030".to_string())
                .parse()
                .unwrap_or(6030),
            taos_user: std::env::var("TAOS_USER").unwrap_or("root".to_string()),
            taos_password: std::env::var("TAOS_PASSWORD").unwrap_or("taosdata".to_string()),
            test_db: format!(
                "test_db_{}",
                uuid::Uuid::new_v4().to_string().replace("-", "")
            ),
        }
    }
}

impl TestEnv {
    /// Get TDengine connection DSN
    pub fn taos_dsn(&self) -> String {
        format!(
            "taos://{}:{}@{}:{}/{}",
            self.taos_user, self.taos_password, self.taos_host, self.taos_port, self.test_db
        )
    }

    /// Get TDengine WebSocket DSN
    pub fn taos_ws_dsn(&self) -> String {
        format!(
            "taos+ws://{}:{}@{}:{}/{}",
            self.taos_user, self.taos_password, self.taos_host, self.taos_port, self.test_db
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_configuration() {
        let env = TestEnv::default();
        println!("Test environment: {:?}", env);
        assert!(!env.taos_host.is_empty());
        assert!(env.taos_port > 0);
        assert!(!env.test_db.is_empty());
    }

    #[test]
    fn test_dsn_generation() {
        let env = TestEnv::default();
        let dsn = env.taos_dsn();
        println!("Generated DSN: {}", dsn);
        assert!(dsn.contains("taos://"));
        assert!(dsn.contains(&env.taos_host));

        let ws_dsn = env.taos_ws_dsn();
        println!("Generated WS DSN: {}", ws_dsn);
        assert!(ws_dsn.contains("taos+ws://"));
        assert!(ws_dsn.contains(&env.taos_host));
    }
}
