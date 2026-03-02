use std::path::Path;

use anyhow::Context;

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

const LLVM_COV_TARGET_DIR: &str = "LLVM_COV_TARGET_DIR";

pub struct TestServiceConfig {
    data_dir: String,
    instance_id: u32,
    serve_listen: String,
    grpc_listen: String,
    monitor_fqdn: String,
    monitor_interval: u32,
}

impl Default for TestServiceConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl TestServiceConfig {
    pub fn new() -> Self {
        let mut rng = fastrand::Rng::new();
        let i = rng.i32(7050..=30050);
        let tempdir = format!("/tmp/taosx_test_data_{}", i);
        Self {
            data_dir: tempdir,
            instance_id: 99,
            serve_listen: format!("127.0.0.1:{}", i),
            grpc_listen: format!("127.0.0.1:{}", i + 1),
            monitor_fqdn: "127.0.0.1".to_string(),
            monitor_interval: 5,
        }
    }

    fn to_toml(&self) -> String {
        format!(
            r#"
data_dir = "{}"
instanceId = {}
[serve]
listen = "{}"
grpc = "{}"
[monitor]
fqdn = "{}"
interval = {}
            "#,
            self.data_dir,
            self.instance_id,
            self.serve_listen,
            self.grpc_listen,
            self.monitor_fqdn,
            self.monitor_interval
        )
    }

    #[allow(dead_code)]
    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    pub fn serve(&self) -> (tempfile::NamedTempFile, tokio::process::Command) {
        let mut cmd = std::env::var(LLVM_COV_TARGET_DIR)
            .with_context(|| format!("No {} environment variable set", LLVM_COV_TARGET_DIR))
            .and_then(|path| {
                let path = Path::new(&path);
                let taosx = path.join("taosx");
                if taosx.exists() {
                    Ok(tokio::process::Command::new(taosx))
                } else {
                    Err(anyhow::anyhow!(
                        "No taosx binary found in {}",
                        path.display()
                    ))
                }
            })
            .unwrap_or_else(|_err| {
                println!("fallback to path taosx");
                tokio::process::Command::new("taosx")
            });
        let toml = self.to_toml();
        let tempfile = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tempfile.path(), toml).unwrap();
        if let Ok(env) = std::env::var("LLVM_PROFILE_FILE") {
            println!("LLVM_PROFILE_FILE: {}", env);
            cmd.env("LLVM_PROFILE_FILE", env);
        }
        cmd.arg("serve")
            .arg("--config")
            .arg(tempfile.path())
            .arg("--data-dir")
            .arg(&self.data_dir)
            .arg("--instance-id")
            .arg(self.instance_id.to_string())
            .arg("--log-level")
            .arg("debug")
            .arg("-D")
            .arg(format!("sqlite:{}/taosx.db", self.data_dir));
        (tempfile, cmd)
    }

    pub fn api_base_url(&self) -> String {
        format!("http://{}", self.serve_listen)
    }
}
impl Drop for TestServiceConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

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
