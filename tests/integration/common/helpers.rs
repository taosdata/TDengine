/// Helper functions for tests
///
/// Provides utility functions used across multiple test suites
/// Create a basic DSN from components
pub fn build_dsn(driver: &str, host: &str, port: u16, database: &str) -> String {
    format!("{}://{}:{}/{}", driver, host, port, database)
}

/// Create a DSN with authentication
pub fn build_dsn_with_auth(
    driver: &str,
    user: &str,
    password: &str,
    host: &str,
    port: u16,
    database: &str,
) -> String {
    format!(
        "{}://{}:{}@{}:{}/{}",
        driver, user, password, host, port, database
    )
}

/// Create a DSN with additional parameters
pub fn build_dsn_with_params(base_dsn: &str, params: &[(&str, &str)]) -> String {
    if params.is_empty() {
        return base_dsn.to_string();
    }

    let param_str = params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&");

    if base_dsn.contains('?') {
        format!("{}&{}", base_dsn, param_str)
    } else {
        format!("{}?{}", base_dsn, param_str)
    }
}

/// Wait for a condition with timeout
pub async fn wait_for<F, Fut>(
    condition: F,
    timeout_secs: u64,
    check_interval_ms: u64,
) -> anyhow::Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);

    loop {
        if condition().await {
            return Ok(());
        }

        if start.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Condition not met within {} seconds",
                timeout_secs
            ));
        }

        tokio::time::sleep(std::time::Duration::from_millis(check_interval_ms)).await;
    }
}

/// Generate unique test database name
pub fn generate_test_db_name(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4().simple().to_string();
    format!(
        "{}_{}_{}",
        prefix,
        chrono::Local::now().format("%Y%m%d%H%M%S"),
        &uuid[..8]
    )
}

/// Generate unique test table name
pub fn generate_test_table_name(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4().simple().to_string();
    format!(
        "{}_{}_{}",
        prefix,
        chrono::Local::now().format("%Y%m%d%H%M%S"),
        &uuid[..8]
    )
}

pub fn terminate_process(pid: u32) {
    if cfg!(windows) {
        // do nothing;
    } else if let Err(err) = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::SIGTERM,
    ) {
        eprintln!("Failed to terminate process {}: {}", pid, err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_dsn() {
        let dsn = build_dsn("taos", "localhost", 6030, "test");
        assert_eq!(dsn, "taos://localhost:6030/test");
    }

    #[test]
    fn test_build_dsn_with_auth() {
        let dsn = build_dsn_with_auth("taos", "root", "taosdata", "localhost", 6030, "test");
        assert_eq!(dsn, "taos://root:taosdata@localhost:6030/test");
    }

    #[test]
    fn test_build_dsn_with_params() {
        let base_dsn = "taos://localhost:6030/test";
        let dsn = build_dsn_with_params(base_dsn, &[("param1", "value1"), ("param2", "value2")]);
        assert!(dsn.contains("param1=value1"));
        assert!(dsn.contains("param2=value2"));
    }

    #[test]
    fn test_generate_test_db_name() {
        let name = generate_test_db_name("test");
        assert!(name.starts_with("test_"));
        assert!(name.len() > 10);
    }

    #[test]
    fn test_generate_test_table_name() {
        let name = generate_test_table_name("t");
        assert!(name.starts_with("t_"));
        assert!(name.len() > 10);
    }

    #[tokio::test]
    async fn test_wait_for_success() {
        let result = wait_for(|| async { true }, 1, 100).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_timeout() {
        let result = wait_for(|| async { false }, 1, 100).await;
        assert!(result.is_err());
    }
}
