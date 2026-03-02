/// taosX Integration Test Framework
///
/// This module provides the foundation for all taosX integration tests.
/// It organizes tests by category:
///
/// - `common`: Shared fixtures and utilities
/// - `core`: Core functionality (TMQ, backup, replication)
/// - `e2e`: End-to-end scenario tests
/// - `datasources`: Data source connector tests
pub mod common;
pub mod core;

#[cfg(test)]
mod datasources;

/// Integration test initialization hook
#[cfg(test)]
fn init_logger() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[test]
    fn test_framework_initialization() {
        init_logger();
        println!("✓ taosX Integration Test Framework initialized");
    }

    /// 打印当前进程的所有环境变量（按名称排序），便于调试与排查配置。
    #[test]
    fn test_print_all_env_vars() {
        init_logger();
        let mut vars: Vec<_> = env::vars().collect();
        vars.sort_by(|a, b| a.0.cmp(&b.0));
        tracing::info!("environment variables (count = {}):", vars.len());
        for (k, v) in &vars {
            tracing::info!("  {}={}", k, v);
        }
        println!("✓ printed {} environment variables", vars.len());
    }
}
