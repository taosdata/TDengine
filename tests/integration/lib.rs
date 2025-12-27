/// taosX Integration Test Framework
///
/// This module provides the foundation for all taosX integration tests.
/// It organizes tests by category:
///
/// - `core`: Core functionality (TMQ, backup, replication)
/// - `datasources`: Data source connector tests
/// - `e2e`: End-to-end scenario tests
/// - `common`: Shared fixtures and utilities
#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "datasources/mod.rs"]
mod datasources;

#[cfg(test)]
#[path = "core/mod.rs"]
mod core;

#[cfg(test)]
mod e2e;

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
    use super::*;

    #[test]
    fn test_framework_initialization() {
        init_logger();
        println!("✓ taosX Integration Test Framework initialized");
    }
}
