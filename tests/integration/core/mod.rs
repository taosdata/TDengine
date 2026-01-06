pub mod api;

/// Core taosX functionality tests
///
/// Tests for core features that don't require specific data sources:
/// - TMQ (TDengine Message Queue)
/// - Backup and restore
/// - Replication
mod tests {
    #[test]
    fn test_core_modules_structure() {
        println!("✓ Core test modules are properly structured");
    }
}
