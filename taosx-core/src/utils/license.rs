/// Check if an TDengine dsn is in cloud env
pub fn is_cloud(to: &taos::Dsn) -> bool {
    debug_assert!(
        matches!(to.driver.as_str(), "tmq" | "taos"),
        "Invalid driver: {}",
        to.driver
    );
    to.protocol
        .as_ref()
        .map(|p| match p.as_str() {
            "http" | "https" | "ws" | "wss" => true,
            _ => false,
        })
        .unwrap_or(false)
        && to.get("token").is_some()
}
