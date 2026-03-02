//! Data source integration tests
//!
//! Tests for all supported data sources (Kafka, MySQL, Oracle, etc.)
//! Each test module is feature-gated to avoid unnecessary compilation

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

/// 读取带 `INTEGRATION_TEST_` 前缀的环境变量，返回 `anyhow::Result<String>`。
#[cfg(all(test, any(feature = "test-all-datasources", feature = "test-mqtt")))]
pub(crate) fn env_var(key: &str) -> anyhow::Result<String> {
    use anyhow::Context;
    std::env::var(format!("INTEGRATION_TEST_{}", key))
        .with_context(|| format!("INTEGRATION_TEST_{} not set", key))
}

#[cfg(all(test, any(feature = "test-all-datasources", feature = "test-mqtt")))]
pub async fn resolve_agent_via(
    client: &crate::core::api::ApiClient,
    with_agent: bool,
) -> anyhow::Result<Option<i64>> {
    use anyhow::Context;

    if !with_agent {
        return Ok(None);
    }

    let agent_name = env_var("AGENT_NAME")?;
    let agent = client
        .get_agent_by_name(&agent_name)
        .await
        .with_context(|| format!("get agent by name {} via api", agent_name))?;
    Ok(Some(agent.id))
}
