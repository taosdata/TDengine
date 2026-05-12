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

#[cfg(feature = "test-tmq")]
pub mod tmq;

#[cfg(feature = "test-opcua")]
pub mod opcua;

#[cfg(feature = "test-opcda")]
pub mod opcda;

#[cfg(feature = "test-pi")]
pub mod pi;

#[cfg(feature = "test-historian")]
pub mod historian;

/// Reads integration test environment variable with `INTEGRATION_TEST_` prefix.
#[cfg(all(
    test,
    any(
        feature = "test-kafka",
        feature = "test-mqtt",
        feature = "test-tmq",
        feature = "test-opcua"
    )
))]
pub(crate) fn env_var(key: &str) -> anyhow::Result<String> {
    use anyhow::Context;
    std::env::var(format!("INTEGRATION_TEST_{}", key))
        .with_context(|| format!("INTEGRATION_TEST_{} not set", key))
}

/// Derives Explorer root URL from INTEGRATION_TEST_TAOSX_API_BASE_URL.
///
/// This is the single source of truth for integration tests.
/// CI sets INTEGRATION_TEST_TAOSX_API_BASE_URL=http://127.0.0.1:${PORT_6060}/api/x/
/// and we derive http://127.0.0.1:${PORT_6060}/ from it.
#[cfg(all(test, any(feature = "test-mqtt", feature = "test-opcua")))]
pub(crate) fn explorer_base_url_from_env() -> String {
    env_var("TAOSX_API_BASE_URL")
        .ok()
        .and_then(|value| derive_explorer_base_url_from_api(&value))
        .unwrap_or_else(|| "http://localhost:6060/".to_string())
}

#[cfg(all(test, any(feature = "test-mqtt", feature = "test-opcua")))]
pub(crate) fn build_explorer_client_from_env() -> anyhow::Result<crate::core::api::ExplorerApiClient>
{
    let explorer_base = explorer_base_url_from_env();
    let username = env_var("TAOSX_API_USERNAME").unwrap_or_else(|_| "root".to_string());
    let password = env_var("TAOSX_API_PASSWORD").unwrap_or_else(|_| "taosdata".to_string());
    crate::core::api::ExplorerApiClient::builder(&explorer_base)
        .with_auth(&username, &password)
        .build()
}

/// Rewrites every task `to` field in an imported payload to the provided target DSN.
#[cfg(all(test, any(feature = "test-mqtt", feature = "test-opcua")))]
pub(crate) fn rewrite_task_target_dsn(
    payload: &mut serde_json::Value,
    target_dsn: &str,
) -> anyhow::Result<()> {
    use anyhow::Context;

    let tasks = payload["tasks"]
        .as_array_mut()
        .context("fixture must contain a tasks array")?;
    for task in tasks {
        task["to"] = serde_json::Value::String(target_dsn.to_string());
    }
    Ok(())
}

/// Derives Explorer base URL from API base URL.
/// Strips path/query/fragment and normalizes to root.
#[cfg(all(test, any(feature = "test-mqtt", feature = "test-opcua")))]
fn derive_explorer_base_url_from_api(raw: &str) -> Option<String> {
    let mut url = url::Url::parse(raw).ok()?;
    url.set_path("/");
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string())
}

#[cfg(all(
    test,
    any(feature = "test-kafka", feature = "test-mqtt", feature = "test-tmq")
))]
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

#[cfg(all(test, any(feature = "test-mqtt", feature = "test-opcua")))]
mod tests {
    use super::{derive_explorer_base_url_from_api, rewrite_task_target_dsn};

    #[test]
    fn test_derive_explorer_base_url_from_api_base_url() {
        let url = derive_explorer_base_url_from_api("http://localhost:46060/api/x/")
            .expect("API base URL should parse");

        assert_eq!(url, "http://localhost:46060/");
    }

    #[test]
    fn test_derive_explorer_base_url_strips_path_and_query() {
        let url = derive_explorer_base_url_from_api("http://localhost:46060/api/x/?foo=bar#frag")
            .expect("API base URL with query and fragment should parse");

        assert_eq!(url, "http://localhost:46060/");
    }

    #[test]
    fn test_rewrite_task_target_dsn_replaces_all_tasks() {
        let mut payload = serde_json::json!({
            "tasks": [
                { "name": "a", "to": "taos+http://root:taosdata@localhost:6041/test" },
                { "name": "b", "to": "taos+http://root:taosdata@localhost:6041/test" }
            ]
        });

        rewrite_task_target_dsn(&mut payload, "taos+ws://localhost:6041/integration_test")
            .expect("target dsn rewrite should succeed");

        let tasks = payload["tasks"]
            .as_array()
            .expect("tasks should remain an array");
        assert_eq!(
            tasks[0]["to"],
            serde_json::Value::String("taos+ws://localhost:6041/integration_test".to_string())
        );
        assert_eq!(
            tasks[1]["to"],
            serde_json::Value::String("taos+ws://localhost:6041/integration_test".to_string())
        );
    }
}
