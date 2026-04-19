//! MQTT data source integration tests

use rumqttc::v5::mqttbytes::QoS;
use tokio_util::sync::CancellationToken;

use crate::datasources::env_var;

#[cfg(all(test, feature = "test-mqtt"))]
pub struct MqttPubBuilder {
    schema: std::path::PathBuf,
    host: String,
    topic: String,
    port: Option<u16>,
    qos: Option<QoS>,
    username_password: Option<(String, String)>,
}

#[cfg(all(test, feature = "test-mqtt"))]
impl MqttPubBuilder {
    pub fn new(
        schema: impl AsRef<std::path::Path>,
        host: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            schema: schema.as_ref().to_path_buf(),
            host: host.into(),
            topic: topic.into(),
            port: None,
            qos: None,
            username_password: None,
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn qos(mut self, qos: QoS) -> Self {
        self.qos = Some(qos);
        self
    }

    pub fn username_password(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username_password = Some((username.into(), password.into()));
        self
    }

    /// Publishes fake JSON messages until `cancel` is cancelled, then shuts down gracefully.
    pub async fn publish(self, cancel: CancellationToken) -> anyhow::Result<()> {
        use std::time::Duration;

        use anyhow::Context;
        use rumqttc::v5::mqttbytes::v5::{ConnectReturnCode, PubAckReason, PubRecReason};
        use rumqttc::v5::{AsyncClient, ConnectionError, Event, Incoming};
        use rumqttc::Outgoing;

        let port = if let Some(port) = self.port {
            port
        } else {
            env_var("MQTT_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1883)
        };

        let qos = self.qos.unwrap_or(QoS::AtLeastOnce);

        let (username, password) = if let Some((u, p)) = self.username_password {
            (Some(u), Some(p))
        } else {
            (env_var("MQTT_USERNAME").ok(), env_var("MQTT_PASSWORD").ok())
        };

        let client_id = env_var("MQTT_CLIENT_ID").unwrap_or_else(|_| {
            format!(
                "taosx_integration_test_mqtt_pub_{}",
                (0..8).map(|_| fastrand::alphanumeric()).collect::<String>()
            )
        });
        let keep_alive_secs: u16 = env_var("MQTT_KEEP_ALIVE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);

        let schema = fake_data::json::DataFakeSchema::from_file(&self.schema)
            .map_err(|e| anyhow::anyhow!("load schema: {}", e))?;

        let mut opts = rumqttc::v5::MqttOptions::new(client_id, self.host, port);
        opts.set_keep_alive(keep_alive_secs);
        if let (Some(u), Some(p)) = (&username, &password) {
            opts.set_credentials(u, p);
        }

        let (client, mut event_loop) = AsyncClient::new(opts, 10);

        let Some(conn_result) = cancel.run_until_cancelled(event_loop.poll()).await else {
            return Ok(());
        };
        let Event::Incoming(Incoming::ConnAck(ack)) = conn_result.context("mqtt ConnAck error")?
        else {
            anyhow::bail!("expected ConnAck packet");
        };
        if !matches!(ack.code, ConnectReturnCode::Success) {
            anyhow::bail!("connect error: {:?}", ack.code);
        }
        tracing::info!("mqtt connected successfully");

        let cancel_for_event_loop = cancel.clone();
        let event_loop_task = tokio::spawn(async move {
            while let Some(poll_result) = cancel_for_event_loop
                .run_until_cancelled(event_loop.poll())
                .await
            {
                match poll_result {
                    Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                        if !matches!(ack.reason, PubAckReason::Success) {
                            tracing::warn!("mqtt PubAck: {:?}", ack.reason);
                        }
                    }
                    Ok(Event::Incoming(Incoming::PubRec(ack))) => {
                        if !matches!(ack.reason, PubRecReason::Success) {
                            tracing::warn!("mqtt PubRec: {:?}", ack.reason);
                        }
                    }
                    Ok(Event::Outgoing(Outgoing::Publish(_))) => {}
                    Ok(_) => {}
                    Err(ConnectionError::RequestsDone) => {
                        tracing::info!("mqtt requests done");
                        break;
                    }
                    Err(e) => {
                        tracing::error!("mqtt event_loop poll error: {}", e);
                        if cancel_for_event_loop
                            .run_until_cancelled(tokio::time::sleep(Duration::from_millis(100)))
                            .await
                            .is_none()
                        {
                            break;
                        }
                    }
                }
            }
        });

        let mut published_count: u32 = 0;
        loop {
            let payload_value = schema.rand_json_value().context("generate fake json")?;
            let payload = serde_json::to_vec(&payload_value).context("serialize json")?;
            let publish_fut = client.publish(self.topic.as_str(), qos, false, payload);
            let Some(pub_result) = cancel.run_until_cancelled(publish_fut).await else {
                break;
            };
            pub_result.context("mqtt publish")?;
            published_count += 1;
            if published_count.is_multiple_of(10)
                && cancel
                    .run_until_cancelled(tokio::time::sleep(Duration::from_millis(100)))
                    .await
                    .is_none()
            {
                break;
            }
        }

        tracing::info!("mqtt publish loop ended (cancelled)");

        drop(client);
        tracing::info!("mqtt client dropped");
        if let Some(join_result) = cancel.run_until_cancelled(event_loop_task).await {
            join_result.context("join mqtt event loop task failed")?;
        }
        tracing::info!("mqtt event loop task finished");

        Ok(())
    }
}

#[cfg(all(test, feature = "test-mqtt"))]
pub fn mqtt_pub(
    schema: impl AsRef<std::path::Path>,
    host: impl Into<String>,
    topic: impl Into<String>,
) -> MqttPubBuilder {
    MqttPubBuilder::new(schema, host, topic)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::{
        core::api::client::rewrite_json_file_refs,
        datasources::{build_explorer_client_from_env, env_var, rewrite_task_target_dsn},
    };

    use anyhow::Context;
    use rumqttc::v5::mqttbytes::QoS;
    use tokio_util::sync::CancellationToken;

    use ha_core::activity::TaskStatus;

    use crate::core::api::{ApiCheckValidParamClient, ApiClient, NewTask};

    use taosx_test_macros::integration_test;
    use taosx_utils::taos_conn::TaosConn;

    fn build_api_client_from_env() -> anyhow::Result<ApiClient> {
        let api_base = env_var("TAOSX_API_BASE_URL")?;
        let api_username = env_var("TAOSX_API_USERNAME").unwrap_or_else(|_| "root".to_string());
        let api_password = env_var("TAOSX_API_PASSWORD").unwrap_or_else(|_| "taosdata".to_string());
        let client = ApiClient::builder(&api_base)
            .with_auth(&api_username, &api_password)
            .build()?;
        Ok(client)
    }

    async fn cleanup_table(table: &str) -> anyhow::Result<()> {
        let host_target_dsn = env_var("HOST_TARGET_DSN")?;
        let taos_conn = TaosConn::create(&host_target_dsn, 3)
            .await
            .with_context(|| format!("create taos conn for cleanup of table {}", table))?;
        let sql = format!("DROP STABLE IF EXISTS {}", table);
        taos_conn
            .exec(&sql)
            .await
            .with_context(|| format!("drop stable {}", table))?;
        Ok(())
    }

    /// Create a single-topic MQTT task and verify data ingestion.
    ///
    /// Flow:
    /// 1. Build an MQTT task configuration with one topic and create the task via HTTP API.
    /// 2. Wait until the task status becomes running.
    /// 3. Run publish and wait_until_written_rows concurrently; when wait returns, cancel the token to stop publish.
    /// 4. Stop and delete the task.
    /// 5. Drop the task-specific MQTT stable in TDengine to clean up test data.
    ///
    /// Expected result:
    ///
    /// The task successfully reaches the running state, consumes published messages,
    /// increases written rows, and is fully stopped, deleted, and cleaned up without errors.
    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_mqtt_task_basic(with_agent: bool) -> anyhow::Result<()> {
        tracing::info!("{}", test_name);
        let mqtt_host = env_var("MQTT_HOST")?;
        let mqtt_port = env_var("MQTT_PORT")?
            .parse::<u16>()
            .context("invalid INTEGRATION_TEST_MQTT_PORT")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;

        let client = build_api_client_from_env()?;

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let task_cfg_path = manifest_dir.join("config/task/mqtt.json");
        let mut parser_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&task_cfg_path)
                .with_context(|| format!("read mqtt task config {:?}", task_cfg_path))?,
        )
        .context("parse mqtt task parser json")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");
        let stable_name = format!("{task_name}_meters");
        parser_json["model"]["using"] = serde_json::Value::String(stable_name.clone());
        let sub_table_pattern = format!(
            "{stable}_{task}_{{id}}",
            stable = stable_name,
            task = task_name
        );
        parser_json["model"]["name"] = serde_json::Value::String(sub_table_pattern);

        let topic = format!("integration_test_topic/{task_name}");
        let client_id = format!("integration_test_client_{task_name}");

        let from = format!(
            "mqtt://{mqtt_host}:{mqtt_port}?version=5&topics={topic}::1&client_id={client_id}"
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let new_task = NewTask {
            name: task_name.clone(),
            from,
            to: container_target_dsn.clone(),
            parser: Some(parser_json),
            via,
            labels: Some(vec!["type::datain".to_string()]),
        };

        let created = client
            .create_task(&new_task)
            .await
            .context("create mqtt task via api")?;
        let task_id = created.id;

        client
            .wait_until_running(task_id)
            .await
            .context("wait for mqtt task running")?;
        tracing::info!("mqtt task running");

        let schema_path = manifest_dir.join("config/schema/mqtt.toml");

        let cancel = CancellationToken::new();
        let cancel_for_pub = cancel.clone();
        let builder = super::mqtt_pub(schema_path, &mqtt_host, &topic).port(mqtt_port);
        let pub_handle = tokio::spawn(async move { builder.publish(cancel_for_pub).await });
        client
            .wait_until_written_rows(task_id, 1)
            .await
            .context("wait for task written_rows")?;
        tracing::info!("mqtt task written_rows reached");
        cancel.cancel();
        pub_handle
            .await
            .context("join mqtt publish task")?
            .context("publish fake mqtt json messages")?;
        tracing::info!("mqtt publish finished");

        client
            .stop_task(task_id)
            .await
            .with_context(|| format!("stop mqtt task id {}", task_id))?;
        tracing::info!("mqtt task stopped");

        client
            .wait_until_status(task_id, TaskStatus::Stopped)
            .await
            .context("wait for mqtt task stopped")?;
        tracing::info!("mqtt task stopped");

        // cleanup: delete task and verify it is gone
        client
            .delete_task(task_id)
            .await
            .context("delete mqtt task via api")?;
        tracing::info!("mqtt task deleted");

        // verify task is deleted
        let get_after = client
            .get_task(task_id)
            .await
            .context("get mqtt task via api")?;
        if get_after.is_some() {
            anyhow::bail!("task {} should have been deleted but still exists", task_id);
        }

        cleanup_table(&stable_name)
            .await
            .context("cleanup mqtt_meters after test_mqtt_task_with_fake_data")?;

        Ok(())
    }

    /// Create a multi-topic MQTT task and verify wildcard subscription behavior.
    ///
    /// Flow:
    /// 1. Build an MQTT task that subscribes to two concrete topics under the same prefix.
    /// 2. Create the task via HTTP API and wait until it is running.
    /// 3. Run two publishers (one per topic) and wait_until_written_rows concurrently; when wait returns, cancel to stop both publishers.
    /// 4. Stop and delete the task.
    /// 5. Drop the task-specific MQTT stable in TDengine to clean up test data.
    ///
    /// Expected result:
    ///
    /// The task correctly receives data from both topics, written rows become greater than zero,
    /// and the task can be stopped, deleted, and cleaned up without leaving residual metadata.
    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_mqtt_task_with_multiple_topics(with_agent: bool) -> anyhow::Result<()> {
        tracing::info!("{}", test_name);

        let mqtt_host = env_var("MQTT_HOST")?;
        let mqtt_port = env_var("MQTT_PORT")?
            .parse::<u16>()
            .context("invalid INTEGRATION_TEST_MQTT_PORT")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;

        let client = build_api_client_from_env()?;

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let task_cfg_path = manifest_dir.join("config/task/mqtt.json");
        let mut parser_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&task_cfg_path)
                .with_context(|| format!("read mqtt task config {:?}", task_cfg_path))?,
        )
        .context("parse mqtt task parser json")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_multi_{name_suffix}");
        let stable_name = format!("{task_name}_meters");
        parser_json["model"]["using"] = serde_json::Value::String(stable_name.clone());
        let sub_table_pattern = format!(
            "{stable}_{task}_{{id}}",
            stable = stable_name,
            task = task_name
        );
        parser_json["model"]["name"] = serde_json::Value::String(sub_table_pattern);

        let topic_prefix = format!("integration_multi/{task_name}");
        let topic_a = format!("{topic_prefix}/a");
        let topic_b = format!("{topic_prefix}/b");

        let client_id = format!("integration_test_client_{task_name}");
        let topics_param = format!("{topic_a}::1,{topic_b}::1");

        let from = format!(
            "mqtt://{mqtt_host}:{mqtt_port}?version=5&topics={topics_param}&client_id={client_id}",
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let new_task = NewTask {
            name: task_name.clone(),
            from,
            to: container_target_dsn.clone(),
            parser: Some(parser_json),
            via,
            labels: Some(vec!["type::datain".to_string()]),
        };

        let created = client
            .create_task(&new_task)
            .await
            .context("create multi-topic mqtt task via api")?;
        let task_id = created.id;

        client
            .wait_until_running(task_id)
            .await
            .context("wait for multi-topic mqtt task running")?;
        tracing::info!("multi-topic mqtt task running");

        let schema_path = manifest_dir.join("config/schema/mqtt.toml");

        let cancel = CancellationToken::new();
        let cancel_a = cancel.clone();
        let cancel_b = cancel.clone();
        let builder_a = super::mqtt_pub(schema_path.clone(), &mqtt_host, &topic_a).port(mqtt_port);
        let builder_b = super::mqtt_pub(schema_path, &mqtt_host, &topic_b)
            .port(mqtt_port)
            .qos(QoS::AtLeastOnce);
        let pub_handle_a = tokio::spawn(async move { builder_a.publish(cancel_a).await });
        let pub_handle_b = tokio::spawn(async move { builder_b.publish(cancel_b).await });
        client
            .wait_until_written_rows(task_id, 1)
            .await
            .context("wait for multi-topic mqtt task written_rows")?;
        tracing::info!("multi-topic mqtt task written_rows reached");
        cancel.cancel();
        pub_handle_a
            .await
            .context("join mqtt publish task for topic_a")?
            .context("publish fake mqtt json messages to first topic")?;
        pub_handle_b
            .await
            .context("join mqtt publish task for topic_b")?
            .context("publish fake mqtt json messages to second topic")?;
        tracing::info!("mqtt publish to both topics finished");

        client
            .stop_task(task_id)
            .await
            .with_context(|| format!("stop multi-topic mqtt task id {}", task_id))?;
        tracing::info!("multi-topic mqtt task stopped");

        client
            .wait_until_status(task_id, TaskStatus::Stopped)
            .await
            .context("wait for multi-topic mqtt task stopped")?;
        tracing::info!("multi-topic mqtt task stopped");

        client
            .delete_task(task_id)
            .await
            .context("delete multi-topic mqtt task via api")?;
        tracing::info!("multi-topic mqtt task deleted");

        let get_after = client
            .get_task(task_id)
            .await
            .context("get multi-topic mqtt task via api")?;
        if get_after.is_some() {
            anyhow::bail!(
                "multi-topic mqtt task {} should have been deleted but still exists",
                task_id
            );
        }

        cleanup_table(&stable_name)
            .await
            .context("cleanup mqtt_meters after test_mqtt_task_with_multiple_topics")?;

        Ok(())
    }

    /// Validate basic MQTT datasource connectivity without authentication.
    ///
    /// Flow:
    /// 1. Construct a simple MQTT DSN with a temporary topic and client_id.
    /// 2. Build a `DsnAgentQuery` with the DSN and optional agent information.
    /// 3. Call `validate_data_source` via the HTTP API.
    /// 4. Drop the `mqtt_meters` stable in TDengine to ensure no residual tables.
    ///
    /// Expected result:
    ///
    /// The validate API call succeeds and returns a positive result, proving the
    /// MQTT datasource is reachable with the given configuration and agent settings.
    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_mqtt_validate_datasource_basic(with_agent: bool) -> anyhow::Result<()> {
        tracing::info!("{}", test_name);

        let mqtt_host = env_var("MQTT_HOST")?;
        let mqtt_port = env_var("MQTT_PORT")?
            .parse::<u16>()
            .context("invalid INTEGRATION_TEST_MQTT_PORT")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;

        let client = build_api_client_from_env()?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");
        let topic = format!("integration_test_topic/{task_name}");
        let client_id = format!("integration_test_client_{task_name}");

        let from = format!(
            "mqtt://{mqtt_host}:{mqtt_port}?version=5&topics={topic}::1&client_id={client_id}"
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let body = ApiCheckValidParamClient {
            from: Some(from.clone()),
            from_json: None,
            to: container_target_dsn.clone(),
            via,
        };

        let result = client
            .validate_data_source(&body)
            .await
            .context("validate mqtt datasource via api")?;

        tracing::info!("mqtt validate datasource result: {}", result);

        cleanup_table("mqtt_meters")
            .await
            .context("cleanup mqtt_meters after test_mqtt_validate_datasource")?;

        Ok(())
    }

    /// Validate MQTT datasource connectivity using username and password.
    ///
    /// Flow:
    /// 1. Load MQTT auth port, username, and password from environment variables.
    /// 2. Construct an authenticated MQTT DSN that embeds the credentials.
    /// 3. Build a `DsnAgentQuery` with the DSN and optional agent information.
    /// 4. Call `validate_data_source` via the HTTP API and then drop `mqtt_meters`.
    ///
    /// Expected result:
    ///
    /// The validate API call succeeds under authenticated settings, indicating that
    /// the broker accepts the provided credentials and the datasource is reachable.
    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_mqtt_validate_datasource_with_auth(with_agent: bool) -> anyhow::Result<()> {
        tracing::info!("{}", test_name);

        let mqtt_host = env_var("MQTT_HOST")?;
        let mqtt_auth_port = env_var("MQTT_AUTH_PORT")?
            .parse::<u16>()
            .context("invalid INTEGRATION_TEST_MQTT_AUTH_PORT")?;
        let mqtt_username = env_var("MQTT_USERNAME")?;
        let mqtt_password = env_var("MQTT_PASSWORD")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;

        let client = build_api_client_from_env()?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");
        let topic = format!("integration_test_topic/{task_name}");
        let client_id = format!("integration_test_client_{task_name}");

        let from = format!(
            "mqtt://{mqtt_username}:{mqtt_password}@{mqtt_host}:{mqtt_auth_port}?version=5&topics={topic}::1&client_id={client_id}"
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let body = ApiCheckValidParamClient {
            from: Some(from.clone()),
            from_json: None,
            to: container_target_dsn.clone(),
            via,
        };

        let result = client
            .validate_data_source(&body)
            .await
            .context("validate auth mqtt datasource via api")?;

        tracing::info!("auth mqtt validate datasource result: {}", result);

        cleanup_table("mqtt_meters")
            .await
            .context("cleanup mqtt_meters after test_mqtt_validate_datasource_with_auth")?;

        Ok(())
    }

    /// Validate MQTT TLS datasource connectivity using uploaded certificates and credentials.
    ///
    /// Flow:
    /// 1. Upload CA, client certificate, and client key files via the upload API.
    /// 2. Build an MQTT DSN that embeds username/password and references the uploaded
    ///    certificate paths using `@path` syntax.
    /// 3. Call `validate_data_source` via the HTTP API using the constructed DSN.
    /// 4. Drop the `mqtt_meters` stable in TDengine to ensure no residual tables.
    ///
    /// Expected result:
    ///
    /// The validate API call succeeds with TLS and authentication enabled, proving that
    /// the MQTT broker is reachable using the provided certificates and credentials.
    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_mqtt_validate_datasource_with_tls_cert(with_agent: bool) -> anyhow::Result<()> {
        tracing::info!("{}", test_name);

        let mqtt_host = env_var("MQTT_HOST")?;
        let mqtt_tls_port = env_var("MQTT_TLS_PORT")?
            .parse::<u16>()
            .context("invalid INTEGRATION_TEST_MQTT_TLS_PORT")?;
        let mqtt_username = env_var("MQTT_USERNAME")?;
        let mqtt_password = env_var("MQTT_PASSWORD")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;

        let client = build_api_client_from_env()?;

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        // 1. Upload TLS certificate files and get server-side paths
        let ca_path = manifest_dir.join("config/tls/ca.pem");
        let cert_path = manifest_dir.join("config/tls/client.pem");
        let key_path = manifest_dir.join("config/tls/client-key.pem");

        let paths = client
            .upload_files_from_paths(vec![ca_path, cert_path, key_path])
            .await
            .context("upload mqtt tls cert files for validate via api")?;

        if paths.len() < 3 {
            anyhow::bail!(
                "upload tls validate response array length < 3, got {}",
                paths.len()
            );
        }
        let ca_server_path = paths[0].as_str();
        let cert_server_path = paths[1].as_str();
        let key_server_path = paths[2].as_str();

        tracing::info!("[validate tls] ca_server_path: {}", ca_server_path);
        tracing::info!("[validate tls] cert_server_path: {}", cert_server_path);
        tracing::info!("[validate tls] key_server_path: {}", key_server_path);

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");
        let topic = format!("integration_test_topic/{task_name}");
        let client_id = format!("integration_test_client_{task_name}");

        // DSN uses TLS certs uploaded to server; ca/cert/cert_key use @path syntax and MQTT username/password are embedded in the authority
        let from = format!(
            "mqtt://{mqtt_username}:{mqtt_password}@{mqtt_host}:{mqtt_tls_port}?version=5&topics={topic}::1&client_id={client_id}&ca=@{ca}&cert=@{cert}&cert_key=@{key}",
            ca = ca_server_path,
            cert = cert_server_path,
            key = key_server_path,
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let body = ApiCheckValidParamClient {
            from: Some(from.clone()),
            from_json: None,
            to: container_target_dsn.clone(),
            via,
        };

        let result = client
            .validate_data_source(&body)
            .await
            .context("validate tls mqtt datasource via api")?;

        tracing::info!("tls mqtt validate datasource result: {}", result);

        cleanup_table("mqtt_meters")
            .await
            .context("cleanup mqtt_meters after test_mqtt_validate_datasource_with_tls_cert")?;

        Ok(())
    }

    /// Create an authenticated MQTT task and verify data ingestion with credentials.
    ///
    /// Flow:
    /// 1. Construct an MQTT DSN that includes username, password, and auth port.
    /// 2. Create an MQTT task via HTTP API and wait until it is running.
    /// 3. Run publish and wait_until_written_rows concurrently; when wait returns, cancel the token to stop publish.
    /// 4. Stop and delete the task.
    /// 5. Drop the task-specific MQTT stable in TDengine to remove test data.
    ///
    /// Expected result:
    ///
    /// The authenticated task successfully connects with the provided credentials,
    /// consumes the published messages, reports written rows, and is gracefully
    /// stopped and deleted with no leftover TDengine stables.
    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_mqtt_task_with_auth(with_agent: bool) -> anyhow::Result<()> {
        tracing::info!("{}", test_name);

        let mqtt_host = env_var("MQTT_HOST")?;
        let mqtt_auth_port = env_var("MQTT_AUTH_PORT")?
            .parse::<u16>()
            .context("invalid INTEGRATION_TEST_MQTT_AUTH_PORT")?;
        let mqtt_username = env_var("MQTT_USERNAME")?;
        let mqtt_password = env_var("MQTT_PASSWORD")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;

        let client = build_api_client_from_env()?;

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let task_cfg_path = manifest_dir.join("config/task/mqtt.json");
        let mut parser_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&task_cfg_path)
                .with_context(|| format!("read mqtt task config {:?}", task_cfg_path))?,
        )
        .context("parse mqtt task parser json")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");
        let stable_name = format!("{task_name}_meters");
        parser_json["model"]["using"] = serde_json::Value::String(stable_name.clone());
        let sub_table_pattern = format!(
            "{stable}_{task}_{{id}}",
            stable = stable_name,
            task = task_name
        );
        parser_json["model"]["name"] = serde_json::Value::String(sub_table_pattern);

        let topic = format!("integration_test_topic/{task_name}");
        let client_id = format!("integration_test_client_{task_name}");

        // DSN uses MQTT username and password for authenticated connection
        let from = format!(
            "mqtt://{mqtt_username}:{mqtt_password}@{mqtt_host}:{mqtt_auth_port}?version=5&topics={topic}::1&client_id={client_id}"
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let new_task = NewTask {
            name: task_name.clone(),
            from,
            to: container_target_dsn.clone(),
            parser: Some(parser_json),
            via,
            labels: Some(vec!["type::datain".to_string()]),
        };

        let created = client
            .create_task(&new_task)
            .await
            .context("create auth mqtt task via api")?;
        let task_id = created.id;

        client
            .wait_until_running(task_id)
            .await
            .context("wait for auth mqtt task running")?;
        tracing::info!("auth mqtt task running");

        let schema_path = manifest_dir.join("config/schema/mqtt.toml");

        let cancel = CancellationToken::new();
        let cancel_for_pub = cancel.clone();
        let builder = super::mqtt_pub(schema_path, &mqtt_host, &topic)
            .port(mqtt_auth_port)
            .username_password(mqtt_username, mqtt_password);
        let pub_handle = tokio::spawn(async move { builder.publish(cancel_for_pub).await });
        client
            .wait_until_written_rows(task_id, 1)
            .await
            .context("wait for auth mqtt task written_rows")?;
        tracing::info!("auth mqtt task written_rows reached");
        cancel.cancel();
        pub_handle
            .await
            .context("join auth mqtt publish task")?
            .context("publish auth mqtt json messages")?;
        tracing::info!("auth mqtt publish finished");

        client
            .stop_task(task_id)
            .await
            .with_context(|| format!("stop auth mqtt task id {}", task_id))?;
        tracing::info!("auth mqtt task stopped");

        client
            .wait_until_status(task_id, TaskStatus::Stopped)
            .await
            .context("wait for auth mqtt task stopped")?;
        tracing::info!("auth mqtt task stopped");

        client
            .delete_task(task_id)
            .await
            .context("delete auth mqtt task via api")?;
        tracing::info!("auth mqtt task deleted");

        let get_after = client
            .get_task(task_id)
            .await
            .context("get auth mqtt task via api")?;
        if get_after.is_some() {
            anyhow::bail!(
                "auth mqtt task {} should have been deleted but still exists",
                task_id
            );
        }

        cleanup_table(&stable_name)
            .await
            .context("cleanup mqtt_meters after test_mqtt_task_with_auth")?;

        Ok(())
    }

    /// Create a TLS MQTT task using uploaded certificates and verify secure ingestion.
    ///
    /// Flow:
    /// 1. Upload CA, client certificate, and client key files via the upload API.
    /// 2. Build an MQTT DSN that references the uploaded certificate paths using `@path` syntax.
    /// 3. Create a TLS-enabled MQTT task via HTTP API and wait until it is running.
    /// 4. Run publish and wait_until_written_rows concurrently; when wait returns, cancel the token to stop publish.
    /// 5. Stop and delete the task and drop the task-specific MQTT stable.
    ///
    /// Expected result:
    ///
    /// The task establishes a TLS-secured connection using the uploaded certificates,
    /// successfully consumes messages from the broker, and leaves the system clean
    /// after task deletion and TDengine stable cleanup.
    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_mqtt_task_with_tls_cert(with_agent: bool) -> anyhow::Result<()> {
        tracing::info!("{}", test_name);

        let mqtt_host = env_var("MQTT_HOST")?;
        let mqtt_tls_port = env_var("MQTT_TLS_PORT")?
            .parse::<u16>()
            .context("invalid INTEGRATION_TEST_MQTT_TLS_PORT")?;
        let mqtt_username = env_var("MQTT_USERNAME")?;
        let mqtt_password = env_var("MQTT_PASSWORD")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;

        let client = build_api_client_from_env()?;

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        // 1. Upload TLS certificate files and get server-side paths
        let ca_path = manifest_dir.join("config/tls/ca.pem");
        let cert_path = manifest_dir.join("config/tls/client.pem");
        let key_path = manifest_dir.join("config/tls/client-key.pem");

        let paths = client
            .upload_files_from_paths(vec![ca_path, cert_path, key_path])
            .await
            .context("upload mqtt tls cert files via api")?;

        if paths.len() < 3 {
            anyhow::bail!("upload tls response array length < 3, got {}", paths.len());
        }
        let ca_server_path = paths[0].as_str();
        let cert_server_path = paths[1].as_str();
        let key_server_path = paths[2].as_str();

        tracing::info!("ca_server_path: {}", ca_server_path);
        tracing::info!("cert_server_path: {}", cert_server_path);
        tracing::info!("key_server_path: {}", key_server_path);

        let task_cfg_path = manifest_dir.join("config/task/mqtt.json");
        let mut parser_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&task_cfg_path)
                .with_context(|| format!("read mqtt task config {:?}", task_cfg_path))?,
        )
        .context("parse mqtt task parser json")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");
        let stable_name = format!("{task_name}_meters");
        parser_json["model"]["using"] = serde_json::Value::String(stable_name.clone());
        let sub_table_pattern = format!(
            "{stable}_{task}_{{id}}",
            stable = stable_name,
            task = task_name
        );
        parser_json["model"]["name"] = serde_json::Value::String(sub_table_pattern);

        let topic = format!("integration_test_topic/{task_name}");
        let client_id = format!("integration_test_client_{task_name}");

        // DSN uses TLS certs uploaded to server; ca/cert/cert_key use @path syntax and MQTT username/password are embedded in the authority
        let from = format!(
            "mqtt://{mqtt_username}:{mqtt_password}@{mqtt_host}:{mqtt_tls_port}?version=5&topics={topic}::1&client_id={client_id}&ca=@{ca}&cert=@{cert}&cert_key=@{key}",
            ca = ca_server_path,
            cert = cert_server_path,
            key = key_server_path,
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let new_task = NewTask {
            name: task_name.clone(),
            from,
            to: container_target_dsn.clone(),
            parser: Some(parser_json),
            via,
            labels: Some(vec!["type::datain".to_string()]),
        };

        let created = client
            .create_task(&new_task)
            .await
            .context("create tls mqtt task via api")?;
        let task_id = created.id;

        client
            .wait_until_running(task_id)
            .await
            .context("wait for tls mqtt task running")?;
        tracing::info!("tls mqtt task running");

        let schema_path = manifest_dir.join("config/schema/mqtt.toml");

        let mqtt_auth_port = env_var("MQTT_AUTH_PORT")?
            .parse::<u16>()
            .context("invalid INTEGRATION_TEST_MQTT_AUTH_PORT")?;
        let cancel = CancellationToken::new();
        let cancel_for_pub = cancel.clone();
        let builder = super::mqtt_pub(schema_path, &mqtt_host, &topic)
            .port(mqtt_auth_port)
            .username_password(mqtt_username, mqtt_password);
        let pub_handle = tokio::spawn(builder.publish(cancel_for_pub));
        client
            .wait_until_written_rows(task_id, 1)
            .await
            .context("wait for tls mqtt task written_rows")?;
        tracing::info!("tls mqtt task written_rows reached");
        cancel.cancel();
        pub_handle
            .await
            .context("join tls mqtt publish task")?
            .context("publish tls mqtt json messages")?;
        tracing::info!("tls mqtt publish finished");

        client
            .stop_task(task_id)
            .await
            .with_context(|| format!("stop tls mqtt task id {}", task_id))?;
        tracing::info!("tls mqtt task stopped");

        client
            .wait_until_status(task_id, TaskStatus::Stopped)
            .await
            .context("wait for tls mqtt task stopped")?;
        tracing::info!("tls mqtt task stopped");

        client
            .delete_task(task_id)
            .await
            .context("delete tls mqtt task via api")?;
        tracing::info!("tls mqtt task deleted");

        let get_after = client
            .get_task(task_id)
            .await
            .context("get tls mqtt task via api")?;
        if get_after.is_some() {
            anyhow::bail!(
                "tls mqtt task {} should have been deleted but still exists",
                task_id
            );
        }

        cleanup_table(&stable_name)
            .await
            .context("cleanup mqtt_meters after test_mqtt_task_with_tls_cert")?;

        Ok(())
    }

    /// Import the MQTT legacy JSON fixture via the Explorer import endpoint,
    /// verify the imported task is visible via the taosx API, then clean up.
    ///
    /// This test does not require a live MQTT broker; it only exercises the
    /// import configuration persistence path.
    ///
    /// Flow:
    /// 1. Load `mqtt-legacy.json` and mutate the task name and client_id to unique values.
    /// 2. POST the payload to `POST /api/x/tasks/import` via the Explorer client.
    /// 3. List tasks via the taosx API and assert the unique-named task is present.
    /// 4. Delete the task and confirm it is gone.
    #[integration_test(tokio::test)]
    async fn test_mqtt_legacy_json_import_creates_task() -> anyhow::Result<()> {
        tracing::info!("{test_name}");
        let explorer_client = build_explorer_client_from_env()?;

        let suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let unique_name = format!("{test_name}_{suffix}");
        let unique_client_id = format!("integration_test_client_{suffix}");

        let fixture_path = crate::common::fixtures::import_export_fixture_path("mqtt-legacy.json");
        let fixture_bytes = tokio::fs::read(&fixture_path)
            .await
            .context("read mqtt-legacy.json fixture")?;
        let mut payload: serde_json::Value =
            serde_json::from_slice(&fixture_bytes).context("parse mqtt-legacy.json")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;
        payload["tasks"][0]["name"] = serde_json::Value::String(unique_name.clone());
        payload["tasks"][0]["from"]["data"]["client_id"] =
            serde_json::Value::String(unique_client_id.clone());
        rewrite_task_target_dsn(&mut payload, &container_target_dsn)
            .context("rewrite mqtt legacy fixture target dsn")?;
        explorer_client
            .import_tasks(&payload)
            .await
            .context("import mqtt legacy json via explorer")?;

        let tasks = explorer_client
            .list_tasks()
            .await
            .context("list explorer tasks after mqtt legacy json import")?;
        let imported = tasks
            .iter()
            .find(|t| t.name == unique_name)
            .with_context(|| {
                format!("imported mqtt task '{unique_name}' not found in task list")
            })?;
        let task_id = imported.id;

        explorer_client
            .delete_task(task_id)
            .await
            .with_context(|| format!("delete imported mqtt task {task_id}"))?;

        let after = explorer_client
            .list_tasks()
            .await
            .context("list explorer tasks after delete")?;
        anyhow::ensure!(
            after.iter().all(|task| task.id != task_id),
            "mqtt task {task_id} still exists after deletion"
        );

        Ok(())
    }

    /// Import the MQTT legacy JSON fixture, export the created task, and assert
    /// that the export response is JSON (not ZIP) containing a valid MQTT task.
    ///
    /// Assertions:
    /// - `Content-Type` header contains `application/json`.
    /// - Exported body is valid JSON.
    /// - `tasks[0].from.type` equals `"mqtt"`.
    ///
    /// Flow:
    /// 1. Import `mqtt-legacy.json` with a unique task name and client_id.
    /// 2. Locate the task via the taosx API.
    /// 3. Export the task via `GET /api/x/tasks/export?ids=<id>`.
    /// 4. Assert content-type and payload shape.
    /// 5. Delete the task.
    #[integration_test(tokio::test)]
    async fn test_mqtt_import_export_is_json() -> anyhow::Result<()> {
        tracing::info!("{test_name}");
        let explorer_client = build_explorer_client_from_env()?;

        let suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let unique_name = format!("{test_name}_{suffix}");
        let unique_client_id = format!("integration_test_client_{suffix}");

        let fixture_path = crate::common::fixtures::import_export_fixture_path("mqtt-legacy.json");
        let fixture_bytes = tokio::fs::read(&fixture_path)
            .await
            .context("read mqtt-legacy.json fixture for export test")?;
        let mut payload: serde_json::Value =
            serde_json::from_slice(&fixture_bytes).context("parse mqtt-legacy.json for export")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;
        payload["tasks"][0]["name"] = serde_json::Value::String(unique_name.clone());
        payload["tasks"][0]["from"]["data"]["client_id"] =
            serde_json::Value::String(unique_client_id.clone());
        rewrite_task_target_dsn(&mut payload, &container_target_dsn)
            .context("rewrite mqtt export fixture target dsn")?;
        explorer_client
            .import_tasks(&payload)
            .await
            .context("import mqtt legacy json for export test")?;

        let tasks = explorer_client
            .list_tasks()
            .await
            .context("list explorer tasks after import for export test")?;
        let imported = tasks
            .iter()
            .find(|t| t.name == unique_name)
            .with_context(|| format!("imported task '{unique_name}' not found for export test"))?;
        let task_id = imported.id;

        let export_resp = explorer_client
            .export_tasks(&[task_id as i64])
            .await
            .with_context(|| format!("export mqtt task {task_id}"))?;

        anyhow::ensure!(
            export_resp.content_type.contains("application/json"),
            "expected application/json content-type for mqtt export, got: '{}'",
            export_resp.content_type
        );

        let exported: serde_json::Value =
            serde_json::from_slice(&export_resp.bytes).context("parse exported mqtt task JSON")?;

        let from_type = exported["tasks"][0]["from"]["type"]
            .as_str()
            .context("exported tasks[0].from.type is missing or not a string")?;
        anyhow::ensure!(
            from_type == "mqtt",
            "expected tasks[0].from.type == 'mqtt', got: '{from_type}'"
        );

        explorer_client
            .delete_task(task_id)
            .await
            .with_context(|| format!("delete exported mqtt task {task_id}"))?;

        Ok(())
    }

    /// Import the MQTT TLS ZIP fixture, start the task, export it, and verify
    /// the exported ZIP contains both tasks.json and the bundled TLS certificate files.
    ///
    /// This test demonstrates the complete lifecycle for a task with bundled files:
    /// import → start → export → verify bundled files preserved.
    ///
    /// Flow:
    /// 1. Load `mqtt-with-tls.zip` fixture and extract tasks.json + bundled TLS files.
    /// 2. Upload the bundled TLS files via `POST /api/x/files/upload`.
    /// 3. Rewrite the task config to use a unique MQTT client_id and the uploaded file paths.
    /// 4. Import the task via `POST /api/x/tasks/import`.
    /// 5. Start the task and wait until it reaches running state.
    /// 6. Export the task via `GET /api/x/tasks/export?ids=<id>`.
    /// 7. Assert the export is ZIP format with expected Content-Type.
    /// 8. Assert the exported ZIP contains tasks.json and all three TLS cert files under files/tls/.
    /// 9. Verify the exported tasks.json has the TLS file field references.
    /// 10. Delete the task and verify it is gone.
    ///
    /// Note: The fixture uses port 1883 (non-TLS MQTT) to avoid requiring a TLS broker,
    /// but includes bundled TLS certificate files to verify the import/export preserves them.
    #[integration_test(tokio::test)]
    async fn test_mqtt_tls_zip_roundtrip() -> anyhow::Result<()> {
        use std::io::Read;

        use crate::common::fixtures::{
            extract_tasks_json_from_zip, import_export_fixture_path, list_zip_entries,
        };

        tracing::info!("{test_name}");
        let explorer_client = build_explorer_client_from_env()?;

        let suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let unique_name = format!("{test_name}_{suffix}");
        let unique_client_id = format!("integration_test_client_{suffix}");

        let fixture_path = import_export_fixture_path("mqtt-with-tls.zip");
        let zip_bytes = tokio::fs::read(&fixture_path)
            .await
            .with_context(|| format!("read ZIP fixture {}", fixture_path.display()))?;

        let mut tasks_json_bytes = None;
        let mut bundled = Vec::new();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&zip_bytes))
            .context("open fixture ZIP archive")?;
        for index in 0..archive.len() {
            let mut entry = archive.by_index(index).context("read ZIP entry")?;
            let name = entry.name().to_string();
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).context("read ZIP entry data")?;
            if name == "tasks.json" {
                tasks_json_bytes = Some(buf);
            } else if name.starts_with("files/") && !name.ends_with('/') {
                bundled.push((name, buf));
            }
        }

        let raw_tasks = tasks_json_bytes.context("tasks.json not found in fixture ZIP")?;
        let mut payload: serde_json::Value =
            serde_json::from_slice(&raw_tasks).context("parse fixture tasks.json")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;
        payload["tasks"][0]["name"] = serde_json::Value::String(unique_name.clone());
        payload["tasks"][0]["from"]["data"]["client_id"] =
            serde_json::Value::String(unique_client_id.clone());
        rewrite_task_target_dsn(&mut payload, &container_target_dsn)
            .context("rewrite mqtt TLS ZIP target dsn")?;

        anyhow::ensure!(
            !bundled.is_empty(),
            "fixture ZIP must contain bundled TLS files under files/"
        );

        let expected_tls_files = [
            "files/tls/ca.pem",
            "files/tls/client.pem",
            "files/tls/client-key.pem",
        ];
        for expected in &expected_tls_files {
            anyhow::ensure!(
                bundled.iter().any(|(name, _)| name == expected),
                "fixture ZIP missing {expected}"
            );
        }

        let bundled_paths: Vec<String> = bundled.iter().map(|(path, _)| path.clone()).collect();
        let upload_parts: Vec<(String, Vec<u8>)> = bundled
            .into_iter()
            .map(|(path, data)| {
                let filename = std::path::Path::new(&path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(&path)
                    .to_string();
                (filename, data)
            })
            .collect();

        let uploaded_paths = explorer_client
            .upload_files(upload_parts)
            .await
            .context("upload bundled TLS files")?;
        tracing::info!("uploaded TLS files: {uploaded_paths:?}");

        let replacements: Vec<(&str, &str)> = bundled_paths
            .iter()
            .map(String::as_str)
            .zip(uploaded_paths.iter().map(String::as_str))
            .collect();
        rewrite_json_file_refs(&mut payload, &replacements);

        explorer_client
            .import_tasks(&payload)
            .await
            .context("import MQTT TLS task")?;

        let tasks = explorer_client
            .list_tasks()
            .await
            .context("list explorer tasks after TLS import")?;
        let imported = tasks
            .iter()
            .find(|t| t.name == unique_name)
            .with_context(|| format!("imported TLS task '{unique_name}' not found"))?;
        let task_id = imported.id;
        tracing::info!("imported TLS task id={task_id}");

        explorer_client
            .start_task(task_id)
            .await
            .with_context(|| format!("start TLS task {task_id}"))?;
        tracing::info!("started TLS task");

        explorer_client
            .wait_until_running(task_id)
            .await
            .context("wait for TLS task running")?;
        tracing::info!("TLS task is running");

        let export_resp = explorer_client
            .export_tasks(&[task_id as i64])
            .await
            .with_context(|| format!("export TLS task {task_id}"))?;

        anyhow::ensure!(
            export_resp.content_type.contains("application/zip"),
            "expected application/zip content-type for TLS export, got: '{}'",
            export_resp.content_type
        );

        let exported_entries =
            list_zip_entries(&export_resp.bytes).context("list exported ZIP entries")?;
        tracing::info!("exported ZIP entries: {exported_entries:?}");

        anyhow::ensure!(
            exported_entries.contains(&"tasks.json".to_string()),
            "exported ZIP must contain tasks.json; entries: {exported_entries:?}"
        );

        let has_ca = exported_entries.iter().any(|e| e.ends_with("ca.pem"));
        let has_client = exported_entries.iter().any(|e| e.ends_with("client.pem"));
        let has_client_key = exported_entries
            .iter()
            .any(|e| e.ends_with("client-key.pem"));

        anyhow::ensure!(
            has_ca,
            "exported ZIP missing ca.pem; entries: {exported_entries:?}"
        );
        anyhow::ensure!(
            has_client,
            "exported ZIP missing client.pem; entries: {exported_entries:?}"
        );
        anyhow::ensure!(
            has_client_key,
            "exported ZIP missing client-key.pem; entries: {exported_entries:?}"
        );

        let exported_tasks_json = extract_tasks_json_from_zip(&export_resp.bytes)
            .context("extract tasks.json from exported ZIP")?;
        let exported_task = &exported_tasks_json["tasks"][0];
        let exported_from_data = &exported_task["from"]["data"];
        anyhow::ensure!(
            exported_from_data.get("tls_ca_file").is_some(),
            "exported task must have tls_ca_file field"
        );
        anyhow::ensure!(
            exported_from_data.get("tls_cert_file").is_some(),
            "exported task must have tls_cert_file field"
        );
        anyhow::ensure!(
            exported_from_data.get("tls_key_file").is_some(),
            "exported task must have tls_key_file field"
        );
        anyhow::ensure!(
            exported_from_data["client_id"].as_str() == Some(unique_client_id.as_str()),
            "expected exported client_id to be '{unique_client_id}', got: {:?}",
            exported_from_data["client_id"]
        );

        explorer_client
            .delete_task(task_id)
            .await
            .with_context(|| format!("delete TLS task {task_id}"))?;

        let after = explorer_client
            .list_tasks()
            .await
            .context("list explorer tasks after delete")?;
        anyhow::ensure!(
            after.iter().all(|task| task.id != task_id),
            "TLS task {task_id} still exists after deletion"
        );

        Ok(())
    }
}
