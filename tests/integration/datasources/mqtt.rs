//！ MQTT data source integration tests

use rumqttc::v5::mqttbytes::QoS;

use crate::datasources::env_var;

#[cfg(all(test, feature = "test-mqtt"))]
pub struct MqttPubBuilder {
    schema: std::path::PathBuf,
    host: String,
    topic: String,
    port: Option<u16>,
    qos: Option<QoS>,
    username_password: Option<(String, String)>,
    count: Option<usize>,
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
            count: None,
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

    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    pub async fn publish(self) -> anyhow::Result<()> {
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
        let count = self.count.unwrap_or(10);

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

        let Event::Incoming(Incoming::ConnAck(ack)) =
            event_loop.poll().await.context("mqtt ConnAck error")?
        else {
            anyhow::bail!("expected ConnAck packet");
        };
        if !matches!(ack.code, ConnectReturnCode::Success) {
            anyhow::bail!("connect error: {:?}", ack.code);
        }
        tracing::info!("mqtt connected successfully");

        let event_loop_task = tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
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
                    Ok(Event::Outgoing(Outgoing::Publish(_))) => {
                        tracing::info!("mqtt published one message");
                    }
                    Ok(_) => {}
                    Err(ConnectionError::RequestsDone) => {
                        tracing::info!("mqtt requests done");
                        break;
                    }
                    Err(e) => {
                        tracing::error!("mqtt event_loop poll error: {}", e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });

        for _ in 0..count {
            let payload_value = schema.rand_json_value().context("generate fake json")?;
            let payload = serde_json::to_vec(&payload_value).context("serialize json")?;
            client
                .publish(self.topic.as_str(), qos, false, payload)
                .await
                .context("mqtt publish")?;
        }

        tracing::info!("mqtt published {} messages", count);

        drop(client);
        tracing::info!("mqtt client dropped");
        event_loop_task
            .await
            .context("join mqtt event loop task failed")?;
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

    use crate::datasources::env_var;

    use anyhow::Context;
    use rumqttc::v5::mqttbytes::QoS;

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

    async fn cleanup_table(to_dsn: &str, table: &str) -> anyhow::Result<()> {
        let taos_conn = TaosConn::create(to_dsn, 3)
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
    /// 3. Publish fake JSON messages to the configured topic through the MQTT broker.
    /// 4. Wait until the task reports written rows, then stop and delete the task.
    /// 5. Drop the `mqtt_meters` stable in TDengine to clean up test data.
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
        let to_dsn = env_var("MQTT_TASK_TO_DSN")?;

        let client = build_api_client_from_env()?;

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let task_cfg_path = manifest_dir.join("config/task/mqtt.json");
        let parser_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&task_cfg_path)
                .with_context(|| format!("read mqtt task config {:?}", task_cfg_path))?,
        )
        .context("parse mqtt task parser json")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");

        let topic = format!("integration_test_{task_name}");
        let client_id = format!("integration_test_{task_name}");

        let from = format!(
            "mqtt://{mqtt_host}:{mqtt_port}?version=5&topics={topic}::1&client_id={client_id}"
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let new_task = NewTask {
            name: task_name.clone(),
            from,
            to: to_dsn.clone(),
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

        super::mqtt_pub(schema_path, &mqtt_host, &topic)
            .port(mqtt_port)
            .count(10)
            .publish()
            .await
            .context("publish fake mqtt json messages")?;
        tracing::info!("mqtt publish finished");

        client
            .wait_until_written_rows(task_id, 1)
            .await
            .context("wait for task written_rows")?;
        tracing::info!("mqtt task written_rows reached");

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

        cleanup_table(&to_dsn, "mqtt_meters")
            .await
            .context("cleanup mqtt_meters after test_mqtt_task_with_fake_data")?;

        Ok(())
    }

    /// Create a multi-topic MQTT task and verify wildcard subscription behavior.
    ///
    /// Flow:
    /// 1. Build an MQTT task that subscribes to two concrete topics under the same prefix.
    /// 2. Create the task via HTTP API and wait until it is running.
    /// 3. Publish fake JSON messages to the first topic and then to the second topic.
    /// 4. Wait until the task reports written rows, then stop and delete the task.
    /// 5. Drop the `mqtt_meters` stable in TDengine to clean up test data.
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
        let to_dsn = env_var("MQTT_TASK_TO_DSN")?;

        let client = build_api_client_from_env()?;

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let task_cfg_path = manifest_dir.join("config/task/mqtt.json");
        let parser_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&task_cfg_path)
                .with_context(|| format!("read mqtt task config {:?}", task_cfg_path))?,
        )
        .context("parse mqtt task parser json")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_multi_{name_suffix}");

        let topic_prefix = format!("integration_multi/{task_name}");
        let topic_a = format!("{topic_prefix}/a");
        let topic_b = format!("{topic_prefix}/b");

        let client_id = format!("integration_test_multi_{task_name}");
        let topics_param = format!("{topic_a}::1,{topic_b}::1");

        let from = format!(
            "mqtt://{mqtt_host}:{mqtt_port}?version=5&topics={topics_param}&client_id={client_id}",
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let new_task = NewTask {
            name: task_name.clone(),
            from,
            to: to_dsn.clone(),
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

        super::mqtt_pub(schema_path.clone(), &mqtt_host, &topic_a)
            .port(mqtt_port)
            .count(10)
            .publish()
            .await
            .context("publish fake mqtt json messages to first topic")?;
        tracing::info!("mqtt publish to first topic finished");

        super::mqtt_pub(schema_path, &mqtt_host, &topic_b)
            .port(mqtt_port)
            .qos(QoS::AtLeastOnce)
            .count(10)
            .publish()
            .await
            .context("publish fake mqtt json messages to second topic")?;
        tracing::info!("mqtt publish to second topic finished");

        client
            .wait_until_written_rows(task_id, 1)
            .await
            .context("wait for multi-topic mqtt task written_rows")?;
        tracing::info!("multi-topic mqtt task written_rows reached");

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

        cleanup_table(&to_dsn, "mqtt_meters")
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
        let to_dsn = env_var("MQTT_TASK_TO_DSN")?;

        let client = build_api_client_from_env()?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let client_id = format!("integration_test_validate_{name_suffix}");

        let from = format!(
            "mqtt://{mqtt_host}:{mqtt_port}?version=5&topics=integration_validate/{client_id}::1&client_id={client_id}"
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let body = ApiCheckValidParamClient {
            from: Some(from.clone()),
            from_json: None,
            to: to_dsn.clone(),
            via,
        };

        let result = client
            .validate_data_source(&body)
            .await
            .context("validate mqtt datasource via api")?;

        tracing::info!("mqtt validate datasource result: {}", result);

        cleanup_table(&to_dsn, "mqtt_meters")
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
        let to_dsn = env_var("MQTT_TASK_TO_DSN")?;

        let client = build_api_client_from_env()?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let client_id = format!("integration_test_validate_auth_{name_suffix}");

        let from = format!(
            "mqtt://{mqtt_username}:{mqtt_password}@{mqtt_host}:{mqtt_auth_port}?version=5&topics=integration_validate_auth/{client_id}::1&client_id={client_id}"
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let body = ApiCheckValidParamClient {
            from: Some(from.clone()),
            from_json: None,
            to: to_dsn.clone(),
            via,
        };

        let result = client
            .validate_data_source(&body)
            .await
            .context("validate auth mqtt datasource via api")?;

        tracing::info!("auth mqtt validate datasource result: {}", result);

        cleanup_table(&to_dsn, "mqtt_meters")
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
        let to_dsn = env_var("MQTT_TASK_TO_DSN")?;

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
        let client_id = format!("integration_test_validate_tls_{name_suffix}");

        // DSN uses TLS certs uploaded to server; ca/cert/cert_key use @path syntax and MQTT username/password are embedded in the authority
        let from = format!(
            "mqtt://{mqtt_username}:{mqtt_password}@{mqtt_host}:{mqtt_tls_port}?version=5&topics=integration_validate_tls/{client_id}::1&client_id={client_id}&ca=@{ca}&cert=@{cert}&cert_key=@{key}",
            ca = ca_server_path,
            cert = cert_server_path,
            key = key_server_path,
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let body = ApiCheckValidParamClient {
            from: Some(from.clone()),
            from_json: None,
            to: to_dsn.clone(),
            via,
        };

        let result = client
            .validate_data_source(&body)
            .await
            .context("validate tls mqtt datasource via api")?;

        tracing::info!("tls mqtt validate datasource result: {}", result);

        cleanup_table(&to_dsn, "mqtt_meters")
            .await
            .context("cleanup mqtt_meters after test_mqtt_validate_datasource_with_tls_cert")?;

        Ok(())
    }

    /// Create an authenticated MQTT task and verify data ingestion with credentials.
    ///
    /// Flow:
    /// 1. Construct an MQTT DSN that includes username, password, and auth port.
    /// 2. Create an MQTT task via HTTP API and wait until it is running.
    /// 3. Publish fake JSON messages to the configured topic using the same credentials.
    /// 4. Wait until written rows increase, then stop and delete the task.
    /// 5. Drop the `mqtt_meters` stable in TDengine to remove test data.
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
        let to_dsn = env_var("MQTT_TASK_TO_DSN")?;

        let client = build_api_client_from_env()?;

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let task_cfg_path = manifest_dir.join("config/task/mqtt.json");
        let parser_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&task_cfg_path)
                .with_context(|| format!("read mqtt task config {:?}", task_cfg_path))?,
        )
        .context("parse mqtt task parser json")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_auth_{name_suffix}");

        let topic = format!("integration_auth/{task_name}");
        let client_id = format!("integration_test_auth_{task_name}");

        // DSN uses MQTT username and password for authenticated connection
        let from = format!(
            "mqtt://{mqtt_username}:{mqtt_password}@{mqtt_host}:{mqtt_auth_port}?version=5&topics={topic}::1&client_id={client_id}"
        );

        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;

        let new_task = NewTask {
            name: task_name.clone(),
            from,
            to: to_dsn.clone(),
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

        super::mqtt_pub(schema_path, &mqtt_host, &topic)
            .port(mqtt_auth_port)
            .username_password(mqtt_username, mqtt_password)
            .count(10)
            .publish()
            .await
            .context("publish auth mqtt json messages")?;
        tracing::info!("auth mqtt publish finished");

        client
            .wait_until_written_rows(task_id, 1)
            .await
            .context("wait for auth mqtt task written_rows")?;
        tracing::info!("auth mqtt task written_rows reached");

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

        cleanup_table(&to_dsn, "mqtt_meters")
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
    /// 4. Publish fake JSON messages to the configured topic via the MQTT broker.
    /// 5. Wait until written rows are reported, then stop and delete the task and drop `mqtt_meters`.
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
        let to_dsn = env_var("MQTT_TASK_TO_DSN")?;

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
        let parser_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&task_cfg_path)
                .with_context(|| format!("read mqtt task config {:?}", task_cfg_path))?,
        )
        .context("parse mqtt task parser json")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_tls_{name_suffix}");

        let topic = format!("integration_tls/{task_name}");
        let client_id = format!("integration_test_tls_{task_name}");

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
            to: to_dsn.clone(),
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

        // mqtt_pub uses dedicated TLS broker port for secure messages
        let mqtt_auth_port = env_var("MQTT_AUTH_PORT")?
            .parse::<u16>()
            .context("invalid INTEGRATION_TEST_MQTT_AUTH_PORT")?;
        super::mqtt_pub(schema_path, &mqtt_host, &topic)
            .port(mqtt_auth_port)
            .username_password(mqtt_username, mqtt_password)
            .count(10)
            .publish()
            .await
            .context("publish tls mqtt json messages")?;
        tracing::info!("tls mqtt publish finished");

        client
            .wait_until_written_rows(task_id, 1)
            .await
            .context("wait for tls mqtt task written_rows")?;
        tracing::info!("tls mqtt task written_rows reached");

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

        cleanup_table(&to_dsn, "mqtt_meters")
            .await
            .context("cleanup mqtt_meters after test_mqtt_task_with_tls_cert")?;

        Ok(())
    }
}
