//! Kafka data source integration tests
//!
//! Tests Kafka source connector functionality including:
//! - Multi-datastructure message routing to separate TDengine supertables via rules-based parser
//! - Data transformation and ingestion verification

/// Schema-based Kafka message publisher for integration tests.
///
/// Loads one or more fake-data schemas and rotates through them when publishing,
/// enabling multi-datastructure message streams to a single topic.
#[cfg(test)]
pub struct KafkaPubBuilder {
    schemas: Vec<std::path::PathBuf>,
    broker: String,
    topic: String,
}

#[cfg(test)]
impl KafkaPubBuilder {
    pub fn new(
        schemas: Vec<std::path::PathBuf>,
        broker: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            schemas,
            broker: broker.into(),
            topic: topic.into(),
        }
    }

    /// Publishes fake JSON messages generated from configured schemas until `cancel` is
    /// triggered. Schemas are rotated round-robin so each message type is evenly represented.
    pub async fn publish(self, cancel: tokio_util::sync::CancellationToken) -> anyhow::Result<()> {
        use std::time::Duration;

        use anyhow::Context;
        use fake_data::json::DataFakeSchema;
        use rdkafka::producer::{FutureProducer, FutureRecord};
        use rdkafka::ClientConfig;

        if self.schemas.is_empty() {
            anyhow::bail!("KafkaPubBuilder requires at least one schema");
        }

        let schemas: Vec<DataFakeSchema> = self
            .schemas
            .iter()
            .map(|p| {
                DataFakeSchema::from_file(p)
                    .map_err(|e| anyhow::anyhow!("load schema {:?}: {}", p, e))
            })
            .collect::<anyhow::Result<_>>()?;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .set("message.timeout.ms", "5000")
            .create()
            .context("create kafka producer")?;

        let mut sent = 0_u64;
        while !cancel.is_cancelled() {
            let schema = &schemas[(sent as usize) % schemas.len()];
            let payload_value = schema.rand_json_value().context("generate fake json")?;
            let payload =
                serde_json::to_string(&payload_value).context("serialize kafka payload")?;
            let key = format!("key-{sent}");
            let record = FutureRecord::to(&self.topic).payload(&payload).key(&key);
            let Some(delivery) = cancel
                .run_until_cancelled(producer.send(record, Duration::from_secs(5)))
                .await
            else {
                break;
            };
            delivery.map_err(|(error, _)| anyhow::anyhow!("deliver kafka payload: {error}"))?;
            sent += 1;
            if sent.is_multiple_of(10)
                && cancel
                    .run_until_cancelled(tokio::time::sleep(Duration::from_millis(100)))
                    .await
                    .is_none()
            {
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub fn kafka_pub(
    schemas: Vec<std::path::PathBuf>,
    broker: impl Into<String>,
    topic: impl Into<String>,
) -> KafkaPubBuilder {
    KafkaPubBuilder::new(schemas, broker, topic)
}

#[cfg(test)]
async fn run_kafka_blocking<T, F>(operation: F) -> anyhow::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
    use anyhow::Context;

    tokio::task::spawn_blocking(operation)
        .await
        .context("join kafka blocking task")?
}

#[cfg(test)]
mod tests {
    use crate::common;
    use anyhow::Context;
    use taosx_test_macros::integration_test;
    use tokio_util::sync::CancellationToken;

    use crate::datasources::env_var;

    fn topic_metadata_is_ready(
        topic_error: Option<rdkafka::types::RDKafkaErrorCode>,
        partitions: &[Option<rdkafka::types::RDKafkaErrorCode>],
    ) -> bool {
        topic_error.is_none() && !partitions.is_empty() && partitions.iter().all(Option::is_none)
    }

    fn ensure_topic_creation_can_continue(
        result: Result<String, (String, rdkafka::types::RDKafkaErrorCode)>,
    ) -> anyhow::Result<()> {
        match result {
            Ok(_) => Ok(()),
            Err((_, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => Ok(()),
            Err((_, rdkafka::types::RDKafkaErrorCode::OperationTimedOut)) => Ok(()),
            Err((name, code)) => anyhow::bail!("failed to create kafka topic {name}: {code}"),
        }
    }

    #[integration_test(tokio::test)]
    async fn test_kafka_broker_connection() -> anyhow::Result<()> {
        // Setup logger if not already initialized
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();

        let broker = env_var("KAFKA_BROKER")?;

        println!("Testing Kafka broker connection: {broker}");

        common::health_check::check_kafka_health(&broker).await
    }

    #[test]
    fn test_kafka_dsn_construction() {
        let base_dsn = "kafka://localhost:9092/test_topic";
        let dsn_with_params = common::helpers::build_dsn_with_params(
            base_dsn,
            &[("group", "test_group"), ("auto_offset_reset", "earliest")],
        );

        assert!(dsn_with_params.contains("group=test_group"));
        assert!(dsn_with_params.contains("auto_offset_reset=earliest"));
        println!("✓ Kafka DSN: {}", dsn_with_params);
    }

    #[test]
    fn test_kafka_sample_data_generation() {
        let data = common::fixtures::SampleData::generate(100);
        assert_eq!(data.records.len(), 100);

        for (i, record) in data.records.iter().enumerate() {
            assert_eq!(record.id, format!("record_{}", i));
            assert!(record.value > 0.0);
            assert!(!record.tags.is_empty());
        }

        println!("✓ Generated {} test records", data.records.len());
    }

    #[test]
    fn test_kafka_test_context() {
        let ctx = common::fixtures::TestContext::new();
        let dsn = ctx.dsn();

        assert!(dsn.contains("taos://"));
        assert!(dsn.contains(&ctx.db_name));
        println!("✓ Test context DSN: {}", dsn);
    }

    #[tokio::test]
    async fn test_kafka_with_timeout() {
        let result = common::helpers::wait_for(|| async { true }, 5, 100).await;

        assert!(result.is_ok());
        println!("✓ Timeout check passed");
    }

    #[tokio::test]
    async fn test_kafka_pub_rejects_empty_schemas() {
        let cancel = CancellationToken::new();
        let result = super::kafka_pub(Vec::new(), "localhost:9092", "test_topic")
            .publish(cancel)
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one schema"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_run_kafka_blocking_yields_to_other_tasks() {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let blocking = super::run_kafka_blocking(|| {
            std::thread::sleep(Duration::from_millis(100));
            Ok::<_, anyhow::Error>(7_u8)
        });
        tokio::pin!(blocking);

        tokio::select! {
            result = &mut blocking => panic!("blocking helper completed too early: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        assert!(
            start.elapsed() < Duration::from_millis(80),
            "runtime stayed blocked for {:?}",
            start.elapsed()
        );
        assert_eq!(blocking.await.unwrap(), 7);
    }

    /// Creates a Kafka topic with the given name and waits until it is visible in metadata.
    ///
    /// The Kafka topic must exist before a taosx task is started; otherwise `split_job`
    /// fetches empty metadata and the task stays in "Created" state with "Topic empty" error.
    async fn create_kafka_topic(broker: &str, topic: &str) -> anyhow::Result<()> {
        use std::time::Duration;

        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::client::DefaultClientContext;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::ClientConfig;

        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .create()
            .context("create kafka admin client")?;
        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        // We only need to trigger topic creation here; readiness is verified by metadata polling
        // below. Waiting for broker-side operation completion is redundant and can surface
        // transient OperationTimedOut results before metadata has a chance to converge.
        let opts = AdminOptions::new();
        let results = admin
            .create_topics(&[new_topic], &opts)
            .await
            .context("create kafka topic")?;
        for result in results {
            if matches!(
                &result,
                Err((_, rdkafka::types::RDKafkaErrorCode::OperationTimedOut))
            ) {
                tracing::warn!(
                    "create_topics for kafka topic '{topic}' timed out; continuing with metadata polling"
                );
            }
            ensure_topic_creation_can_continue(result)?;
        }

        const MAX_METADATA_ATTEMPTS: u32 = 20;
        const FETCH_TIMEOUT: Duration = Duration::from_secs(1);
        const RETRY_DELAY: Duration = Duration::from_millis(250);
        for attempt in 1..=MAX_METADATA_ATTEMPTS {
            let metadata = super::run_kafka_blocking({
                let broker = broker.to_string();
                let topic = topic.to_string();
                move || {
                    let metadata_consumer: BaseConsumer = ClientConfig::new()
                        .set("bootstrap.servers", &broker)
                        .set("group.id", format!("metadata_probe_{topic}"))
                        .create()
                        .context("create kafka metadata consumer")?;
                    metadata_consumer
                        .fetch_metadata(Some(&topic), FETCH_TIMEOUT)
                        .with_context(|| format!("fetch metadata for kafka topic {topic}"))
                }
            })
            .await?;
            if let Some(topic_meta) = metadata.topics().iter().find(|meta| meta.name() == topic) {
                let partition_errors: Vec<_> = topic_meta
                    .partitions()
                    .iter()
                    .map(|partition| partition.error().map(Into::into))
                    .collect();
                if topic_metadata_is_ready(topic_meta.error().map(Into::into), &partition_errors) {
                    tracing::info!(
                        "kafka topic '{topic}' became visible in metadata after {attempt} attempt(s)"
                    );
                    return Ok(());
                }
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }

        anyhow::bail!(
            "kafka topic '{topic}' was created on broker {broker} but did not become visible in metadata"
        )
    }

    #[test]
    fn test_topic_metadata_is_ready_requires_visible_partitions() {
        use rdkafka::types::RDKafkaErrorCode;

        assert!(topic_metadata_is_ready(None, &[None]));
        assert!(!topic_metadata_is_ready(
            Some(RDKafkaErrorCode::LeaderNotAvailable),
            &[None]
        ));
        assert!(!topic_metadata_is_ready(None, &[]));
        assert!(!topic_metadata_is_ready(
            None,
            &[Some(RDKafkaErrorCode::UnknownTopicOrPartition)]
        ));
    }

    #[test]
    fn test_topic_creation_continues_after_transient_admin_results() {
        use rdkafka::types::RDKafkaErrorCode;

        assert!(ensure_topic_creation_can_continue(Ok("topic".to_string())).is_ok());
        assert!(ensure_topic_creation_can_continue(Err((
            "topic".to_string(),
            RDKafkaErrorCode::TopicAlreadyExists
        )))
        .is_ok());
        assert!(ensure_topic_creation_can_continue(Err((
            "topic".to_string(),
            RDKafkaErrorCode::OperationTimedOut
        )))
        .is_ok());
        assert!(ensure_topic_creation_can_continue(Err((
            "topic".to_string(),
            RDKafkaErrorCode::InvalidTopic
        )))
        .is_err());
    }

    #[test]
    fn test_legacy_kafka_parser_shape_has_no_rules() {
        let parser_json = legacy_kafka_parser_json("meters", "task_123");

        assert_eq!(
            parser_json["parse"]["value"]["json"],
            serde_json::json!(["kind", "device", "value::double", "ts::timestamp(ms)"])
        );
        assert_eq!(parser_json["model"]["using"], "meters");
        assert_eq!(parser_json["model"]["tags"], serde_json::json!(["device"]));
        assert_eq!(
            parser_json["model"]["columns"],
            serde_json::json!(["ts", "value"])
        );
        assert_eq!(parser_json["model"]["name"], "meters_task_123_{device}");
        assert!(parser_json.get("rules").is_none());
    }

    fn legacy_kafka_parser_json(stable_name: &str, task_name: &str) -> serde_json::Value {
        serde_json::json!({
            "parse": {
                "value": {
                    "json": ["kind", "device", "value::double", "ts::timestamp(ms)"]
                }
            },
            "model": {
                "name": format!("{stable_name}_{task_name}_{{device}}"),
                "using": stable_name,
                "tags": ["device"],
                "columns": ["ts", "value"]
            }
        })
    }

    fn build_api_client_from_env() -> anyhow::Result<crate::core::api::ApiClient> {
        let api_base = env_var("TAOSX_API_BASE_URL")?;
        let api_username = env_var("TAOSX_API_USERNAME").unwrap_or_else(|_| "root".to_string());
        let api_password = env_var("TAOSX_API_PASSWORD").unwrap_or_else(|_| "taosdata".to_string());
        crate::core::api::ApiClient::builder(&api_base)
            .with_auth(&api_username, &api_password)
            .build()
    }

    async fn cleanup_table(to_dsn: &str, table: &str) -> anyhow::Result<()> {
        let taos_conn = taosx_utils::taos_conn::TaosConn::create(to_dsn, 3)
            .await
            .with_context(|| format!("create taos conn for cleanup of table {table}"))?;
        taos_conn
            .exec(&format!("DROP STABLE IF EXISTS `{table}`"))
            .await
            .with_context(|| format!("drop stable {table}"))?;
        Ok(())
    }

    /// Wait until `table` contains at least `min_rows` rows, retrying up to `max_attempts`
    /// times with a 1-second delay. This guards against the race where `written_rows` is
    /// incremented before TDengine has committed the stable/rows.
    ///
    /// 120 attempts (2 minutes) allows time for taosx's table-not-exist auto-create cycle:
    /// first INSERT fails → CREATE TABLE issued → subsequent INSERT succeed.
    async fn wait_for_rows(to_dsn: &str, table: &str, min_rows: i64) -> anyhow::Result<()> {
        const MAX_ATTEMPTS: u32 = 120;

        let taos_conn = taosx_utils::taos_conn::TaosConn::create(to_dsn, 3)
            .await
            .with_context(|| format!("create taos conn to wait for rows in {table}"))?;

        for attempt in 1..=MAX_ATTEMPTS {
            let result: Result<Option<(i64,)>, _> = taos_conn
                // Backtick-quote the table name so TDengine treats it as a
                // case-sensitive identifier. Without backticks, TDengine lowercases
                // the name before lookup, which fails for mixed-case stable names
                // created with backtick-quoted CREATE STABLE.
                .query_one(&format!("SELECT COUNT(*) FROM `{table}`"))
                .await;
            match result {
                Ok(Some((count,))) if count >= min_rows => {
                    tracing::info!(
                        "table {table} has {count} rows (>= {min_rows}) after {attempt} attempt(s)"
                    );
                    return Ok(());
                }
                Ok(Some((count,))) => {
                    tracing::info!(
                        "table {table} has {count} rows (need {min_rows}), \
                         waiting... ({attempt}/{MAX_ATTEMPTS})"
                    );
                }
                Ok(None) => {
                    tracing::info!(
                        "table {table} query returned None, \
                         waiting... ({attempt}/{MAX_ATTEMPTS})"
                    );
                }
                Err(e) => {
                    tracing::info!(
                        "table {table} not yet visible: {e:#}, \
                         waiting... ({attempt}/{MAX_ATTEMPTS})"
                    );
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        anyhow::bail!("table {table} did not reach {min_rows} rows within {MAX_ATTEMPTS} seconds")
    }

    /// Delete all taosx tasks whose names start with `prefix` via the HTTP API.
    ///
    /// Called at the start of each test to remove stale tasks left behind by previous
    /// failed runs. Stale tasks overload TDengine (slow CREATE TABLE) and consume
    /// Kafka consumer-group slots, causing flaky failures.
    async fn cleanup_tasks_by_prefix(
        client: &crate::core::api::ApiClient,
        prefix: &str,
    ) -> anyhow::Result<()> {
        let tasks = client
            .list_tasks()
            .await
            .context("list tasks for cleanup")?;
        for task in tasks {
            if task.name.starts_with(prefix) {
                tracing::info!(
                    "cleanup: stopping stale task {} (id={})",
                    task.name,
                    task.id
                );
                let _ = client.stop_task(task.id).await;
                let _ = client.delete_task(task.id).await;
            }
        }
        Ok(())
    }

    /// Pre-creates a supertable in TDengine using the test's TaosConn.
    ///
    /// taosx's flat_sink uses the same TaosWS connection to issue CREATE TABLE after a
    /// failed INSERT. In some environments that connection hangs indefinitely after
    /// receiving a TDengine error response. Pre-creating the stable ensures the first
    /// INSERT USING succeeds immediately without needing the flat_sink's CREATE path.
    async fn pre_create_stable(to_dsn: &str, sql: &str) -> anyhow::Result<()> {
        let taos_conn = taosx_utils::taos_conn::TaosConn::create(to_dsn, 3)
            .await
            .context("create taos conn for stable pre-creation")?;
        taos_conn.exec(sql).await.context("pre-create stable")?;
        Ok(())
    }

    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_kafka_multi_rule_transform_config(with_agent: bool) -> anyhow::Result<()> {
        use ha_core::activity::TaskStatus;

        use crate::core::api::{ApiClient, NewTask};

        tracing::info!("{test_name}");
        let broker = env_var("KAFKA_BROKER")?;
        let to_dsn = env_var("KAFKA_TASK_TO_DSN")?;
        let client: ApiClient = build_api_client_from_env()?;

        // Remove stale tasks from previous failed runs to avoid overloading TDengine
        // and Kafka consumer groups.
        cleanup_tasks_by_prefix(&client, test_name)
            .await
            .context("cleanup stale tasks before test")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");
        let stable_name = format!("{task_name}_meters");
        let topic = format!("integration_test_topic_{task_name}");
        let group_id = format!("integration_test_group_{task_name}");

        let parser_json = serde_json::json!({
            "parse": {
                "value": {
                    "json": ["kind", "device", "value::double", "ts::timestamp(ms)"]
                }
            },
            "rules": [
                {
                    "matches": { "expr": "kind == \"temp\"" },
                    "model": {
                        "name": format!("{stable_name}_{task_name}_temp_{{device}}"),
                        "using": stable_name,
                        "tags": ["device"],
                        "columns": ["ts", "value"]
                    }
                },
                {
                    "matches": { "expr": "kind == \"power\"" },
                    "model": {
                        "name": format!("{stable_name}_{task_name}_power_{{device}}"),
                        "using": stable_name,
                        "tags": ["device"],
                        "columns": ["ts", "value"]
                    }
                }
            ]
        });

        let from = format!(
            "kafka://{broker}?topics={topic}&group.id={group_id}&auto.offset.reset=earliest"
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

        // Topic must exist before the task is created so that `split_job` can
        // fetch partition metadata and produce a non-empty topics list.
        create_kafka_topic(&broker, &topic)
            .await
            .context("pre-create kafka topic")?;

        // Pre-create the supertable so taosx's first INSERT USING succeeds without
        // going through the flat_sink's CREATE TABLE path (which hangs in some
        // environments after a failed INSERT).
        pre_create_stable(
            &to_dsn,
            &format!(
                "CREATE STABLE IF NOT EXISTS `{stable_name}` \
                 (`ts` TIMESTAMP, `value` DOUBLE) TAGS (`device` BINARY(32))"
            ),
        )
        .await
        .context("pre-create meters stable")?;

        let created = client
            .create_task(&new_task)
            .await
            .context("create kafka task via api")?;
        let task_id = created.id;

        client
            .wait_until_running(task_id)
            .await
            .context("wait for kafka task running")?;

        // Publish messages and wait for rows. The supertable is pre-created so
        // the first INSERT USING succeeds without taosx needing CREATE TABLE.
        // Cancellation is always issued before joining so the publisher outlives
        // wait_for_rows even if it returns an error.
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let schema_temp = manifest_dir.join("config/schema/kafka-meters-temp.toml");
        let schema_power = manifest_dir.join("config/schema/kafka-meters-power.toml");
        let cancel = CancellationToken::new();
        let cancel_for_pub = cancel.clone();
        let pub_handle = tokio::spawn(async move {
            super::kafka_pub(vec![schema_temp, schema_power], broker, topic)
                .publish(cancel_for_pub)
                .await
        });

        let wait_result = wait_for_rows(&to_dsn, &stable_name, 2).await;
        cancel.cancel();
        let pub_result = pub_handle
            .await
            .context("join kafka publish task")?
            .context("publish kafka json messages");
        wait_result.context("wait for kafka stable to have rows")?;
        pub_result?;

        let taos_conn = taosx_utils::taos_conn::TaosConn::create(&to_dsn, 3)
            .await
            .context("create taos conn for kafka verification")?;
        let table_names: Vec<(String,)> = taos_conn
            .query(&format!("SELECT DISTINCT tbname FROM `{stable_name}`"))
            .await
            .context("query kafka child table names")?;
        let table_names = table_names
            .into_iter()
            .map(|(name,)| name)
            .collect::<std::collections::HashSet<_>>();
        assert!(table_names.iter().any(|name| name.contains("_temp_")));
        assert!(table_names.iter().any(|name| name.contains("_power_")));

        client
            .stop_task(task_id)
            .await
            .with_context(|| format!("stop kafka task id {task_id}"))?;
        client
            .wait_until_status(task_id, TaskStatus::Stopped)
            .await
            .context("wait for kafka task stopped")?;
        client
            .delete_task(task_id)
            .await
            .context("delete kafka task via api")?;

        let get_after = client
            .get_task(task_id)
            .await
            .context("get kafka task via api")?;
        if get_after.is_some() {
            anyhow::bail!("task {task_id} should have been deleted but still exists");
        }

        cleanup_table(&to_dsn, &stable_name)
            .await
            .context("cleanup kafka stable after integration test")?;
        Ok(())
    }

    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_kafka_legacy_transform_config(with_agent: bool) -> anyhow::Result<()> {
        use ha_core::activity::TaskStatus;

        use crate::core::api::{ApiClient, NewTask};

        tracing::info!("{test_name}");
        let broker = env_var("KAFKA_BROKER")?;
        let to_dsn = env_var("KAFKA_TASK_TO_DSN")?;
        let client: ApiClient = build_api_client_from_env()?;

        cleanup_tasks_by_prefix(&client, test_name)
            .await
            .context("cleanup stale tasks before legacy kafka parser test")?;

        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");
        let stable_name = format!("{task_name}_legacy");
        let topic = format!("integration_test_topic_{task_name}");
        let group_id = format!("integration_test_group_{task_name}");

        let parser_json = legacy_kafka_parser_json(&stable_name, &task_name);
        let from = format!(
            "kafka://{broker}?topics={topic}&group.id={group_id}&auto.offset.reset=earliest"
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

        create_kafka_topic(&broker, &topic)
            .await
            .context("pre-create kafka topic for legacy parser test")?;

        pre_create_stable(
            &to_dsn,
            &format!(
                "CREATE STABLE IF NOT EXISTS `{stable_name}` \
                 (`ts` TIMESTAMP, `value` DOUBLE) TAGS (`device` BINARY(32))"
            ),
        )
        .await
        .context("pre-create legacy kafka stable")?;

        let created = client
            .create_task(&new_task)
            .await
            .context("create legacy kafka task via api")?;
        let task_id = created.id;

        client
            .wait_until_running(task_id)
            .await
            .context("wait for legacy kafka task running")?;

        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let schema_temp = manifest_dir.join("config/schema/kafka-meters-temp.toml");
        let schema_power = manifest_dir.join("config/schema/kafka-meters-power.toml");
        let cancel = CancellationToken::new();
        let cancel_for_pub = cancel.clone();
        let pub_handle = tokio::spawn(async move {
            super::kafka_pub(vec![schema_temp, schema_power], broker, topic)
                .publish(cancel_for_pub)
                .await
        });

        let wait_result = wait_for_rows(&to_dsn, &stable_name, 2).await;
        cancel.cancel();
        let pub_result = pub_handle
            .await
            .context("join legacy kafka publish task")?
            .context("publish legacy kafka json messages");
        wait_result.context("wait for legacy kafka stable to have rows")?;
        pub_result?;

        let taos_conn = taosx_utils::taos_conn::TaosConn::create(&to_dsn, 3)
            .await
            .context("create taos conn for legacy kafka verification")?;
        let table_names: Vec<(String,)> = taos_conn
            .query(&format!("SELECT DISTINCT tbname FROM `{stable_name}`"))
            .await
            .context("query legacy kafka child table names")?;
        let table_names = table_names
            .into_iter()
            .map(|(name,)| name)
            .collect::<std::collections::HashSet<_>>();
        assert!(!table_names.is_empty());
        let table_prefix = format!("{stable_name}_{task_name}_");
        assert!(table_names
            .iter()
            .all(|name| name.starts_with(&table_prefix)));

        client
            .stop_task(task_id)
            .await
            .with_context(|| format!("stop legacy kafka task id {task_id}"))?;
        client
            .wait_until_status(task_id, TaskStatus::Stopped)
            .await
            .context("wait for legacy kafka task stopped")?;
        client
            .delete_task(task_id)
            .await
            .context("delete legacy kafka task via api")?;

        let get_after = client
            .get_task(task_id)
            .await
            .context("get legacy kafka task via api")?;
        if get_after.is_some() {
            anyhow::bail!("task {task_id} should have been deleted but still exists");
        }

        cleanup_table(&to_dsn, &stable_name)
            .await
            .context("cleanup legacy kafka stable after integration test")?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Helpers for multi-datastructure test
    // -------------------------------------------------------------------------

    /// For every rule whose `model.using` matches `old_using`, update both the
    /// `using` field (supertable) AND prepend `prefix_` to the `name` template
    /// (child table name). Patching both avoids child-table-name collisions
    /// across test runs when the model name template uses static variables like
    /// `robot_${uuid}` that would otherwise map to the same child table name.
    fn patch_model_using(parser: &mut serde_json::Value, old_using: &str, new_using: &str) {
        if let Some(rules) = parser["rules"].as_array_mut() {
            for rule in rules.iter_mut() {
                let current = rule["model"]["using"].as_str().unwrap_or("");
                if current == old_using {
                    rule["model"]["using"] = serde_json::Value::String(new_using.to_string());
                    // Prefix the child-table name template with the new stable name so
                    // child tables are unique per test run and won't collide across runs.
                    if let Some(name) = rule["model"]["name"].as_str() {
                        rule["model"]["name"] =
                            serde_json::Value::String(format!("{new_using}_{name}"));
                    }
                }
            }
        }
    }

    /// Verify that both supertables contain at least one row.
    async fn verify_multi_datastructure_data(
        to_dsn: &str,
        robot_stable: &str,
        conditionor_stable: &str,
    ) -> anyhow::Result<()> {
        // Use wait_for_rows to handle the race where written_rows is incremented
        // before TDengine finishes committing the auto-created stable.
        wait_for_rows(to_dsn, robot_stable, 1)
            .await
            .with_context(|| format!("wait for robot stable {robot_stable} to have rows"))?;
        wait_for_rows(to_dsn, conditionor_stable, 1)
            .await
            .with_context(|| {
                format!("wait for conditionor stable {conditionor_stable} to have rows")
            })?;

        Ok(())
    }

    /// Stop, wait for stopped status, delete, and confirm a task is gone.
    async fn stop_delete_verify_task(
        client: &crate::core::api::ApiClient,
        task_id: u32,
    ) -> anyhow::Result<()> {
        use ha_core::activity::TaskStatus;

        client
            .stop_task(task_id)
            .await
            .with_context(|| format!("stop kafka task id {task_id}"))?;
        tracing::info!("kafka task {task_id} stop requested");

        client
            .wait_until_status(task_id, TaskStatus::Stopped)
            .await
            .context("wait for kafka task stopped")?;
        tracing::info!("kafka task {task_id} stopped");

        client
            .delete_task(task_id)
            .await
            .context("delete kafka task via api")?;
        tracing::info!("kafka task {task_id} deleted");

        let get_after = client
            .get_task(task_id)
            .await
            .context("get kafka task after delete")?;
        if get_after.is_some() {
            anyhow::bail!("task {task_id} should have been deleted but still exists");
        }

        Ok(())
    }

    /// Test multi-datastructure Kafka ingestion using the production-style parser config.
    ///
    /// Flow:
    /// 1. Load the parser config from `datain-kafka-multi-datastructures.json`.
    /// 2. Rename supertables to test-specific names to avoid cross-test collisions.
    /// 3. Create a taosx task via HTTP API and wait until it is running.
    /// 4. Publish alternating robot and air-conditioner JSON messages via fake schemas.
    /// 5. Wait until at least two rows are written; cancel the publisher.
    /// 6. Verify that rows exist in both the robot and conditionor supertables.
    /// 7. Stop, delete the task, and drop both supertables.
    ///
    /// Expected result:
    ///
    /// Messages with `modelQualifier == "air_robot_v"` are routed to the robot supertable
    /// and messages with `modelQualifier == "air_conditioner_v"` are routed to the
    /// conditionor supertable. Both supertables contain at least one row after the run.
    #[integration_test(tokio::test, with_agent = [true, false])]
    async fn test_kafka_multi_datastructures(with_agent: bool) -> anyhow::Result<()> {
        use std::{fs, path::PathBuf};

        use crate::core::api::NewTask;

        tracing::info!("{test_name}");
        let broker = env_var("KAFKA_BROKER")?;
        let to_dsn = env_var("KAFKA_TASK_TO_DSN")?;
        let client = build_api_client_from_env()?;

        // Remove stale tasks from previous failed runs to avoid overloading TDengine.
        cleanup_tasks_by_prefix(&client, test_name)
            .await
            .context("cleanup stale tasks before test")?;

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let task_cfg_path = manifest_dir.join("config/task/datain-kafka-multi-datastructures.json");
        let cfg: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&task_cfg_path)
                .with_context(|| format!("read task config {:?}", task_cfg_path))?,
        )
        .context("parse task config json")?;

        // Extract the parser config and rename stable names to be test-specific
        let mut parser_json = cfg["tasks"][0]["parser"]["parser"].clone();
        let name_suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let task_name = format!("{test_name}_{name_suffix}");
        let robot_stable = format!("{task_name}_robot");
        let conditionor_stable = format!("{task_name}_conditionor");
        patch_model_using(&mut parser_json, "robot", &robot_stable);
        patch_model_using(&mut parser_json, "conditionor", &conditionor_stable);

        let topic = format!("integration_test_topic_{task_name}");
        let group_id = format!("integration_test_group_{task_name}");
        let from = format!(
            "kafka://{broker}?topics={topic}&group.id={group_id}&auto.offset.reset=earliest"
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

        // Topic must exist before the task is created so that `split_job` can
        // fetch partition metadata and produce a non-empty topics list.
        create_kafka_topic(&broker, &topic)
            .await
            .context("pre-create kafka topic for multi-datastructures test")?;

        let created = client
            .create_task(&new_task)
            .await
            .context("create kafka multi-datastructures task via api")?;
        let task_id = created.id;

        client
            .wait_until_running(task_id)
            .await
            .context("wait for kafka multi-datastructures task running")?;
        tracing::info!("kafka multi-datastructures task running");

        // Publish messages and wait until rows are visible in both TDengine stables.
        // Cancellation is always issued before joining so the publisher outlives
        // verify_multi_datastructure_data even if it returns an error.
        let schema_robot = manifest_dir.join("config/schema/kafka-robot.toml");
        let schema_conditionor = manifest_dir.join("config/schema/kafka-conditionor.toml");
        let cancel = CancellationToken::new();
        let cancel_for_pub = cancel.clone();
        let pub_handle = tokio::spawn(async move {
            super::kafka_pub(vec![schema_robot, schema_conditionor], broker, topic)
                .publish(cancel_for_pub)
                .await
        });

        let wait_result =
            verify_multi_datastructure_data(&to_dsn, &robot_stable, &conditionor_stable).await;
        cancel.cancel();
        let pub_result = pub_handle
            .await
            .context("join kafka publish task")?
            .context("publish kafka multi-datastructures messages");
        wait_result.context("wait for rows in both stables")?;
        pub_result?;
        tracing::info!("kafka multi-datastructures data verified and publish finished");

        stop_delete_verify_task(&client, task_id).await?;

        cleanup_table(&to_dsn, &robot_stable)
            .await
            .context("cleanup robot stable after test_kafka_multi_datastructures")?;
        cleanup_table(&to_dsn, &conditionor_stable)
            .await
            .context("cleanup conditionor stable after test_kafka_multi_datastructures")?;

        Ok(())
    }
}
