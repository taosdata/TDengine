//! TMQ data source integration tests.

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use ha_core::activity::TaskStatus;
    use std::time::Duration;
    use taosx_test_macros::integration_test;
    use taosx_utils::{backoff::RetryBackoff, taos_conn::TaosConn};

    use crate::{
        core::api::{ApiClient, NewTask},
        datasources::env_var,
    };

    fn build_api_client_from_env() -> anyhow::Result<ApiClient> {
        let api_base = env_var("TAOSX_API_BASE_URL")?;
        let api_username = env_var("TAOSX_API_USERNAME").unwrap_or_else(|_| "root".to_string());
        let api_password = env_var("TAOSX_API_PASSWORD").unwrap_or_else(|_| "taosdata".to_string());
        ApiClient::builder(&api_base)
            .with_auth(&api_username, &api_password)
            .build()
    }

    fn random_suffix() -> String {
        (0..8).map(|_| fastrand::alphanumeric()).collect()
    }

    fn test_identifier_prefix(test_name: &str) -> String {
        test_name
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() {
                    ch.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .collect()
    }

    struct TmqFixtureNames {
        task_name: String,
        source_db: String,
        target_db: String,
        topic: String,
    }

    fn fixture_names(test_name: &str) -> TmqFixtureNames {
        let prefix = test_identifier_prefix(test_name);
        let suffix = random_suffix().to_ascii_lowercase();
        TmqFixtureNames {
            task_name: format!("{prefix}_{suffix}"),
            source_db: format!("tmq_src_{prefix}_{suffix}"),
            target_db: format!("tmq_dst_{prefix}_{suffix}"),
            topic: format!("tmq_topic_{prefix}_{suffix}"),
        }
    }

    fn dsn_with_database(raw: &str, database: &str) -> anyhow::Result<String> {
        let mut url = url::Url::parse(raw).with_context(|| format!("parse DSN {raw}"))?;
        url.set_path(database);
        Ok(url.to_string())
    }

    async fn exec(conn: &TaosConn, sql: impl AsRef<str>) -> anyhow::Result<()> {
        let sql = sql.as_ref();
        conn.exec(sql)
            .await
            .map(|_| ())
            .map_err(anyhow::Error::new)
            .with_context(|| format!("execute SQL: {sql}"))
    }

    async fn prepare_source_topic(
        conn: &TaosConn,
        source_db: &str,
        topic: &str,
        table: &str,
    ) -> anyhow::Result<()> {
        exec(conn, format!("DROP TOPIC IF EXISTS `{topic}`")).await?;
        exec(conn, format!("DROP DATABASE IF EXISTS `{source_db}`")).await?;
        exec(
            conn,
            format!("CREATE DATABASE `{source_db}` VGROUPS 1 WAL_RETENTION_PERIOD 3600"),
        )
        .await?;
        exec(
            conn,
            format!("CREATE TABLE `{source_db}`.`{table}` (`ts` TIMESTAMP, `value` DOUBLE)"),
        )
        .await?;
        exec(
            conn,
            format!("CREATE TOPIC `{topic}` WITH META AS DATABASE `{source_db}`"),
        )
        .await
    }

    async fn cleanup_tmq_fixture(
        conn: &TaosConn,
        source_db: &str,
        target_db: &str,
        topic: &str,
    ) -> anyhow::Result<()> {
        let mut last_error = None;
        for sql in [
            format!("DROP TOPIC IF EXISTS FORCE `{topic}`"),
            format!("DROP DATABASE IF EXISTS `{source_db}`"),
            format!("DROP DATABASE IF EXISTS `{target_db}`"),
        ] {
            if let Err(e) = exec(conn, &sql).await {
                last_error = Some(e);
            }
        }
        if let Some(e) = last_error {
            return Err(e);
        }
        Ok(())
    }

    fn tmq_from_dsn(topic: &str) -> anyhow::Result<String> {
        let host = env_var("TMQ_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = env_var("TMQ_PORT").unwrap_or_else(|_| "6041".to_string());
        let username = env_var("TMQ_USERNAME").unwrap_or_else(|_| "root".to_string());
        let password = env_var("TMQ_PASSWORD").unwrap_or_else(|_| "taosdata".to_string());
        let mut url = url::Url::parse(&format!(
            "tmq+ws://{username}:{password}@{host}:{port}/{topic}"
        ))
        .context("build TMQ source DSN")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("auto.offset.reset", "earliest");
            query.append_pair("commit.chunk.size", "0");
            query.append_pair("commit.interval.ms", "0");
            query.append_pair("experimental.snapshot.enable", "true");
            query.append_pair("prefer", "auto");
            query.append_pair("timeout", "30s");
            query.append_pair("with.meta.delete", "true");
            query.append_pair("with.meta.drop", "true");
        }
        Ok(url.to_string())
    }

    fn assert_vgroup_progress(value: &serde_json::Value) {
        assert!(
            value.is_object(),
            "vgroup progress should be a JSON object: {value}"
        );
        let update_time = value
            .get("update_time")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("missing or invalid vgroup update_time: {value}"));
        assert!(
            update_time > 0,
            "vgroup progress update_time should be a positive epoch millis value: {value}"
        );

        let entries = value
            .get("data")
            .and_then(|v| v.as_array())
            .unwrap_or_else(|| panic!("missing vgroup progress data array: {value}"));
        assert!(
            !entries.is_empty(),
            "vgroup progress data should not be empty: {value}"
        );
        for entry in entries {
            let topic = entry.get("topic").and_then(|v| v.as_str()).unwrap_or("");
            let vgroup = entry.get("vgroup").and_then(|v| v.as_i64()).unwrap_or(-1);
            let offset = entry.get("offset").and_then(|v| v.as_i64()).unwrap_or(-1);
            let latest = entry.get("latest").and_then(|v| v.as_i64()).unwrap_or(-1);

            assert!(!topic.is_empty(), "vgroup entry has empty topic: {entry}");
            assert!(vgroup >= 0, "vgroup entry has invalid vgroup: {entry}");
            assert!(offset >= 0, "vgroup entry has invalid offset: {entry}");
            assert!(
                latest >= offset,
                "vgroup entry latest should be >= offset: {entry}"
            );
        }
    }

    fn vgroup_progress_has_data(value: &serde_json::Value) -> bool {
        let Some(update_time) = value.get("update_time").and_then(|v| v.as_i64()) else {
            return false;
        };
        if update_time <= 0 {
            return false;
        }
        value
            .get("data")
            .and_then(|v| v.as_array())
            .is_some_and(|entries| !entries.is_empty())
    }

    async fn wait_until_vgroup_progress(
        client: &ApiClient,
        task_id: u32,
    ) -> anyhow::Result<serde_json::Value> {
        let mut backoff = RetryBackoff::new(Duration::from_secs(1), Duration::from_secs(5));
        loop {
            let progress = client
                .get_task_vgroup_progress(task_id)
                .await
                .context("get TMQ vgroup progress")?;
            if vgroup_progress_has_data(&progress) {
                return Ok(progress);
            }
            if backoff.retries() >= 10 {
                anyhow::bail!(
                    "vgroup progress did not contain persisted data after {} attempts: {progress}",
                    backoff.retries()
                );
            }
            backoff.wait().await;
        }
    }

    fn assert_table_progress(value: &serde_json::Value, table: &str) {
        assert_eq!(value["table_name"], table);
        assert_eq!(
            value["from_count"].as_u64(),
            Some(1),
            "expected one source row in table progress: {value}"
        );
        assert_eq!(
            value["to_count"].as_u64(),
            Some(1),
            "expected one target row in table progress: {value}"
        );

        let from_last_ts = value
            .get("from_last_ts")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| panic!("missing source last timestamp: {value}"));
        let to_last_ts = value
            .get("to_last_ts")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| panic!("missing target last timestamp: {value}"));
        assert_eq!(
            from_last_ts, to_last_ts,
            "source and target last timestamps should match: {value}"
        );
    }

    async fn run_tmq_task_with_progress_assertions(
        client: &ApiClient,
        new_task: &NewTask,
        source_db: &str,
        table: &str,
    ) -> anyhow::Result<()> {
        let mut created_task_id = None;
        let test_result = async {
            let created = client
                .create_task(new_task)
                .await
                .context("create TMQ task via API")?;
            let task_id = created.id;
            created_task_id = Some(task_id);

            let metrics = client
                .wait_until_written_rows(task_id, 1)
                .await
                .context("wait for TMQ task written rows")?;
            let written_rows = metrics["current"]["written_rows"].as_u64().unwrap_or(0);
            assert!(
                written_rows >= 1,
                "expected written_rows >= 1, got {metrics}"
            );

            let vgroup_progress = wait_until_vgroup_progress(client, task_id).await?;
            assert_vgroup_progress(&vgroup_progress);

            let table_progress = client
                .get_task_table_progress(task_id, &format!("{source_db}.{table}"))
                .await
                .context("get TMQ table progress")?;
            assert_table_progress(&table_progress, table);

            client
                .stop_task(task_id)
                .await
                .with_context(|| format!("stop TMQ task id {task_id}"))?;
            client
                .wait_until_status(task_id, TaskStatus::Stopped)
                .await
                .context("wait for TMQ task stopped")?;
            client
                .delete_task(task_id)
                .await
                .context("delete TMQ task via API")?;
            created_task_id = None;
            anyhow::Ok(())
        }
        .await;

        if let Some(task_id) = created_task_id {
            let _ = client.stop_task(task_id).await;
            let _ = client.delete_task(task_id).await;
        }

        test_result
    }

    /// Creates a TMQ topic from a generated TDengine database and verifies the
    /// task metrics and progress endpoints after consuming the snapshot.
    #[integration_test(tokio::test, with_agent = [false])]
    async fn test_tmq_task_metrics_and_progress(with_agent: bool) -> anyhow::Result<()> {
        tracing::info!("{}", test_name);

        let host_target_dsn = env_var("HOST_TARGET_DSN")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;
        let admin_conn = TaosConn::create(&host_target_dsn, 3)
            .await
            .map_err(anyhow::Error::new)
            .context("create TDengine admin connection")?;

        let names = fixture_names(test_name);
        let table = "meters";

        cleanup_tmq_fixture(
            &admin_conn,
            &names.source_db,
            &names.target_db,
            &names.topic,
        )
        .await
        .context("cleanup stale TMQ fixture")?;
        exec(
            &admin_conn,
            format!("CREATE DATABASE `{}` VGROUPS 1", names.target_db),
        )
        .await
        .context("create target database")?;
        prepare_source_topic(&admin_conn, &names.source_db, &names.topic, table)
            .await
            .context("prepare TMQ source topic")?;
        exec(
            &admin_conn,
            format!(
                "INSERT INTO `{}`.`{table}` VALUES (now, 1.0)",
                names.source_db
            ),
        )
        .await
        .context("insert TMQ source row before task startup")?;

        let client = build_api_client_from_env()?;
        let via = crate::datasources::resolve_agent_via(&client, with_agent).await?;
        let from = tmq_from_dsn(&names.topic)?;
        let to = dsn_with_database(&container_target_dsn, &names.target_db)?;
        let new_task = NewTask {
            name: names.task_name.clone(),
            from,
            to,
            parser: None,
            via,
            labels: Some(vec!["type::datain".to_string()]),
        };

        let test_result =
            run_tmq_task_with_progress_assertions(&client, &new_task, &names.source_db, table)
                .await;

        let cleanup_result = cleanup_tmq_fixture(
            &admin_conn,
            &names.source_db,
            &names.target_db,
            &names.topic,
        )
        .await
        .context("cleanup TMQ fixture");

        test_result?;
        cleanup_result
    }

    #[test]
    fn test_dsn_with_database_replaces_path() {
        let dsn =
            dsn_with_database("taos+http://root:taosdata@localhost:6041/test", "target").unwrap();

        assert_eq!(dsn, "taos+http://root:taosdata@localhost:6041/target");
    }

    #[test]
    fn test_fixture_names_include_normalized_test_name() {
        let names = fixture_names("test_tmq_task_metrics_and_progress_case0");
        let expected_prefix = "test_tmq_task_metrics_and_progress_case0";

        assert!(names.task_name.starts_with(expected_prefix));
        assert!(names
            .source_db
            .starts_with(&format!("tmq_src_{expected_prefix}")));
        assert!(names
            .target_db
            .starts_with(&format!("tmq_dst_{expected_prefix}")));
        assert!(names
            .topic
            .starts_with(&format!("tmq_topic_{expected_prefix}")));
    }
}
