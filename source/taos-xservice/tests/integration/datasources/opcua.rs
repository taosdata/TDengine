//! OPC-UA task import/export compatibility tests.

#[cfg(test)]
mod tests {
    use std::io::Read;

    use anyhow::Context;

    use crate::{
        common::fixtures::{
            extract_tasks_json_from_zip, import_export_fixture_path, list_zip_entries,
        },
        core::api::{client::rewrite_json_file_refs, ExplorerApiClient, ExportResponse},
        datasources::{build_explorer_client_from_env, env_var, rewrite_task_target_dsn},
    };
    use taosx_test_macros::integration_test;

    fn set_fixture_task_name(
        payload: &mut serde_json::Value,
        task_name: &str,
    ) -> anyhow::Result<()> {
        let task = payload["tasks"]
            .as_array_mut()
            .and_then(|tasks| tasks.first_mut())
            .context("fixture must contain one task")?;
        task["name"] = serde_json::Value::String(task_name.to_string());
        Ok(())
    }

    fn set_opcua_csv_config_file(
        payload: &mut serde_json::Value,
        uploaded_path: &str,
    ) -> anyhow::Result<()> {
        let task = payload["tasks"]
            .as_array_mut()
            .and_then(|tasks| tasks.first_mut())
            .context("fixture must contain one task")?;
        task["from"]["data"]["csv_config_file"] =
            serde_json::Value::String(format!("@{uploaded_path}"));
        Ok(())
    }

    /// Pure helper that sets OPCUA server address fields from explicit parameters.
    /// No environment variable access, suitable for unit testing.
    fn set_opcua_server_address(
        payload: &mut serde_json::Value,
        host: &str,
        port: u16,
        subject: &str,
    ) -> anyhow::Result<()> {
        let endpoint = format!("opcua://{host}:{port}/{subject}");
        let task = payload["tasks"]
            .as_array_mut()
            .and_then(|tasks| tasks.first_mut())
            .context("fixture must contain one task")?;
        task["from"]["data"]["host"] = serde_json::Value::String(host.to_string());
        task["from"]["data"]["port"] = serde_json::Value::from(port);
        task["from"]["data"]["subject"] = serde_json::Value::String(subject.to_string());
        task["from"]["data"]["endpoint"] = serde_json::Value::String(endpoint);
        Ok(())
    }

    /// Production helper that reads OPCUA server address from environment variables.
    fn set_opcua_server_address_from_env(payload: &mut serde_json::Value) -> anyhow::Result<()> {
        let host = env_var("OPCUA_HOST").unwrap_or_else(|_| "192.168.0.34".to_string());
        let port = env_var("OPCUA_PORT")
            .unwrap_or_else(|_| "53530".to_string())
            .parse::<u16>()
            .context("OPCUA_PORT must be a valid u16")?;
        let subject =
            env_var("OPCUA_SUBJECT").unwrap_or_else(|_| "OPCUA/SimulationServer".to_string());
        set_opcua_server_address(payload, &host, port, &subject)
    }

    async fn import_json_fixture_with_unique_name(
        client: &ExplorerApiClient,
        fixture_name: &str,
        task_name: &str,
    ) -> anyhow::Result<u32> {
        let fixture_path = import_export_fixture_path(fixture_name);
        let fixture_bytes = tokio::fs::read(&fixture_path)
            .await
            .with_context(|| format!("read fixture {}", fixture_path.display()))?;
        let mut payload: serde_json::Value =
            serde_json::from_slice(&fixture_bytes).context("parse fixture JSON")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;
        set_fixture_task_name(&mut payload, task_name)?;
        rewrite_task_target_dsn(&mut payload, &container_target_dsn)
            .context("rewrite OPCUA JSON fixture target dsn")?;
        if fixture_name == "opcua-legacy.json" {
            let uploaded_csv_path = upload_opcua_legacy_csv_resource(client)
                .await
                .context("upload CSV for OPCUA legacy fixture")?;
            set_opcua_csv_config_file(&mut payload, &uploaded_csv_path)?;
            set_opcua_server_address_from_env(&mut payload)?;
        }

        client
            .import_tasks(&payload)
            .await
            .with_context(|| format!("import fixture {fixture_name}"))?;

        client
            .list_tasks()
            .await
            .context("list explorer tasks after JSON import")?
            .into_iter()
            .find(|task| task.name == task_name)
            .map(|task| task.id)
            .with_context(|| format!("imported task '{task_name}' not found"))
    }

    async fn upload_opcua_legacy_csv_resource(
        client: &ExplorerApiClient,
    ) -> anyhow::Result<String> {
        let resource_path = import_export_fixture_path("resources/opcua/opcua-points.csv");
        let csv_bytes = tokio::fs::read(&resource_path)
            .await
            .with_context(|| format!("read OPCUA CSV resource {}", resource_path.display()))?;
        let mut uploaded_paths = client
            .upload_files(vec![("opcua-points.csv".to_string(), csv_bytes)])
            .await
            .context("upload OPCUA legacy CSV resource")?;
        uploaded_paths
            .pop()
            .context("missing uploaded path for OPCUA legacy CSV resource")
    }

    #[test]
    fn opcua_legacy_fixture_csv_path_can_be_rewritten_to_uploaded_path() {
        let fixture_bytes = std::fs::read(import_export_fixture_path("opcua-legacy.json")).unwrap();
        let mut payload: serde_json::Value = serde_json::from_slice(&fixture_bytes).unwrap();

        set_opcua_csv_config_file(&mut payload, "files/imported/points.csv").unwrap();

        assert_eq!(
            payload["tasks"][0]["from"]["data"]["csv_config_file"],
            serde_json::Value::String("@files/imported/points.csv".to_string())
        );
    }

    #[test]
    fn opcua_legacy_fixture_server_address_can_be_rewritten_from_env() {
        let fixture_bytes = std::fs::read(import_export_fixture_path("opcua-legacy.json")).unwrap();
        let mut payload: serde_json::Value = serde_json::from_slice(&fixture_bytes).unwrap();

        set_opcua_server_address(&mut payload, "192.168.100.50", 4840, "Test/Server").unwrap();

        assert_eq!(
            payload["tasks"][0]["from"]["data"]["host"],
            serde_json::Value::String("192.168.100.50".to_string())
        );
        assert_eq!(
            payload["tasks"][0]["from"]["data"]["port"],
            serde_json::Value::from(4840)
        );
        assert_eq!(
            payload["tasks"][0]["from"]["data"]["subject"],
            serde_json::Value::String("Test/Server".to_string())
        );
        assert_eq!(
            payload["tasks"][0]["from"]["data"]["endpoint"],
            serde_json::Value::String("opcua://192.168.100.50:4840/Test/Server".to_string())
        );
    }

    async fn import_zip_fixture_with_unique_name(
        client: &ExplorerApiClient,
        fixture_name: &str,
        task_name: &str,
    ) -> anyhow::Result<u32> {
        let fixture_path = import_export_fixture_path(fixture_name);
        let zip_bytes = tokio::fs::read(&fixture_path)
            .await
            .with_context(|| format!("read ZIP fixture {}", fixture_path.display()))?;
        let mut tasks_json_bytes = None;
        let mut bundled = Vec::new();
        let mut archive =
            zip::ZipArchive::new(std::io::Cursor::new(&zip_bytes)).context("open ZIP archive")?;
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
        let raw_tasks = tasks_json_bytes.context("tasks.json not found in ZIP")?;
        let mut payload: serde_json::Value =
            serde_json::from_slice(&raw_tasks).context("parse ZIP tasks.json")?;
        let container_target_dsn = env_var("CONTAINER_TARGET_DSN")?;
        set_fixture_task_name(&mut payload, task_name)?;
        rewrite_task_target_dsn(&mut payload, &container_target_dsn)
            .context("rewrite OPCUA ZIP fixture target dsn")?;

        if !bundled.is_empty() {
            let mut uploaded_replacements: Vec<(String, String)> =
                Vec::with_capacity(bundled.len());
            for (path, data) in bundled {
                let filename = std::path::Path::new(&path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(&path)
                    .to_string();
                let mut uploaded_paths = client
                    .upload_files(vec![(filename, data)])
                    .await
                    .with_context(|| format!("upload bundled ZIP file {path}"))?;
                let uploaded_path = uploaded_paths.pop().with_context(|| {
                    format!("missing uploaded path for bundled ZIP file {path}")
                })?;
                uploaded_replacements.push((path, uploaded_path));
            }
            let replacements: Vec<(&str, &str)> = uploaded_replacements
                .iter()
                .map(|(original_path, uploaded_path)| {
                    (original_path.as_str(), uploaded_path.as_str())
                })
                .collect();
            rewrite_json_file_refs(&mut payload, &replacements);
        }

        client
            .import_tasks(&payload)
            .await
            .with_context(|| format!("import ZIP fixture {}", fixture_path.display()))?;

        client
            .list_tasks()
            .await
            .context("list explorer tasks after ZIP import")?
            .into_iter()
            .find(|task| task.name == task_name)
            .map(|task| task.id)
            .with_context(|| format!("ZIP-imported task '{task_name}' not found"))
    }

    async fn delete_task_and_assert_absent(
        client: &ExplorerApiClient,
        task_id: u32,
    ) -> anyhow::Result<()> {
        client
            .delete_task(task_id)
            .await
            .with_context(|| format!("delete task {task_id}"))?;
        let remaining = client
            .list_tasks()
            .await
            .context("list explorer tasks after deletion")?;
        anyhow::ensure!(
            remaining.iter().all(|task| task.id != task_id),
            "task {task_id} still exists after deletion"
        );
        Ok(())
    }

    fn assert_opcua_zip_export(resp: &ExportResponse) -> anyhow::Result<()> {
        anyhow::ensure!(
            resp.content_type.contains("application/zip"),
            "expected application/zip content-type, got '{}'",
            resp.content_type
        );

        let entries = list_zip_entries(&resp.bytes).context("list export ZIP entries")?;
        anyhow::ensure!(
            entries.iter().any(|entry| entry == "tasks.json"),
            "export ZIP missing tasks.json: {entries:?}"
        );
        anyhow::ensure!(
            entries.iter().any(|entry| entry.starts_with("files/")),
            "export ZIP missing bundled files directory: {entries:?}"
        );

        let tasks_json =
            extract_tasks_json_from_zip(&resp.bytes).context("extract tasks.json from ZIP")?;
        let from_type = tasks_json["tasks"][0]["from"]["type"]
            .as_str()
            .context("exported tasks[0].from.type is missing or not a string")?;
        anyhow::ensure!(
            from_type == "opcua",
            "expected tasks[0].from.type == 'opcua', got '{from_type}'"
        );
        let csv_config_file = tasks_json["tasks"][0]["from"]["data"]["csv_config_file"]
            .as_str()
            .context("exported csv_config_file is missing or not a string")?;
        anyhow::ensure!(
            csv_config_file.starts_with("@files/"),
            "expected exported csv_config_file to use bundled @files reference, got '{csv_config_file}'"
        );
        Ok(())
    }

    /// Import the legacy OPCUA JSON export, verify the new task is created,
    /// start it and wait until running, then delete it through the Explorer task API.
    #[integration_test(tokio::test)]
    async fn test_opcua_legacy_json_import_creates_task() -> anyhow::Result<()> {
        tracing::info!("{test_name}");
        let explorer_client = build_explorer_client_from_env()?;
        let suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let unique_name = format!("{test_name}_{suffix}");

        let task_id = import_json_fixture_with_unique_name(
            &explorer_client,
            "opcua-legacy.json",
            &unique_name,
        )
        .await?;

        explorer_client
            .start_task(task_id)
            .await
            .with_context(|| format!("start opcua task {task_id}"))?;

        explorer_client
            .wait_until_running(task_id)
            .await
            .context("wait for opcua task running")?;

        delete_task_and_assert_absent(&explorer_client, task_id).await
    }

    /// Import the legacy OPCUA JSON export, start the task and wait until running,
    /// then verify that exporting the created task produces a bundled ZIP payload.
    #[integration_test(tokio::test)]
    async fn test_opcua_legacy_json_import_export_is_zip() -> anyhow::Result<()> {
        tracing::info!("{test_name}");
        let explorer_client = build_explorer_client_from_env()?;
        let suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let unique_name = format!("{test_name}_{suffix}");

        let task_id = import_json_fixture_with_unique_name(
            &explorer_client,
            "opcua-legacy.json",
            &unique_name,
        )
        .await?;

        explorer_client
            .start_task(task_id)
            .await
            .with_context(|| format!("start opcua task {task_id}"))?;

        explorer_client
            .wait_until_running(task_id)
            .await
            .context("wait for opcua task running")?;

        let export_resp = explorer_client
            .export_tasks(&[task_id as i64])
            .await
            .with_context(|| format!("export OPCUA task {task_id}"))?;
        delete_task_and_assert_absent(&explorer_client, task_id).await?;
        assert_opcua_zip_export(&export_resp)
    }

    /// Import the bundled OPCUA ZIP fixture, start the task and wait until running,
    /// then verify a round-trip export still contains `tasks.json` plus the bundled
    /// files directory.
    #[integration_test(tokio::test)]
    async fn test_opcua_zip_import_export_bundles_files() -> anyhow::Result<()> {
        tracing::info!("{test_name}");
        let explorer_client = build_explorer_client_from_env()?;
        let suffix: String = (0..8).map(|_| fastrand::alphanumeric()).collect();
        let unique_name = format!("{test_name}_{suffix}");

        let task_id = import_zip_fixture_with_unique_name(
            &explorer_client,
            "opcua-with-files.zip",
            &unique_name,
        )
        .await?;

        explorer_client
            .start_task(task_id)
            .await
            .with_context(|| format!("start opcua task {task_id}"))?;

        explorer_client
            .wait_until_running(task_id)
            .await
            .context("wait for opcua task running")?;

        let export_resp = explorer_client
            .export_tasks(&[task_id as i64])
            .await
            .with_context(|| format!("export ZIP-imported OPCUA task {task_id}"))?;
        delete_task_and_assert_absent(&explorer_client, task_id).await?;
        assert_opcua_zip_export(&export_resp)
    }
}
