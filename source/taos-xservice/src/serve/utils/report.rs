use std::path::Path;

use anyhow::Context;
use taosx_core::get_data_dir;

use crate::build;

pub fn get_instance_id(dir: &Path) -> anyhow::Result<Option<String>> {
    let mut read_dir = std::fs::read_dir(dir).context("read data dir error")?;

    let mut instance_id = None;
    while let Some(entry) = read_dir
        .next()
        .transpose()
        .context("iter data dir instance file error")?
    {
        let filename = entry.file_name();
        let Some(filename) = filename.to_str() else {
            continue;
        };
        let Some(id) = filename.strip_prefix("instance.") else {
            continue;
        };
        instance_id = Some(id.to_string());
        break;
    }

    if instance_id.is_some() {
        return Ok(None);
    }

    let instance_id = uuid::Uuid::new_v4();
    std::fs::File::create(dir.join(format!("instance.{instance_id}")))
        .context("create instance file error")?;

    Ok(Some(instance_id.to_string()))
}

async fn report_info(url: &str, instance_id: &str) -> anyhow::Result<()> {
    let resp = reqwest::Client::new()
        .post(url)
        .json(&serde_json::json!({
            "appName": "taosX",
            "instanceId": instance_id,
            "xnodeId": instance_id,
            "reportVersion": 1,
            "version": build::TD_VERSION,
            "buildInfo": format!("Built at {}", build::BUILD_TIME),
            "gitInfo": build::COMMIT_HASH,
        }))
        .send()
        .await
        .context("report instance info error")?;

    let code = resp.status();
    if !code.is_success() {
        if let Ok(res) = resp.text().await {
            anyhow::bail!("report instance info failed, code {code}, response: {res}");
        } else {
            anyhow::bail!("report instance info failed, code {code}");
        }
    }

    Ok(())
}

pub fn report(url: String) {
    let dir = get_data_dir();
    std::thread::spawn(move || match get_instance_id(&dir) {
        Ok(Some(instance_id)) => {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async move {
                if let Err(e) = report_info(&url, &instance_id).await {
                    tracing::warn!("report telemetry info error: {e:#}");
                }
            })
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!("get instance id error: {e:#}");
        }
    });
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn instant_id_test() {
        let dir = tempdir().unwrap();
        assert!(get_instance_id(dir.path()).is_ok_and(|id| id.is_some()));
        assert!(get_instance_id(dir.path()).is_ok_and(|id| id.is_none()));
    }
}
