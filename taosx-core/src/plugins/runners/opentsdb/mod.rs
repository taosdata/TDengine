use std::path::PathBuf;

use anyhow::Context;
use taos::Dsn;
use taosx_ipc::types::DataSet;

use crate::runners::{get_plugin_dir, opentsdb::config::ConnectionConfig};

mod config;

const EXE: &str = "taosx-opentsdb.jar";

pub fn info() -> anyhow::Result<(&'static str, PathBuf, String)> {
    let path = opentsdb_jar_path()?;
    let output = std::process::Command::new("java")
        .arg("-jar")
        .arg(&path)
        .arg("-version")
        .output()?;
    Ok((
        "opentsdb",
        path,
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}

fn opentsdb_jar_path() -> anyhow::Result<PathBuf> {
    let path = get_plugin_dir("opentsdb").join(EXE);
    if !path.exists() {
        anyhow::bail!(format!("opentsdb plugin not found {:?}", path))
    }
    Ok(path)
}

pub async fn opentsdb_datasets(dsn: Dsn) -> anyhow::Result<Vec<DataSet>> {
    let config = ConnectionConfig::from_dsn(&dsn);
    match config {
        Err(err) => {
            anyhow::bail!(err)
        }
        Ok(c) => {
            // 连接器路径
            let connector_path = opentsdb_jar_path()?;
            // get the version of jdk
            let _ = tokio::process::Command::new("java")
                .arg("-version")
                .output()
                .await
                .context("Get JDK version error")?;
            // startup the connector
            let mut command = tokio::process::Command::new("java");
            // 查询命令
            let output = command
                .arg("-jar")
                .arg(&connector_path)
                .arg("-fetch")
                .arg(&c.url)
                .kill_on_drop(true)
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::piped())
                .output()
                .await
                .with_context(|| "Start OpenTSDB collector error")?;
            if output.status.success() {
                let s = String::from_utf8(output.stdout.clone())?;
                if s.is_empty() {
                    anyhow::bail!("OpenTSDB connector returns OK, but result is nothing");
                }

                Ok(vec![DataSet {
                    id: s,
                    name: None,
                    category: None,
                    r#type: None,
                    options: None,
                    format: None,
                }])
            } else {
                match output.status.code() {
                    Some(101) => anyhow::bail!("Failed to connect, ip or port error"),
                    Some(102) => anyhow::bail!("Protocol error"),
                    Some(103) => anyhow::bail!("Params error or service mismatch"),
                    None => anyhow::bail!("OpenTSDB connector closed by signal"),
                    Some(exit) => {
                        anyhow::bail!(
                            "Unknown exit code {exit}, maybe failed to connect, ip or port error"
                        )
                    }
                }
            }
        }
    }
}
