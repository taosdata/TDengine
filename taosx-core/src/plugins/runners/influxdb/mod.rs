use std::path::PathBuf;

use anyhow::Context;
use taos::Dsn;
use taosx_ipc::types::DataSet;

use crate::runners::get_plugin_dir;
use config::{ConnectionConfig, INFLUXDB_V1};

mod config;

const EXE: &str = "taosx-influxdb.jar";

pub fn info() -> anyhow::Result<(&'static str, PathBuf, String)> {
    let path = influxdb_jar_path()?;
    let output = std::process::Command::new("java")
        .arg("-jar")
        .arg(&path)
        .arg("-version")
        .output()?;
    Ok((
        "influxdb",
        path,
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}

fn influxdb_jar_path() -> anyhow::Result<PathBuf> {
    let path = get_plugin_dir("influxdb").join(EXE);
    if !path.exists() {
        return Err(anyhow::anyhow!(format!(
            "influxdb plugin not found {:?}",
            path.to_str()
        )));
    }
    Ok(path)
}

pub async fn influxdb_datasets(dsn: Dsn) -> anyhow::Result<Vec<DataSet>> {
    let c = ConnectionConfig::from_dsn(&dsn)?;
    // 连接器路径
    let path = influxdb_jar_path()?;
    // get the version of jdk
    let _ = tokio::process::Command::new("java")
        .arg("-version")
        .output()
        .await
        .context("Get JDK version error")?;
    // startup the connector
    let mut command = tokio::process::Command::new("java");
    // 查询命令
    // 不同版本不同参数
    let output = if INFLUXDB_V1.contains(&c.version.as_str()) {
        // 查询命令
        command
            .arg("-jar")
            .arg(&path)
            .arg("-fetch")
            .arg(&c.version)
            .arg(&c.url)
            .arg(c.username.unwrap())
            .arg(c.password.unwrap())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .with_context(|| "Start InfluxDB collector error")?
    } else {
        command
            .arg("-jar")
            .arg(&path)
            .arg("-fetch")
            .arg(&c.version)
            .arg(&c.url)
            .arg(c.token.unwrap())
            .arg(c.org_id.unwrap())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .with_context(|| "Start InfluxDB collector error")?
    };

    if output.status.success() {
        let s = String::from_utf8(output.stdout.clone())?;
        if s.is_empty() {
            anyhow::bail!("InfluxDB connector returns OK, but result is nothing");
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
            Some(102) => anyhow::bail!("Unauthorized access"),
            Some(103) => anyhow::bail!("Organization not found"),
            None => anyhow::bail!("InfluxDB connector closed by signal"),
            Some(exit) => {
                anyhow::bail!("Unknown exit code {exit}, maybe failed to connect, ip or port error")
            }
        }
    }
}
