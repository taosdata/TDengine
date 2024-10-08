use std::{fs, io::prelude::*, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use chrono::Local;
use itertools::Itertools;
use taos::Dsn;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_process_terminate::TerminateExt;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

use crate::dsv::DataSourceValidation;
use crate::plugins::mask_dsn;
use crate::runners::influxdb::config::{ConnectionConfig, InfluxdbConfig, INFLUXDB_V1};
use crate::runners::log_rotation;
use crate::utils::monitor::send_sub_process_info;
use crate::{
    build_ipc, get_log_keep_days, utils::port_pool::PortPool, Action, DataSet, Transferred,
};

use super::get_data_dir;
use super::get_plugin_dir;

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

/// InfluxDB DSN example: "influxdb://127.0.0.1:8086/?token=abc&orgId=def&mode=normal&beginTime=2023-05-01&endTime="
#[instrument(
    skip_all,
    fields(
        x.influxdb.source = % mask_dsn(& from),
        x.influxdb.sink = % mask_dsn(& to),
        x.influxdb.agent = with_agent.as_ref().map(| a | a.0),
    )
)]
pub async fn influxdb_to_taos(
    from: Dsn,
    _actions: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    let ipc_port = port_pool
        .get()
        .await
        .ok_or(anyhow::anyhow!("No available port for InfluxDB connection"))?;

    // generate config
    let config = InfluxdbConfig::from(&from, ipc_port.get())?;
    // transform to toml
    let toml = toml::to_string(&config)?;
    // write to a temporary file
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    // get the path of the temporary file
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    tracing::info!("Using config file {}", config_path.display());
    // save the temporary file to task dir
    if let Some(task_id) = task_id {
        let path = get_data_dir().join("tasks").join(task_id.to_string());
        std::fs::create_dir_all(&path).unwrap();
        let path = path.join(format!(
            "{}-{}-{}.{}",
            task_id,
            "influxdb",
            chrono::Local::now().format("%Y%m%d%H%M"),
            "toml"
        ));
        let _ = fs::copy(&config_path, path);
    }
    // create socket channel
    let mut ipc = build_ipc(
        format!("127.0.0.1:{ipc_port}").as_str(),
        None,
        &to,
        Some("influxdb"),
        None,
        None,
        &cancel,
        with_agent,
        transferred,
        task_id,
        notify,
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut log_path = log_path();
    std::fs::create_dir_all(&log_path)
        .with_context(|| format!("Log path {}", log_path.display()))?;
    log_path.push(format!("influxdb-{}.log", task_id.unwrap_or(0)));
    tracing::info!("log file dir: {}", &log_path.display());

    let log_keep_days = get_log_keep_days();

    let mut log_rotation = log_rotation(&log_path, log_keep_days);

    // get the version of jdk
    let get_jdk_version = tokio::process::Command::new("java")
        .arg("-version")
        .output()
        .await
        .context("Get JDK version error")?;
    let jdk_version = String::from_utf8(get_jdk_version.stderr.clone())?;

    let mut command = tokio::process::Command::new("java");
    let child;

    // generate report or not
    let enable_coverage = if let Ok(val) = std::env::var("ENABLE_COVERAGE") {
        val.to_lowercase() == "true"
    } else {
        false
    };
    // command line additional arg
    let arg_coverage = {
        let coverage_report_file = format!(
            "/data/coverage/influxdb/jacoco_test_report_{}.exec",
            Local::now().format("%Y%m%d%H%M%S%3f")
        );
        format!(
            "-javaagent:/data/coverage/jacocoagent.jar=destfile={},output=file",
            coverage_report_file
        )
    };
    let args = if enable_coverage {
        vec!["-jar", &arg_coverage]
    } else {
        vec!["-jar"]
    };

    let connector_path = influxdb_jar_path()?;
    if jdk_version.contains("build 1.") {
        child = command
            .args(&args)
            .arg(&connector_path)
            .arg(&config_path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped());
    } else {
        child = command
            .arg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .args(&args)
            .arg(&connector_path)
            .arg(&config_path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped());
    }

    let mut child = child.spawn().context("Start InfluxDB collector error")?;
    send_sub_process_info(child.id(), task_id, "influxdb");
    const ERROR_BUF_SIZE: usize = 2;
    let error_buf = Arc::new(Mutex::new(ringbuf::HeapRb::<String>::new(ERROR_BUF_SIZE)));
    let error_buf_producer = error_buf.clone();
    let stderr = child.stderr.take().context("Failed to capture stderr")?;
    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut line = String::new();
        loop {
            // Read a line from stderr
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break; // End of stream, exit the loop
            }
            if line.contains("ERROR") {
                use ringbuf::Rb;
                let mut guard = error_buf_producer.lock().await;
                let _ = guard.push_overwrite(line.clone());
            }
            // Write the line to log_rotation
            write!(log_rotation, "{}", line)?;
            line.clear();
        }
        Ok::<(), std::io::Error>(())
    });
    // waiting until the end
    tracing::info!("waiting for InfluxDB connector");
    tokio::spawn(async move {
        macro_rules! safe_exit {
            () => {
                // kill child pid before raising error
                let _ = child.terminate_timeout(Duration::from_secs(2)).await;
                let _ = ipc.close().await;
                temp_path.close().unwrap();
            };
        }
        tokio::select! {
            // application exit with error code
            status = child.wait() => {
                let status = status?;
                tracing::info!("InfluxDB exit with {}", status);
                if !status.success() {
                    use ringbuf::Rb;
                    safe_exit!();
                    let error = error_buf.lock().await.iter().join("");
                    anyhow::bail!("InfluxDB exit with status {status}: {error}");
                }
            },
            err = ipc.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                if let Some(err) = err {
                    safe_exit!();
                    anyhow::bail!("InfluxDB writer error: {err}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("InfluxDB task cancelled");
            }
        }
        // stop the connector
        tracing::info!("InfluxDB task Done");
        safe_exit!();
        Ok(())
    }.in_current_span()).await??;

    Ok(())
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
    let output;
    // 不同版本不同参数
    if INFLUXDB_V1.contains(&c.version.as_str()) {
        // 查询命令
        output = command
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
            .with_context(|| "Start InfluxDB collector error")?;
    } else {
        output = command
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
            .with_context(|| "Start InfluxDB collector error")?;
    }

    if output.status.success() {
        let s = String::from_utf8(output.stdout.clone())?;
        if s.is_empty() {
            anyhow::bail!("InfluxDB connector returns OK, but result is nothing");
        }
        let mut vec = Vec::new();
        vec.push(DataSet {
            id: s,
            name: None,
            category: None,
            r#type: None,
            options: None,
            format: None,
        });
        Ok(vec)
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

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectionConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            "influxdb".to_string(),
            format!("invalid dsn: {}, cause: {}", dsn, err),
        ),
        Ok(c) => {
            let result = validate_source_influxdb(c).await;
            match result {
                Err(err) => DataSourceValidation::invalid(
                    "influxdb".to_string(),
                    format!("failed to connect to dsn: {}, cause: {}", dsn, err),
                ),
                Ok(validate) => validate,
            }
        }
    }
}

async fn validate_source_influxdb(
    config: ConnectionConfig,
) -> anyhow::Result<DataSourceValidation> {
    // get the version of jdk
    let _ = tokio::process::Command::new("java")
        .arg("-version")
        .output()
        .await
        .context("Get JDK version error")?;
    // http 客户端
    let client = reqwest::Client::new();
    // 发送请求，获取结果，不同版本不同参数
    let result = if INFLUXDB_V1.contains(&config.version.as_str()) {
        // 获取 bucket 列表的接口
        let url = format!("{}query?q=SHOW DATABASES", config.url);
        // 发送请求，获取结果
        client
            .get(url)
            .header(
                "Authorization",
                format!(
                    "Token {}:{}",
                    config.username.unwrap(),
                    config.password.unwrap()
                ),
            )
            .send()
            .await
    } else {
        // 获取 bucket 列表的接口
        let url = format!(
            "{}api/v2/buckets?orgID={}",
            config.url,
            config.org_id.unwrap()
        );
        // 发送请求，获取结果
        client
            .get(url)
            .header("Authorization", format!("Token {}", config.token.unwrap()))
            .send()
            .await
    };
    // 请求成功
    if result.is_ok() {
        let response = result.unwrap();
        let status = response.status().as_u16();
        let headers = response.headers();
        if status == 200 {
            // 获取版本
            let x_build = headers.get("x-influxdb-build");
            let x_version = headers.get("x-influxdb-version");
            // 拼接版本
            let version = if x_build.is_some() && x_version.is_some() {
                format!(
                    "{} - {}",
                    x_build.unwrap().to_str().unwrap(),
                    x_version.unwrap().to_str().unwrap()
                )
            } else if x_build.is_some() {
                x_build.unwrap().to_str().unwrap().to_string()
            } else if x_version.is_some() {
                x_version.unwrap().to_str().unwrap().to_string()
            } else {
                "unknown".to_string()
            };
            // 组装结果
            Ok(DataSourceValidation {
                valid: true,
                support: true,
                data_source: String::from("influxdb"),
                version: Some(version.clone()),
                message: Some(format!("Your data source is available, its version is {}, which is supported, you can proceed to transfer your data to TDengine.", version.clone())),
                namespaces: None,
            })
        } else {
            let error_code = headers
                .get("x-platform-error-code")
                .unwrap()
                .to_str()
                .unwrap();
            // 组装结果
            Ok(DataSourceValidation {
                valid: false,
                support: false,
                data_source: String::from("influxdb"),
                version: None,
                message: Some(error_code.to_string()),
                namespaces: None,
            })
        }
    } else {
        Ok(DataSourceValidation {
            valid: false,
            support: false,
            data_source: String::from("influxdb"),
            version: None,
            message: Some(result.err().unwrap().to_string()),
            namespaces: None,
        })
    }
}

fn log_path() -> PathBuf {
    super::get_log_dir("")
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_invalid() {
        let dsn = Dsn::from_str("influxdb://?version=2.7").unwrap();
        let validation = is_valid(&dsn).await;
        assert!(!validation.valid);
        assert!(!validation.support);
        assert_eq!("influxdb", validation.data_source);
        assert!(validation.version.is_none());
        assert_eq!(
            "invalid dsn: influxdb://?version=2.7, cause: orgId is required",
            validation.message.unwrap()
        );

        let dsn =
            Dsn::from_str("influxdb://127.0.0.1:8086?version=2.7&orgId=abc&token=123").unwrap();
        let validation = is_valid(&dsn).await;
        assert!(!validation.valid);
        assert!(!validation.support);
        assert_eq!("influxdb", validation.data_source);
        assert!(validation.version.is_none());
        assert!(validation
            .message
            .unwrap_or_default()
            .contains("plugin not found"));
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid_1x() {
        env::set_var("PLUGINS_HOME", "../plugins");
        // ip error
        {
            let dsn = Dsn::from_str(
                "influxdb://192.168.2.13:8088/?version=1.8&username=zqsong&password=Test0102",
            )
            .unwrap();
            let dsv = is_valid(&dsn).await;
            assert!(!dsv.valid);
            dbg!(dsv.message);
        }
        // port error
        {
            let dsn = Dsn::from_str(
                "influxdb://192.168.2.12:8087/?version=1.8&username=zqsong&password=Test0102",
            )
            .unwrap();
            let dsv = is_valid(&dsn).await;
            assert!(!dsv.valid);
            dbg!(dsv.message);
        }
        // success
        {
            let dsn = Dsn::from_str(
                "influxdb://192.168.2.12:8088/?version=1.8&username=zqsong&password=Test0102",
            )
            .unwrap();
            let dsv = is_valid(&dsn).await;
            assert!(dsv.valid);
            assert!(dsv.support);
            assert_eq!("influxdb", dsv.data_source);
            assert_eq!("OSS - 1.8.0", dsv.version.unwrap());
            dbg!(dsv.message);
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid_2x() {
        env::set_var("PLUGINS_HOME", "../plugins");
        // ip error
        {
            let dsn = Dsn::from_str("influxdb://192.168.2.13:8086/?version=2.7&orgId=b7e20025329a0715&token=g4Gxcr3Gipa9tmEDYkdAXODMCdDwOemDxDV30VN2oI0rw7fDca6_jDQbsXvj0LoI2qIeReX7Cf9SGbzeeIN3Xw==").unwrap();
            let dsv = is_valid(&dsn).await;
            assert!(!dsv.valid);
            dbg!(dsv.message);
        }
        // port error
        {
            let dsn = Dsn::from_str("influxdb://192.168.2.12:8087/?version=2.7&orgId=b7e20025329a0715&token=g4Gxcr3Gipa9tmEDYkdAXODMCdDwOemDxDV30VN2oI0rw7fDca6_jDQbsXvj0LoI2qIeReX7Cf9SGbzeeIN3Xw==").unwrap();
            let dsv = is_valid(&dsn).await;
            assert!(!dsv.valid);
            dbg!(dsv.message);
        }
        // token error
        {
            let dsn = Dsn::from_str("influxdb://192.168.2.12:8086/?version=2.7&orgId=b7e20025329a0715&token=g4Gxcr3Gipa9tmEDYkdAXODMCdDwOemDxDV30VN2oI0rw7fDca6_jDQbsXvj0LoI2qIeReX7Cf9SGbzeeIN3Xw=").unwrap();
            let dsv = is_valid(&dsn).await;
            assert!(!dsv.valid);
            dbg!(dsv.message);
        }
        // success
        {
            let dsn = Dsn::from_str("influxdb://192.168.2.12:8086/?version=2.7&orgId=b7e20025329a0715&token=g4Gxcr3Gipa9tmEDYkdAXODMCdDwOemDxDV30VN2oI0rw7fDca6_jDQbsXvj0LoI2qIeReX7Cf9SGbzeeIN3Xw==").unwrap();
            let dsv = is_valid(&dsn).await;
            assert!(dsv.valid);
            assert!(dsv.support);
            assert_eq!("influxdb", dsv.data_source);
            assert_eq!("OSS - v2.7.1", dsv.version.unwrap());
            dbg!(dsv.message);
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid_cloud() {
        // token error
        {
            let dsn = Dsn::from_str("influxdb+https://us-east-1-1.aws.cloud2.influxdata.com:443/?version=2.7&orgId=18cda906d2dda66c&token=soX1nb8pVzjuYlNomO717q19aS0Aa-aA5M4Wnjf1pGYAeepm7M2OmuOfANWHX_Dd0HA8LVqe8SVV83d5-QCBeQ=").unwrap();
            let dsv = is_valid(&dsn).await;
            assert!(!dsv.valid);
            dbg!(dsv.message);
        }
        // success
        {
            let dsn = Dsn::from_str("influxdb+https://us-east-1-1.aws.cloud2.influxdata.com:443/?version=2.7&orgId=18cda906d2dda66c&token=soX1nb8pVzjuYlNomO717q19aS0Aa-aA5M4Wnjf1pGYAeepm7M2OmuOfANWHX_Dd0HA8LVqe8SVV83d5-QCBeQ==").unwrap();
            let dsv = is_valid(&dsn).await;
            assert!(dsv.valid);
            assert!(dsv.support);
            assert_eq!("influxdb", dsv.data_source);
            assert_eq!("Cloud", dsv.version.unwrap());
            dbg!(dsv.message);
        }
    }
}
