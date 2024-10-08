use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::{io::Write, path::PathBuf, sync::Arc};

use anyhow::{bail, Context};
use chrono::Utc;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use ringbuf::Rb;
use rumqttc::tokio_rustls::rustls;
use rumqttc::{
    tokio_rustls, AsyncClient, Event, Incoming, MqttOptions, QoS, SubscribeFilter,
    TlsConfiguration, Transport,
};
use serde_json::json;
use taos::Dsn;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_process_terminate::TerminateExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::dsv::DataSourceValidation;
use crate::plugins::transform::sample::DsSampleIn;
use crate::runners::mqtt::config::{MqttConfig, MqttConnectConfig};
use crate::runners::{log_rotation, NoCertificateVerification};
use crate::utils::monitor::send_sub_process_info;
use crate::{
    build_ipc, get_log_keep_days, plugins::runners::get_plugin_dir, utils::port_pool::PortPool,
    Parser, Transferred,
};

use super::get_data_dir;

mod config;

pub const MQTT_ID: &str = "mqtt";

const EXE: &str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "taosx-mqtt.exe"
        } else {
            "taosx-mqtt"
        }
    }
};

fn mqtt_exe_path() -> anyhow::Result<PathBuf> {
    let path = get_plugin_dir("mqtt").join(EXE);
    if !path.exists() {
        anyhow::bail!(format!("mqtt plugin not found {:?}", path))
    }
    Ok(path)
}

fn log_path() -> PathBuf {
    super::get_log_dir("")
}

pub fn info() -> anyhow::Result<(&'static str, PathBuf, String)> {
    let path = mqtt_exe_path()?;
    let output = std::process::Command::new(&path)
        .arg("--version")
        .output()?;
    Ok((
        "mqtt",
        path,
        String::from_utf8_lossy(&output.stderr).trim().to_string(),
    ))
}

/// Run the mqtt DataIn task
#[instrument(skip_all)]
pub async fn mqtt_to_taos(
    from: Dsn,
    parser: Option<Parser>,
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
        .ok_or_else(|| anyhow::format_err!("No available port for MQTT connection"))?;

    let config = MqttConfig::from(&from, Some(ipc_port.get()), task_id)?;
    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    tracing::info!(
        "Using mqtt config file {} \n{}",
        config_path.display(),
        toml
    );

    // save the temporary file to task dir
    if let Some(task_id) = task_id {
        let path = get_data_dir().join("tasks").join(task_id.to_string());
        std::fs::create_dir_all(&path).map_err(|err| {
            anyhow::format_err!("failed to create task dir: {:?}, cause: {:?}", path, err)
        })?;
        let path = path.join(format!(
            "{}-{}-{}.{}",
            task_id,
            "mqtt",
            chrono::Local::now().format("%Y%m%d%H%M"),
            "toml"
        ));
        let _ = std::fs::copy(&config_path, path);
    }
    // create socket channel
    let mut ipc_handler = build_ipc(
        &config.remote,
        parser,
        &to,
        Some("mqtt"),
        None,
        None,
        &cancel,
        with_agent,
        transferred,
        task_id,
        notify,
    )
    .await?;

    let mqtt = mqtt_exe_path()?;
    let mut command = tokio::process::Command::new(mqtt);

    let mut log_path = log_path();
    std::fs::create_dir_all(&log_path)
        .with_context(|| format!("Log path {}", log_path.display()))?;
    log_path.push(format!("mqtt-{}.log", task_id.unwrap_or(0)));
    tracing::info!("log file: {}", &log_path.display());

    let log_keep_days = get_log_keep_days();
    let mut log_rotation = log_rotation(&log_path, log_keep_days);
    let child = command
        .arg("-c")
        .arg(&config_path)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = child
        .spawn()
        .map_err(|err| anyhow::format_err!("Cannot spawn mqtt process: {err:?}"))?;
    send_sub_process_info(child.id(), task_id, "mqtt");
    const ERROR_BUF_SIZE: usize = 2;
    let error_buf = Arc::new(Mutex::new(ringbuf::HeapRb::<String>::new(ERROR_BUF_SIZE)));
    let error_buf_producer = error_buf.clone();
    let stderr = child.stderr.take().expect("Failed to capture stderr");
    let is_killed = Arc::new(AtomicBool::new(false));
    let is_killed_clone = is_killed.clone();
    let stderr_handler = tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut line = String::new();
        loop {
            // Read a line from stderr
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break;
            }
            if line.contains("fatal") || line.contains("error") {
                use ringbuf::Rb;
                let mut guard = error_buf_producer.lock().await;
                let _ = guard.push_overwrite(line.clone());
            }
            if line.contains(r#""stop server""#) {
                is_killed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            // Write the line to log_rotation
            write!(log_rotation, "{}", line)?;
            line.clear();
        }
        #[allow(unreachable_code)]
        Ok::<(), std::io::Error>(())
    });

    macro_rules! safe_exit {
        () => {
            let _ = ipc_handler.close().await;
            let _ = temp_path.close();
        };
    }
    tokio::select! {
        status = child.wait() => {
            let status = status?;
            tracing::info!("mqtt exit with {status}");
            let _ = stderr_handler.await;
            if !status.success() {
                safe_exit!();
                let error = error_buf.lock().await.iter().join("");
                anyhow::bail!("MQTT exit with {}\n{error}", status);
            } else if is_killed.load(std::sync::atomic::Ordering::SeqCst) {
                safe_exit!();
                anyhow::bail!("MQTT process is killed by user or system");
            }
        },
        err = ipc_handler.recv_error() => {
            tracing::info!("have received worker thread panicked message, terminate child process");
            tokio::time::sleep(Duration::from_secs(1)).await;
            // Check if the child process is still running.
            if let Ok(Some(status)) = child.try_wait() {
                tracing::warn!(err, "IPC handler error, mqtt already exit with {status}");
                let _ = stderr_handler.await;
                if status.success() {
                    safe_exit!();
                    if let Some(err) = err {
                        anyhow::bail!("MQTT writer error: {err}");
                    } else {
                        anyhow::bail!("IPC panicked and mqtt exit with 0");
                    }
                } else {
                    let error = error_buf.lock().await.iter().join("");
                    let error = if let Some(err) = err {
                        format!("MQTT writer fails, details:\n  1. MQTT IPC error: {err}.\n  2. mqtt exit with {status}: {error}")
                    } else {
                        format!("MQTT connector exit with {status}: {error}")
                    };
                    safe_exit!();
                    anyhow::bail!("{error}");
                }
            } else {
                tracing::warn!(err, "IPC handler error, mqtt connector is still running, terminate it");
                // The child process is still running, terminate it.
                let _ = child.terminate_timeout(Duration::from_secs(2)).await;
                let _ = stderr_handler.await;
                safe_exit!();
                if let Some(err) = err {
                    anyhow::bail!("mqtt writer error: {err}");
                } else {
                    anyhow::bail!("IPC handler closed and mqtt connector exit");
                }
            }
        },
        _ = cancel.cancelled() => {
            tracing::info!("mqtt task cancelled");
        },
    }

    let _ = child.terminate_timeout(Duration::from_secs(2)).await;
    tracing::info!("mqtt to taos task done");
    safe_exit!();
    Ok(())
}

/// Check the connectivity of the mqtt server
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = MqttConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            "mqtt".to_string(),
            format!("invalid mqtt dsn: {}, cause: {}", dsn, err),
        ),
        Ok(c) => {
            let mqtt_config = MqttConfig {
                log_level: "".to_string(),
                remote: "".to_string(),
                mqtt: c,
                topics: HashMap::new(),
                dump: None,
            };
            let valid = is_valid_impl(mqtt_config).await;
            match valid {
                Err(err) => DataSourceValidation::invalid(
                    "mqtt".to_string(),
                    format!("failed to connect to dsn: {}, cause: {}", dsn, err),
                ),
                Ok(v) => v,
            }
        }
    }
}

async fn is_valid_impl(config: MqttConfig) -> anyhow::Result<DataSourceValidation> {
    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;

    // startup the connector
    let mqtt = mqtt_exe_path()?;
    let mut command = tokio::process::Command::new(mqtt.clone());
    let output = command
        .arg("--check")
        .arg("-c")
        .arg(config_file.path())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .with_context(|| format!("failed to execute mqtt: {:?}", mqtt.as_path()))?;

    if output.status.success() {
        let result: serde_json::Value =
            serde_json::from_slice(&output.stdout).with_context(|| {
                format!(
                    "Deserialize mqtt validation result error: {}",
                    String::from_utf8_lossy(&output.stdout)
                )
            })?;
        Ok(DataSourceValidation {
            valid: result["valid"].as_bool().unwrap_or(false),
            support: result["support"].as_bool().unwrap_or(false),
            data_source: "mqtt".to_string(),
            version: result["version"].as_str().map(|s| s.to_string()),
            message: result["message"].as_str().map(|s| s.to_string()),
            namespaces: None,
        })
    } else {
        Ok(DataSourceValidation::invalid(
            "mqtt".to_string(),
            format!(
                "failed to execute mqtt: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ))
    }
}

/// get sample data from mqtt server
pub async fn get_sample(dsn: &Dsn, limit: usize, timeout: Duration) -> anyhow::Result<DsSampleIn> {
    let sample_list: Vec<String> = get_sample_impl(dsn, limit, timeout).await?;

    let mut sample_vec: Vec<LinkedHashMap<String, serde_json::Value>> = Vec::new();
    for payload in sample_list {
        let mut p = LinkedHashMap::new();
        p.insert("payload".to_string(), json!(payload));
        sample_vec.push(p);
    }

    let sample_json = json!({
        "input": sample_vec,
        "parser": {}
    });

    let sample: DsSampleIn = serde_json::from_value(sample_json.clone()).map_err(|err| {
        anyhow::anyhow!(
            "failed to parse mqtt sample data: {:?}, cause: {:?}",
            sample_json,
            err
        )
    })?;

    Ok(sample)
}

async fn get_sample_impl(
    dsn: &Dsn,
    limit: usize,
    timeout: Duration,
) -> anyhow::Result<Vec<String>> {
    let version = MqttConnectConfig::parse_version(dsn)?;
    match version.as_str() {
        "5.0" => get_sample_impl_v5(dsn, limit, timeout).await,
        _ => get_sample_impl_v3(dsn, limit, timeout).await,
    }
}

async fn get_sample_impl_v3(
    dsn: &Dsn,
    limit: usize,
    timeout: Duration,
) -> anyhow::Result<Vec<String>> {
    // build mqtt client
    let config = MqttConfig::from(dsn, None, None)?;
    let connect_config = config.mqtt;
    // host and port
    let (host, port) = connect_config.host_port();
    let mut options = MqttOptions::new(connect_config.client_id(), host, port);
    // username and password
    if let (Some(username), Some(password)) = (connect_config.username(), connect_config.password())
    {
        options.set_credentials(username, password);
    }
    // ssl
    if MqttConnectConfig::ssl_enabled(dsn) {
        let (ca, client_cert, client_key) = connect_config.ssl()?;
        let tls_config = build_tls_config(ca, client_cert, client_key)?;
        options.set_transport(Transport::tls_with_config(tls_config));
    }
    // keep alive
    options.set_keep_alive(connect_config.keep_alive());
    // clean session
    options.set_clean_session(connect_config.clean_session());
    // topics
    let (client, mut event_loop) = AsyncClient::new(options, 10);
    let mut subscriptions = vec![];
    for (topic, qos) in &config.topics {
        let subscribe_filter = match qos {
            0 => SubscribeFilter::new(topic.clone(), QoS::AtMostOnce), // 0: AtMostOnce
            1 => SubscribeFilter::new(topic.clone(), QoS::AtLeastOnce), // 1: AtLeastOnce
            2 => SubscribeFilter::new(topic.clone(), QoS::ExactlyOnce), // 2: ExactlyOnce
            _ => bail!("invalid qos: {}", qos),
        };
        subscriptions.push(subscribe_filter);
    }
    client
        .try_subscribe_many(subscriptions)
        .map_err(|err| anyhow::anyhow!("failed to subscribe mqtt topics, cause: {:?}", err))?;

    let start = Utc::now().timestamp();
    let mut count = 0;
    let mut payload_list: Vec<String> = Vec::new();
    'GET_SAMPLE_V3: loop {
        let now = Utc::now().timestamp();
        if now - start > timeout.as_secs() as i64 || count >= limit {
            break 'GET_SAMPLE_V3;
        }

        let notification = match tokio::time::timeout(Duration::from_secs(1), event_loop.poll())
            .await
        {
            Ok(event) => event
                .map_err(|err| anyhow::anyhow!("failed to poll mqtt event, cause: {:?}", err))?,
            Err(_err) => {
                continue 'GET_SAMPLE_V3;
            }
        };

        if let Event::Incoming(Incoming::Publish(publish)) = notification {
            let payload = String::from_utf8(publish.payload.to_vec()).map_err(|err| {
                anyhow::anyhow!(
                    "failed to parse mqtt payload: {:?}, cause: {:?}",
                    publish,
                    err
                )
            })?;
            payload_list.push(payload);
            count += 1;
        }
    }

    Ok(payload_list)
}

async fn get_sample_impl_v5(
    dsn: &Dsn,
    limit: usize,
    timeout: Duration,
) -> anyhow::Result<Vec<String>> {
    let config = MqttConfig::from(dsn, None, None)?;
    let connect_config = config.mqtt;

    let (host, port) = connect_config.host_port();
    let mut options = rumqttc::v5::MqttOptions::new(connect_config.client_id(), host, port);
    // username and password
    if let (Some(username), Some(password)) = (connect_config.username(), connect_config.password())
    {
        options.set_credentials(username, password);
    }
    // ssl
    if MqttConnectConfig::ssl_enabled(dsn) {
        let (ca, client_cert, client_key) = connect_config.ssl()?;
        let tls_config = build_tls_config(ca, client_cert, client_key)?;
        options.set_transport(Transport::tls_with_config(tls_config));
    }
    // keep alive
    options.set_keep_alive(connect_config.keep_alive());
    // clean session
    options.set_clean_start(connect_config.clean_session());

    // topics
    let mut subscriptions = vec![];
    for (topic, qos) in config.topics {
        let filter = match qos {
            0 => rumqttc::v5::mqttbytes::v5::Filter::new(
                topic,
                rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            ),
            1 => rumqttc::v5::mqttbytes::v5::Filter::new(
                topic,
                rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            ),
            2 => rumqttc::v5::mqttbytes::v5::Filter::new(
                topic,
                rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
            ),
            _ => bail!("invalid qos: {}", qos),
        };
        subscriptions.push(filter);
    }
    let (client, mut event_loop) = rumqttc::v5::AsyncClient::new(options, 10);
    client
        .try_subscribe_many(subscriptions)
        .map_err(|err| anyhow::anyhow!("failed to subscribe mqtt topics, cause: {:?}", err))?;

    let start = Utc::now().timestamp();
    let mut count = 0;
    let mut payload_list: Vec<String> = Vec::new();
    'GET_SAMPLE_V5: loop {
        let now = Utc::now().timestamp();
        if now - start > timeout.as_secs() as i64 || count >= limit {
            break 'GET_SAMPLE_V5;
        }

        let event = match tokio::time::timeout(Duration::from_secs(1), event_loop.poll()).await {
            Err(_err) => {
                continue 'GET_SAMPLE_V5;
            }
            Ok(event) => event
                .map_err(|err| anyhow::anyhow!("failed to poll mqtt event, cause: {:?}", err))?,
        };

        if let rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::Publish(publish)) = event {
            let payload = String::from_utf8(publish.payload.to_vec()).map_err(|err| {
                anyhow::anyhow!(
                    "failed to parse mqtt payload: {:?}, cause: {:?}",
                    publish,
                    err
                )
            })?;
            payload_list.push(payload);
            count += 1;
        }
    }

    Ok(payload_list)
}

fn build_tls_config(
    ca: Vec<u8>,
    _client_pem: Vec<u8>,
    _client_key: Vec<u8>,
) -> anyhow::Result<TlsConfiguration> {
    let mut ca = std::io::Cursor::new(ca);

    use itertools::Itertools;
    let certs: Vec<_> = rustls_pemfile::certs(&mut ca).try_collect().unwrap();
    let mut root_cert_store = rustls::RootCertStore::empty();
    root_cert_store.add_parsable_certificates(
        rustls_native_certs::load_native_certs().expect("could not load platform certs"),
    );
    root_cert_store.add_parsable_certificates(certs);
    let mut rustls_config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();
    rustls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoCertificateVerification()));
    let tls_config = TlsConfiguration::Rustls(Arc::new(rustls_config));

    Ok(tls_config)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_invalid() {
        let dsn = Dsn::from_str("mqtt://").unwrap();
        let validation = is_valid(&dsn).await;
        assert!(!validation.valid);
        assert!(!validation.support);
        assert_eq!("mqtt", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!(
            "invalid mqtt dsn: mqtt://, cause: host is required",
            validation.message.unwrap()
        );

        let dsn =
            Dsn::from_str("mqtt://127.0.0.1:1833?clean_session=true&keep_alive=60&version=3.0")
                .unwrap();
        let validation = is_valid(&dsn).await;
        assert!(!validation.valid);
        assert!(!validation.support);
        assert_eq!("mqtt", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!("failed to connect to dsn: mqtt://127.0.0.1:1833?clean_session=true&keep_alive=60&version=3.0, cause: mqtt plugin not found \"/usr/local/taos/plugins/mqtt/taosx-mqtt\"", validation.message.unwrap());
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid() {
        unsafe {
            std::env::set_var("PLUGINS_HOME", "../plugins");
        }

        let dsn = Dsn::from_str("mqtt://192.168.1.42:1883?version=3.0").unwrap();
        let dsv = is_valid(&dsn).await;
        dbg!(&dsv);
        assert!(dsv.valid);
        assert!(dsv.support);
        assert_eq!("mqtt", dsv.data_source);
        assert_eq!(None, dsv.version);
    }
}
