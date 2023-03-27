use std::{
    collections::HashMap,
    io::prelude::*,
    num::ParseIntError,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

use anyhow::bail;
use anyhow::Context;
use itertools::Itertools;
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use taosx_ipc::prelude::IpcDataType;
use tracing::instrument;

use crate::{
    plugins::sink,
    utils::{port_pool::PortPool, stop_thread},
    Action,
};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum OpcType {
    OPCUA,
    OPCDA,
}

impl FromStr for OpcType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            _ => Err(s.to_string()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum OpcError {
    #[error("Endpoint is required in OPC dsn: {0} like `opc+..://localhost:4840?...`")]
    EndpointIsRequired(Dsn),
    #[error("Database name is required in OPC dsn: {0}")]
    DatabaseIsRequired(Dsn),
    #[error("Username and password are both required for UserName authentication method in {0}")]
    UserPassRequired(Dsn),
    #[error("Parse integer error from {1} while parsing parameter {0}: {2:?}")]
    ParseNumberError(&'static str, String, ParseIntError),
}

#[derive(Debug, serde::Serialize)]
struct OPCConfig {
    opc_type: OpcType,
    connect: ConnectConfig,
    collect: CollectConfig,
    report: ReportConfig,

    #[serde(skip)]
    param_mapping: HashMap<String, (String, String, IpcDataType)>,
    #[serde(skip)]
    /// table_info: table_name, Vec<(field, type)>
    table_info: HashMap<String, Vec<(String, String)>>,
}

#[derive(Debug, serde::Serialize)]
struct ConnectConfig {
    ua: Option<UaConnectConfig>,
    da: Option<DaConnectConfig>,
}

#[derive(Debug, serde::Serialize, Default)]
enum AuthMethod {
    Anonymous,
    UserName,
    #[default]
    Certificate,
}

#[derive(Debug, serde::Serialize)]
struct UaConnectConfig {
    endpoint: String,
    connect_timeout: Option<i64>,
    request_timeout: Option<i64>,
    security_policy: String,
    security_mode: String,
    certificate: Option<String>,
    private_key: Option<String>,
    auth_method: AuthMethod,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct DaConnectConfig {
    server: String,
    nodes: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
struct CollectConfig {
    interval: Option<i64>,
    ua: Option<UaCollectConfig>,
    da: Option<DaCollectConfig>,
}

#[derive(Debug, serde::Serialize)]
struct UaCollectConfig {
    nodes: Vec<UANodeConfig>,
}

#[derive(Debug, serde::Serialize)]
struct UANodeConfig {
    id: String,
    value_type: String,
}

#[derive(Debug, serde::Serialize)]
struct DaCollectConfig {
    nodes: Vec<DaNodeConfig>,
}

#[derive(Debug, serde::Serialize)]
struct DaNodeConfig {
    tag: String,
    value_type: String,
}

#[derive(Debug, serde::Serialize)]
struct ReportConfig {
    remote: String,
    concurrent: Option<i64>,
    batch_size: Option<i64>,
    batch_timeout: Option<i64>,
}

impl OPCConfig {
    fn new(mut dsn: Dsn, ipc_port: u16) -> Result<Self, OpcError> {
        debug_assert!(dsn.driver == "opc");
        macro_rules! parse_int_at {
            ($n:expr) => {
                dsn.remove($n)
                    .map(|v| {
                        v.parse::<i64>()
                            .map_err(|err| OpcError::ParseNumberError($n, v, err))
                    })
                    .transpose()?
            };
        }
        let opc_type;
        let connect;
        let collect;
        let mut param_mapping = HashMap::new();
        let mut table_info: HashMap<String, Vec<(String, String)>> = HashMap::new();
        match dsn.protocol.as_deref() {
            Some("ua") => {
                opc_type = OpcType::OPCUA;
                let addr = dsn
                    .addresses
                    .first()
                    .ok_or_else(|| OpcError::EndpointIsRequired(dsn.clone()))?;
                if addr.host.is_none() || addr.port.is_none() {
                    return Err(OpcError::EndpointIsRequired(dsn));
                }
                let endpoint = format!(
                    "opc.tcp://{}:{}/{}",
                    addr.host.as_ref().unwrap(),
                    addr.port.as_ref().unwrap(),
                    dsn.subject.as_ref().unwrap_or(&"".to_string())
                );

                let connect_timeout = parse_int_at!("connect_timeout");
                let request_timeout = parse_int_at!("request_timeout");
                let security_policy = dsn.remove("security_policy").unwrap_or("None".to_string());
                let security_mode = dsn.remove("security_mode").unwrap_or("None".to_string());

                let certificate = dsn.remove("certificate");
                let private_key = dsn.remove("private_key");

                let username = dsn.username.clone();
                let password = dsn.password.clone();

                let auth_method = if username.is_some() || password.is_some() {
                    match username.as_ref().zip(password.as_ref()) {
                        Some(_) => AuthMethod::UserName,
                        None => Err(OpcError::UserPassRequired(dsn.clone()))?,
                    }
                } else if certificate.is_some() || private_key.is_some() {
                    AuthMethod::Certificate
                } else {
                    AuthMethod::Anonymous
                };
                let connect_ua_config = UaConnectConfig {
                    endpoint,
                    connect_timeout,
                    request_timeout,
                    security_policy,
                    security_mode,
                    certificate,
                    private_key,
                    auth_method,
                    username,
                    password,
                };
                connect = ConnectConfig {
                    ua: Some(connect_ua_config),
                    da: None,
                };
                let interval = parse_int_at!("interval");
                let node_vec = get_string_vec_from_param(&mut dsn, "ua.nodes");
                // let type_vec = get_string_vec_from_param(&mut dsn, "value_type");
                let mut ua_node_config_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    assert_eq!(4, pair.len());
                    let id = String::from(pair[0]);
                    let table = String::from(pair[1]);
                    let feild = String::from(pair[2]);
                    let value_type = String::from(pair[3]);
                    let ua_node_config = UANodeConfig {
                        id: id.clone(),
                        value_type: value_type.clone(),
                    };
                    param_mapping.insert(
                        id,
                        (
                            table.clone(),
                            feild.clone(),
                            IpcDataType::from_str(value_type.to_lowercase().as_str()).unwrap(),
                        ),
                    );
                    ua_node_config_vec.push(ua_node_config);
                    process_table_info(&mut table_info, table, feild, value_type);
                }
                let collect_ua_config = UaCollectConfig {
                    nodes: ua_node_config_vec,
                };
                collect = CollectConfig {
                    interval,
                    ua: Some(collect_ua_config),
                    da: None,
                };
            }
            Some("da") => {
                opc_type = OpcType::OPCDA;
                let server = dsn
                    .addresses
                    .first()
                    .and_then(|addr| addr.host.clone())
                    .expect("should config server");
                let nodes = dsn
                    .remove("nodes")
                    .unwrap_or_default()
                    .split(",")
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect_vec();
                let connect_da_config = DaConnectConfig { server, nodes };
                connect = ConnectConfig {
                    ua: None,
                    da: Some(connect_da_config),
                };
                let interval = parse_int_at!("interval");
                let node_vec = get_string_vec_from_param(&mut dsn, "da.nodes");
                let mut da_nodes_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    let tag = String::from(pair[0]);
                    let table = String::from(pair[1]);
                    let feild = String::from(pair[2]);
                    let value_type = String::from(pair[3]);
                    da_nodes_vec.push(DaNodeConfig {
                        tag: tag.clone(),
                        value_type: value_type.clone(),
                    });
                    param_mapping.insert(
                        tag,
                        (
                            table.clone(),
                            feild.clone(),
                            IpcDataType::from_str(&value_type.to_lowercase().as_str()).unwrap(),
                        ),
                    );
                    process_table_info(&mut table_info, table, feild, value_type);
                }
                collect = CollectConfig {
                    interval,
                    ua: None,
                    da: Some(DaCollectConfig {
                        nodes: da_nodes_vec,
                    }),
                }
            }
            _ => {
                panic!()
                // bail!("opc config has wrong protocol");
            }
        }
        let remote = format!("127.0.0.1:{ipc_port}");
        let concurrent = parse_int_at!("concurrent");
        let batch_size = parse_int_at!("batch_size");
        let batch_timeout = parse_int_at!("batch_timeout");
        let report = ReportConfig {
            remote,
            concurrent,
            batch_size,
            batch_timeout,
        };
        Ok(OPCConfig {
            opc_type,
            connect,
            collect,
            report,
            param_mapping,
            table_info,
        })
    }
}

fn get_string_vec_from_param(dsn: &mut Dsn, key: &str) -> Vec<String> {
    dsn.remove(key)
        .unwrap_or_default()
        .split(",")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect_vec()
}

fn process_table_info(
    table_info: &mut HashMap<String, Vec<(String, String)>>,
    table: String,
    feild: String,
    value_type: String,
) {
    if table_info.get_mut(&table).is_none() {
        let mut t_v = Vec::new();
        t_v.push((feild, value_type));
        table_info.insert(table, t_v);
    } else {
        let t_v = table_info.get_mut(&table).unwrap();
        t_v.push((feild, value_type));
    };
}

#[instrument(skip(port_pool))]
pub async fn opc_to_taos(
    mut from: Dsn,
    actions: Vec<Action>,
    to: Dsn,
    jobs: usize,
    port_pool: &PortPool,
) -> anyhow::Result<()> {
    println!("# loading plugin: OPC");
    let target_pool = TaosBuilder::from_dsn(to)?.pool()?;
    use taos::AsyncQueryable;
    let taos = target_pool.get().await?;
    let target_pool_for_ipc = target_pool.clone();

    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for OPC connection"))?;

    let config = OPCConfig::new(from, ipc_port)?;
    dbg!(&config);
    // process table info
    for (table_name, feild_info) in &config.table_info {
        let mut sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (`ts` TIMESTAMP");
        for (feild, feild_type) in feild_info {
            sql.push_str(format!(", `{feild}` {feild_type}").as_str());
        }
        sql.push_str(")");
        log::info!("create table: {table_name}, sql: {sql}");
        taos.exec(&sql).await?;
    }

    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    log::info!("Using opc config file {} \n{}", config_path.display(), toml);

    let ipc = std::thread::spawn(move || {
        sink::listen_tcp_socket(
            target_pool_for_ipc,
            config.report.remote,
            Some(config.param_mapping.clone()),
        )
    });
    // TODO use unix socket on unix-like os
    // let ipc = if cfg!(target_os = "windows") {
    //     std::thread::spawn(move || sink::listen_tcp_socket(target_pool_for_ipc, socket))
    // } else {
    //     std::thread::spawn(move || sink::listen_unix_socket(target_pool_for_ipc, socket))
    // };
    let v = tokio::task::spawn_blocking(move || {
        #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
        let mut command =
            std::process::Command::new("C:\\TDengine\\xplugins\\opc-collector_windows_amd64.exe");
        #[cfg(all(target_os = "windows", target_arch = "x86"))]
        let mut command =
            std::process::Command::new("C:\\TDengine\\xplugins\\opc-collector_windows_386.exe");
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        let mut command =
            std::process::Command::new("/usr/local/taos/xplugins/opc-collector_linux_amd64");
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        let mut command =
            std::process::Command::new("/usr/local/taos/xplugins/opc-collector_linux_arm64");
        #[cfg(all(target_os = "linux", target_arch = "arm"))]
        let mut command =
            std::process::Command::new("/usr/local/taos/xplugins/opc-collector_linux_arm");
        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        let mut command =
            std::process::Command::new("/usr/local/taos/xplugins/opc-collector_darwin_amd64");
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        let mut command =
            std::process::Command::new("/usr/local/taos/xplugins/opc-collector_darwin_arm64");
        command
            .arg("collect")
            .arg(format!("--conf={}", &config_path.display()))
            .output()
    });

    tokio::select! {
        output = v => {
            let output = output??;
            // dbg!(output);
            log::info!("OPC exit with status {}", output.status);
            // server.abort();
            panic!();
        },
        _ = tokio::signal::ctrl_c() => {
            log::info!("Ctrl+C triggered, cancel tasks");
            // panic!();
        }
    };

    stop_thread(ipc);
    temp_path.close()?;
    log::info!("OPC to taos task done");
    Ok(())
}

#[tokio::test]
async fn opc_config_to_toml() -> anyhow::Result<()> {
    let mut map = HashMap::new();
    map.insert(
        String::from("123"),
        (
            String::from("meter"),
            String::from("cu"),
            IpcDataType::Float32,
        ),
    );
    let config = OPCConfig {
        opc_type: OpcType::OPCUA,
        connect: ConnectConfig {
            ua: Some(UaConnectConfig {
                endpoint: String::from("endpoint.123"),
                connect_timeout: Some(10),
                request_timeout: Some(20),
                security_policy: String::from("None"),
                security_mode: String::from("None"),
                certificate: None,
                private_key: None,
                auth_method: AuthMethod::Anonymous,
                username: None,
                password: None,
            }),
            da: Some(DaConnectConfig {
                server: String::from("server.server"),
                nodes: vec![String::from("localhost")],
            }),
        },
        collect: CollectConfig {
            interval: Some(10),
            ua: Some(UaCollectConfig {
                nodes: vec![UANodeConfig {
                    id: String::from("1"),
                    value_type: String::from("DOUBLE"),
                }],
            }),
            da: Some(DaCollectConfig {
                nodes: vec![DaNodeConfig {
                    tag: String::from("123"),
                    value_type: String::from("VARCHAR"),
                }],
            }),
        },
        report: ReportConfig {
            remote: String::from("remote.remote"),
            concurrent: Some(10),
            batch_size: None,
            batch_timeout: Some(100),
        },
        param_mapping: map,
        table_info: HashMap::new(),
    };
    let toml = toml::to_string(&config)?;
    println!("toml:[{}]", toml);
    Ok(())
}
