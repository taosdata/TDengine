use std::{
    io::prelude::*,
    num::ParseIntError,
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
    time::Duration, str::FromStr, collections::HashMap,
};

use anyhow::bail;
use itertools::Itertools;
use taos::{Dsn, TBuilder, TaosBuilder};
use taosx_ipc::prelude::IpcDataType;

use crate::{plugins::service::spawn_rest_service, utils::port_pool::PortPool, Action};

mod config;
mod service;
mod sink;
mod source;
mod transform;

#[derive(Debug, serde::Serialize)]
struct PiConfig {
    // system
    #[serde(rename = "PIServerName")]
    server_name: String,
    #[serde(rename = "PISystemName")]
    system_name: String,
    #[serde(rename = "AFDatabaseName")]
    database: String,
    #[serde(rename = "PIDataPipesInstances")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pi_data_pipes_instances: Option<u32>,
    #[serde(rename = "AFDataPipesInstances")]
    #[serde(skip_serializing_if = "Option::is_none")]
    af_data_pipes_instances: Option<u32>,
    // runtime
    #[serde(rename = "MaxWaitLen")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_wait_len: Option<u32>,
    #[serde(rename = "UpdateInterval")]
    #[serde(skip_serializing_if = "Option::is_none")]
    update_interval: Option<u32>,
    #[serde(rename = "MaxBackfillRangeDays")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_backfill_range_days: Option<u32>,

    #[serde(rename = "IPCStream")]
    ipc_stream: String,
    #[serde(rename = "SQLAPI")]
    sql_api: String,
    // data set
    #[serde(rename = "TemplateForPIPoint")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_pi_point: Vec<String>,
    #[serde(rename = "TemplateForAFElement")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_af_element: Vec<String>,
    #[serde(rename = "Points")]
    #[serde(skip_serializing_if = "Option::is_none")]
    points: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum PiError {
    #[error("Server is required in PI dsn: {0}")]
    ServerIsRequired(Dsn),
    #[error("Database name is required in PI dsn: {0}")]
    DatabaseIsRequired(Dsn),
    #[error("Parse integer error from {1} while parsing parameter {0}: {2:?}")]
    ParseNumberError(&'static str, String, ParseIntError),
}

impl PiConfig {
    pub fn new(mut dsn: Dsn, ipc: u16, sql: u16) -> Result<Self, PiError> {
        debug_assert!(dsn.driver == "pi");
        let server_name = dsn
            .addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or_else(|| PiError::ServerIsRequired(dsn.clone()))?;
        let system_name = dsn
            .remove("PISystemName")
            .unwrap_or_else(|| server_name.clone());
        let database = dsn
            .subject
            .clone()
            .ok_or_else(|| PiError::DatabaseIsRequired(dsn.clone()))?;

        macro_rules! parse_int_at {
            ($n:expr) => {
                dsn.remove($n)
                    .map(|v| {
                        v.parse::<u32>()
                            .map_err(|err| PiError::ParseNumberError($n, v, err))
                    })
                    .transpose()?
            };
        }
        let pi_data_pipes_instances = parse_int_at!("PIDataPipesInstances");
        let af_data_pipes_instances = parse_int_at!("AFDataPipesInstances");
        let max_wait_len = parse_int_at!("MaxWaitLen");
        let update_interval = parse_int_at!("UpdateInterval");
        let max_backfill_range_days = parse_int_at!("MaxBackfillRangeDays");

        let template_for_pi_point = dsn
            .remove("TemplateForPIPoint")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec();
        let template_for_af_element = dsn
            .remove("TemplateForAFElement")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec();
        let points = dsn.remove("Points").map(|s| Path::new(&s).to_path_buf());

        let ipc_stream = format!("127.0.0.1:{ipc}");
        let sql_api = format!("127.0.0.1:{sql}");

        // dsn.addresses
        Ok(Self {
            server_name,
            system_name,
            database,
            pi_data_pipes_instances,
            af_data_pipes_instances,
            max_wait_len,
            update_interval,
            max_backfill_range_days,
            ipc_stream,
            sql_api,
            template_for_pi_point,
            template_for_af_element,
            points,
        })
    }
}

/// PI DSN example: "pi://WIN-2OA23UM12TN/Met1?PISystemName=other&points=@<file>"
pub async fn pi_to_taos(
    mut from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
    port_pool: &PortPool,
) -> anyhow::Result<()> {
    println!("# loading plugin: PI");
    #[cfg(not(target_os = "windows"))]
    {
        anyhow::bail!("PI connector support only windows platform");
    }

    let target_pool = TaosBuilder::from_dsn(to)?.pool()?;

    let taos = target_pool.get_timeout(Duration::from_secs(5))?;

    let target_pool_for_ipc = target_pool.clone();

    let ipc = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;
    let sql = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;

    let config = PiConfig::new(from, ipc, sql)?;

    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    log::info!("Using config file {} \n{}", config_path.display(), toml);

    let server = spawn_rest_service(target_pool, 6052).await?;

    let ipc =
        std::thread::spawn(move || sink::listen_tcp_socket(target_pool_for_ipc, config.ipc_stream, None));

    // let ipc = ;
    // let ipc = tokio::spawn(future);

    let v = tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(
            "C:\\Program Files (x86)\\TD PI Connector\\TDPIConnector.Service.exe",
            // "target/debug/examples/pi",
        );
        command
            .arg("-f")
            .arg(&config_path)
            // .stdout(Stdio::piped())
            // .stderr(Stdio::piped())
            .output()
    });

    tokio::select! {
        output = v => {
            let output = output??;
            // dbg!(output);
            log::info!("PI exit with status {}", output.status);
            // server.abort();
            panic!();
        },
        _ = server => {
            panic!();
        }
        _ = tokio::signal::ctrl_c() => {
            log::info!("Ctrl+C triggered, cancel tasks");
            // panic!();
        }
    };

    stop_thread(ipc);
    temp_path.close()?;
    // rt.handle();
    // (&unsafe { *Arc::into_raw(rt) }).shutdown_background();
    log::info!("Done");
    // server.abort();
    Ok(())
}

fn stop_thread<T>(handle: JoinHandle<T>) {
    #[cfg(windows)]
    unsafe {
        use std::os::windows::io::IntoRawHandle;
        use winapi::ctypes::c_void as winapi_c_void;
        use winapi::um::processthreadsapi::TerminateThread;

        let raw_handle = handle.into_raw_handle();
        TerminateThread(raw_handle as *mut winapi_c_void, 0);
    }
    #[cfg(unix)]
    unsafe {
        use libc::pthread_kill;
        use std::os::unix::thread::JoinHandleExt;

        let raw_handle = handle.into_pthread_t();
        pthread_kill(raw_handle, 2);
    };
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum OPCType {
    OPCUA,
    OPCDA
}

// impl OPCType {
//     pub fn as_str(&self) -> &'static str {
//         match self {
//             OPCType::OPCUA => "opcua",
//             OPCType::OPCDA => "opcda",
//         }
//     }
// }

impl FromStr for OPCType {
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
enum OPCError {
    #[error("endpoint is required in OPC dsn: {0}")]
    EndpointIsRequired(Dsn),
    #[error("Database name is required in OPC dsn: {0}")]
    DatabaseIsRequired(Dsn),
    #[error("Parse integer error from {1} while parsing parameter {0}: {2:?}")]
    ParseNumberError(&'static str, String, ParseIntError),
}

#[derive(Debug, serde::Serialize)]
struct OPCConfig {
    opc_type: OPCType,
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
    ua: Option<UAConnectConfig>,
    da: Option<DAConnectConfig>,
}

#[derive(Debug, serde::Serialize)]
struct UAConnectConfig {
    endpoint: String,
    connect_timeout: Option<i64>,
    request_timeout: Option<i64>,
    security_policy: Option<String>,
    security_mode: Option<String>,
    certificate: Option<String>,
    private_key: Option<String>,
    auth_method: String,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct DAConnectConfig {
    server: String,
    nodes: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
struct CollectConfig {
    interval: Option<i64>,
    ua: Option<UACollectConfig>,
    da: Option<DACollectConfig>,
}

#[derive(Debug, serde::Serialize)]
struct UACollectConfig {
    nodes: Vec<UANodeConfig>,
}

#[derive(Debug, serde::Serialize)]
struct UANodeConfig {
    id: String,
    value_type: String,
}

#[derive(Debug, serde::Serialize)]
struct DACollectConfig {
    nodes: Vec<DANodeConfig>
}

#[derive(Debug, serde::Serialize)]
struct DANodeConfig {
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
    fn new(mut dsn: Dsn, ipc_port: u16) -> Result<Self, OPCError> {
        debug_assert!(dsn.driver == "opc");
        macro_rules! parse_int_at {
            ($n:expr) => {
                dsn.remove($n)
                    .map(|v| {
                        v.parse::<i64>()
                            .map_err(|err| OPCError::ParseNumberError($n, v, err))
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
                opc_type = OPCType::OPCUA;
                let endpoint = dsn.addresses.first()
                .and_then(|addr| addr.host.clone()).expect("should config endpoint");

                let connect_timeout = parse_int_at!("connect_timeout");
                let request_timeout = parse_int_at!("request_timeout");
                let security_policy = dsn.remove("security_policy");
                let security_mode = dsn.remove("security_mode");
                let certificate = dsn.remove("certificate");
                let private_key = dsn.remove("private_key");
                let auth_method = dsn.remove("auth_method").expect("should config auth_method");
                let username = dsn.username.clone();
                let password = dsn.password.clone();
                let connect_ua_config = UAConnectConfig { 
                    endpoint, 
                    connect_timeout, 
                    request_timeout, 
                    security_policy, 
                    security_mode, 
                    certificate, 
                    private_key, 
                    auth_method, 
                    username, 
                    password 
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
                        id ,
                        value_type,
                    };
                    param_mapping.insert(id, (table, feild, IpcDataType::from_str(value_type.to_lowercase().as_str()).unwrap()));
                    ua_node_config_vec.push(ua_node_config);
                    process_table_info(&mut table_info, table, feild, value_type);
                }
                let collect_ua_config = UACollectConfig {
                    nodes: ua_node_config_vec,
                };
                collect = CollectConfig {
                    interval,
                    ua: Some(collect_ua_config),
                    da: None,
                };

            },
            Some("da") => {
                opc_type = OPCType::OPCDA;
                let server = dsn.addresses.first().and_then(|addr| addr.host.clone()).expect("should config server");
                let nodes = dsn.remove("nodes").unwrap_or_default()
                            .split(",").map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .collect_vec();
                let connect_da_config = DAConnectConfig {
                    server,
                    nodes,
                };
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
                    da_nodes_vec.push(DANodeConfig{
                        tag,
                        value_type,
                    });
                    param_mapping.insert(tag, (table, feild, IpcDataType::from_str(&value_type.to_lowercase().as_str()).unwrap()));
                    process_table_info(&mut table_info, table, feild, value_type);
                }
                collect = CollectConfig {
                    interval,
                    ua: None,
                    da: Some(DACollectConfig { nodes: da_nodes_vec }),
                }
            },
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
    dsn.remove(key).unwrap_or_default()
        .split(",").map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect_vec()
}

fn process_table_info(table_info: &mut HashMap<String, Vec<(String, String)>>, table: String
    , feild: String, value_type: String) {
    let mut table_vec = if table_info.get_mut(&table).is_none() {
        let mut t_v = Vec::new();
        table_info.insert(table, t_v);
        &mut t_v
    } else {
        table_info.get_mut(&table).unwrap()
    };
    table_vec.push((feild, value_type));
}

pub async fn opc_to_taos(mut from: Dsn, actions: Vec<Action>, to: Dsn, jobs: usize, port_pool: &PortPool) -> anyhow::Result<()> {
    println!("# loading plugin: OPC");
    let target_pool = TaosBuilder::from_dsn(to)?.pool()?;
    use taos::AsyncQueryable;
    let taos = target_pool.get_timeout(Duration::from_secs(5))?;
    let target_pool_for_ipc = target_pool.clone();

    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for OPC connection"))?;

    let config = OPCConfig::new(from, ipc_port)?;
    // process table info
    for (table_name, feild_info) in &config.table_info {
        let mut sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (`ts` TIMESTAMP");
        for (feild, feild_type) in feild_info {
            sql.push_str(format!(" `{feild}` {feild_type}").as_str());
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

    let ipc = std::thread::spawn(move || sink::listen_tcp_socket(target_pool_for_ipc, config.report.remote, Some(&config.param_mapping)));
    // TODO use unix socket on unix-like os
    // let ipc = if cfg!(target_os = "windows") {
    //     std::thread::spawn(move || sink::listen_tcp_socket(target_pool_for_ipc, socket))
    // } else {
    //     std::thread::spawn(move || sink::listen_unix_socket(target_pool_for_ipc, socket))
    // };
    let v = tokio::task::spawn_blocking(move || {
        #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
        let mut command = std::process::Command::new(
            "C:\\TDengine\\xplugins\\opc-collector_windows_amd64.exe",
        );
        #[cfg(all(target_os = "windows", target_arch = "x86"))]
        let mut command = std::process::Command::new(
            "C:\\TDengine\\xplugins\\opc-collector_windows_386.exe",
        );
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        let mut command = std::process::Command::new(
            "/usr/local/taos/xplugins/opc-collector_linux_amd64",
        );
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        let mut command = std::process::Command::new(
            "/usr/local/taos/xplugins/opc-collector_linux_arm64",
        );
        #[cfg(all(target_os = "linux", target_arch = "arm"))]
        let mut command = std::process::Command::new(
            "/usr/local/taos/xplugins/opc-collector_linux_arm",
        );
        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        let mut command = std::process::Command::new(
            "/usr/local/taos/xplugins/opc-collector_darwin_amd64",
        );
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        let mut command = std::process::Command::new(
            "/usr/local/taos/xplugins/opc-collector_darwin_arm64",
        );
        command
            .arg("-f")
            .arg(&config_path)
            .output()
    });

    tokio::select! {
        output = v => {
            let output = output??;
            // dbg!(output);
            log::info!("PI exit with status {}", output.status);
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
async fn opc_config_to_toml() -> anyhow::Result<()>{
    let mut map = HashMap::new();
    map.insert(String::from("123"), (String::from("meter"), String::from("cu"), IpcDataType::Float32));
    let config = OPCConfig {
        opc_type: OPCType::OPCUA,
        connect: ConnectConfig { 
            ua: Some(UAConnectConfig { 
                endpoint: String::from("endpoint.123"), 
                connect_timeout: Some(10), 
                request_timeout: Some(20), 
                security_policy: Some(String::from("None")), 
                security_mode: Some(String::from("None")), 
                certificate: None, 
                private_key: None, 
                auth_method: String::from("Anonymous"), 
                username: None, 
                password: None }), 
        da: Some(DAConnectConfig { 
            server: String::from("server.server"), 
            nodes: vec![String::from("localhost")] }) },
        collect: CollectConfig { interval: Some(10), 
            ua: Some(UACollectConfig {
                nodes: vec![UANodeConfig{
                    id: String::from("1"),
                    value_type: String::from("DOUBLE"),
                }],
            }), 
            da: Some(DACollectConfig {
                nodes: vec![DANodeConfig{
                    tag: String::from("123"),
                    value_type: String::from("VARCHAR"),
                }]
            }) },
        report: ReportConfig { remote: String::from("remote.remote"), 
        concurrent: Some(10), 
        batch_size: None,
        batch_timeout: Some(100), },
        param_mapping: map,
        table_info: map,
    };
    let toml = toml::to_string(&config)?;
    println!("toml:[{}]", toml);
    Ok(())
}