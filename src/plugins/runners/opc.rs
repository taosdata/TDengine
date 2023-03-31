use std::{
    collections::HashMap,
    io::prelude::*,
    num::ParseIntError,
    str::FromStr, 
};

use itertools::Itertools;
use taos::{AsyncTBuilder, Dsn, TaosBuilder, taos_query::helpers::ColumnMeta, IntoDsn};
use taosx_ipc::prelude::IpcDataType;
use tracing::instrument;

use crate::{
    plugins::sink,
    utils::{port_pool::PortPool, stop_thread},
    Action, DataSet,
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
    #[error("config file not found: {0}")]
    FileNotFound(String),
    #[error("table cloumn not match in {0}")]
    CloumnNotMatch(String),
    #[error("config file content is empty in {0}")]
    EmptyConfig(String),
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
    tags: Vec<DaNodeConfig>,
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
                let node_vec = get_string_vec_from_param_or_file(&mut dsn, "ua.nodes")?;
                let mut ua_node_config_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    assert_eq!(4, pair.len());
                    let id = String::from(pair[0]);
                    let table = String::from(pair[1]);
                    let field = String::from(pair[2]);
                    let value_type = String::from(pair[3]);
                    let ua_node_config = UANodeConfig {
                        id: id.clone(),
                        value_type: value_type.clone(),
                    };
                    param_mapping.insert(
                        id,
                        (
                            table.clone(),
                            field.clone(),
                            IpcDataType::from_str(value_type.to_lowercase().as_str()).unwrap(),
                        ),
                    );
                    ua_node_config_vec.push(ua_node_config);
                    process_table_info(&mut table_info, table, field, value_type);
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
                let node_vec = get_string_vec_from_param_or_file(&mut dsn, "da.tags")?;
                let mut da_nodes_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    let tag = String::from(pair[0]);
                    let table = String::from(pair[1]);
                    let field = String::from(pair[2]);
                    let value_type = String::from(pair[3]);
                    da_nodes_vec.push(DaNodeConfig {
                        tag: tag.clone(),
                        value_type: value_type.clone(),
                    });
                    param_mapping.insert(
                        tag,
                        (
                            table.clone(),
                            field.clone(),
                            IpcDataType::from_str(&value_type.to_lowercase().as_str()).unwrap(),
                        ),
                    );
                    process_table_info(&mut table_info, table, field, value_type);
                }
                collect = CollectConfig {
                    interval,
                    ua: None,
                    da: Some(DaCollectConfig {
                        tags: da_nodes_vec,
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

fn get_string_vec_from_param_or_file(dsn: &mut Dsn, key: &str) -> Result<Vec<String>, OpcError> {
    if let Some(nodes) = dsn.remove(key) {
        let (files, mut node_config): (Vec<_>, Vec<_>) = nodes
            .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .partition(|v| v.starts_with("@"));
        for file in files {
            let f = std::fs::File::open(&file[1..]);
            if f.is_err() {
                log::warn!("file: {} read error", file);
                return Err(OpcError::FileNotFound(file));
            }
            let buf = std::io::BufReader::new(f.unwrap());
            let mut file_data = buf.lines().collect_vec();
            // remove header
            if file_data.remove(0).is_err() {
                log::warn!("file: {} content length < 1", file);
            }
            
            node_config.extend(file_data.iter().filter_map(|r| r.as_ref().ok()).map(|s| s.replace(",", "::")));

        }
        if node_config.len() == 0 {
            log::warn!("node config is empty");
            return Err(OpcError::EmptyConfig(nodes));
        }
        return Result::Ok(node_config);
    }
    log::warn!("node config is empty");
    return Err(OpcError::EmptyConfig(String::new()));
}

fn process_table_info(
    table_info: &mut HashMap<String, Vec<(String, String)>>,
    table: String,
    field: String,
    value_type: String,
) {
    if table_info.get_mut(&table).is_none() {
        let mut t_v = Vec::new();
        t_v.push((field, value_type));
        table_info.insert(table, t_v);
    } else {
        let t_v = table_info.get_mut(&table).unwrap();
        t_v.push((field, value_type));
    };
}

#[instrument(skip(port_pool))]
pub async fn opc_to_taos(
    from: Dsn,
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
    let mut ts_cloumn_name_map = HashMap::new();
    for (table_name, field_info) in &config.table_info {
        let res = taos.describe(&table_name).await;
        if res.is_err() {
            // table not exists, will create normal table
            let mut sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (`ts` TIMESTAMP");
            log::info!("table {table_name} use `ts` as ts column");
            ts_cloumn_name_map.insert(table_name.clone(), String::from("ts"));
            for (field, field_type) in field_info {
                sql.push_str(format!(", `{field}` {field_type}").as_str());
            }
            sql.push_str(")");
            log::info!("create normal table: {table_name}, sql: {sql}");
            taos.exec(&sql).await?;
        } else {
            // table exists and check normal table or child table
            let desc = res.unwrap();
            let mut field_map = HashMap::new();
                desc.iter().for_each(|c| {
                    match c {
                        ColumnMeta::Column(d) => {
                            field_map.insert(d.field.clone(), d.ty.to_string());
                        },
                        _ => (),
                    }
                });
            // insert ts column
            log::info!("table {table_name} use `{}` as ts column", desc[0].field);
            ts_cloumn_name_map.insert(table_name.clone(), desc[0].field.clone());
            if desc.is_stable() {
                // child table.
                for (field, field_type) in field_info {
                    let field_get = field_map.get(field);
                    if field_get.is_none() {
                        anyhow::bail!("field: {} not found in table: {}", field, table_name);
                    }
                    if !check_field_type(field_get.unwrap(), field_type.to_ascii_lowercase()) {
                        anyhow::bail!("field: {} type: {} not match in child table: {} which type is {}", field, field_type,table_name, field_get.unwrap());
                    }
                    
                }
            } else {
                // ordinary table.
                let mut columns_to_add = HashMap::new();
                for (field, field_type) in field_info {
                    let field_get = field_map.get(field); 
                    if field_get.is_none() {
                        // column not exists and alter table
                        columns_to_add.insert(field, field_type);
                    } else if !check_field_type(field_get.unwrap(), field_type.to_ascii_lowercase()) {
                        anyhow::bail!("field: {} type: {} not match in normal table: {} which type is {} ", field, field_type, table_name, field_get.unwrap());
                    }
                }
                if columns_to_add.len() > 0 {
                    for (field_name, field_type) in columns_to_add {
                        let alter_sql = format!("ALTER TABLE {table_name} ADD COLUMN `{field_name}` {field_type}");
                        log::info!("alter table sql: {}", alter_sql);
                        taos.exec(alter_sql).await?;
                    }
                }
            }
        }
        
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
            Some(OpcTableConfig {
                    table_info: config.param_mapping.clone(),
                    ts_cloumn_name: ts_cloumn_name_map.clone(),
                }
                ),
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
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
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

/// check field type 
/// return true if matched 
/// if type equals return true else false
/// if type is binary|varchar|nchar check whether type configed contains those characters
fn check_field_type(field_type_config: &String, field_type: String) -> bool {
    let field_type_config = field_type_config.to_ascii_lowercase();
    if field_type.contains("binary") || field_type.contains("varchar") || field_type.contains("nchar") {
        field_type_config.contains("binary") || field_type_config.contains("varchar") || field_type_config.contains("nchar")
    } else if field_type_config == field_type {
        true
    } else {
        false
    }
}

#[derive(Clone)]
pub struct OpcTableConfig {
    /// table_info: table_name, Vec<(field, type)>
    pub(crate) table_info: HashMap<String, (String, String, IpcDataType)>,

    /// table_name, ts column
    pub(crate) ts_cloumn_name: HashMap<String, String>,
}

pub async fn ops_datasets(from: &Dsn) -> anyhow::Result<Vec<DataSet>> {
    let config = OPCConfig::new(from.clone(), 0)?;
    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    log::info!("Using opc config file {} \n{}", config_path.display(), toml);

    // TODO use unix socket on unix-like os
    // let ipc = if cfg!(target_os = "windows") {
    //     std::thread::spawn(move || sink::listen_tcp_socket(target_pool_for_ipc, socket))
    // } else {
    //     std::thread::spawn(move || sink::listen_unix_socket(target_pool_for_ipc, socket))
    // };
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
    let output = command
        .arg("points")
        .arg(format!("--conf={}", &config_path.display()))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()?;
    // dbg!(output);
    log::info!("OPC exit with status {}", output.status);

    // let json = String::from_utf8_lossy(&output.stdout);
    let res: Vec<DataSet> = serde_json::from_slice(&output.stdout)?;
    temp_path.close()?;
    Ok(res)
}

#[tokio::test]
async fn test_opc_config_to_toml() -> anyhow::Result<()> {
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
                tags: vec![DaNodeConfig {
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
    assert_eq!(r#"opc_type = "opcua"

[connect.ua]
endpoint = "endpoint.123"
connect_timeout = 10
request_timeout = 20
security_policy = "None"
security_mode = "None"
auth_method = "Anonymous"

[connect.da]
server = "server.server"
nodes = ["localhost"]

[collect]
interval = 10

[[collect.ua.nodes]]
id = "1"
value_type = "DOUBLE"

[[collect.da.nodes]]
tag = "123"
value_type = "VARCHAR"

[report]
remote = "remote.remote"
concurrent = 10
batch_timeout = 100
"#, toml);
    Ok(())
}

#[tokio::test]
async fn test_get_string_vec_from_param_or_file() -> anyhow::Result<()> { 
    let mut dsn = "opc+ua://Win10-2021XIVKQ:53530/OPCUA/SimulationServer?ua.nodes=ns=3;i=1004::ntb1::c0::double,ns=3;i=1008::ntb1::c1::double".into_dsn()?;
    let vec_string = get_string_vec_from_param_or_file(&mut dsn, "ua.nodes")?;
    assert_eq!(vec_string, vec![String::from("ns=3;i=1004::ntb1::c0::double"), String::from("ns=3;i=1008::ntb1::c1::double")]);
    let mut dsn = "opc+ua://Win10-2021XIVKQ:53530/OPCUA/SimulationServer?ua.nodes=ns=3;i=1004::ntb1::c0::double,ns=3;i=1008::ntb1::c1::double,@/Users/zmlgirl/Downloads/test_opc.csv".into_dsn()?;
    let vec_string = get_string_vec_from_param_or_file(&mut dsn, "ua.nodes")?;
    assert_eq!(vec_string, vec![String::from("ns=3;i=1004::ntb1::c0::double"), String::from("ns=3;i=1008::ntb1::c1::double"), String::from("ns=2;i=2::ntb2::c1::double"), String::from("ns=2;i=3::ntb3::c2::int")]);
    Ok(())
}