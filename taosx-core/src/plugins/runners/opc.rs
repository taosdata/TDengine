use std::{
    collections::HashMap, io::prelude::*, num::ParseIntError, path::PathBuf, str::FromStr,
    sync::Arc, time::Duration,
};

use anyhow::Context;
use itertools::Itertools;
use taos::{
    taos_query::helpers::ColumnMeta, AsyncQueryable, AsyncTBuilder, Dsn, Taos, TaosBuilder, Ty,
};
use taosx_ipc::{prelude::IpcDataType, types::OptionSet};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{plugins::sink, utils::port_pool::PortPool, Action, DataSet, DataSetsReq, Transferred};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum OpcType {
    OPCUA,
    OPCDA,
    FAKE,
}

impl FromStr for OpcType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            "fake" => Ok(Self::FAKE),
            _ => Err(s.to_string()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum OpcError {
    #[error("One of `ua` `da` protocol should be set")]
    ProtocolNotFound(Dsn),
    #[error("Endpoint is required in OPC dsn: {0} like `opc+..://localhost:4840?...`")]
    EndpointIsRequired(Dsn),
    #[error("Database name is required in OPC dsn: {0}")]
    DatabaseIsRequired(Dsn),
    #[error("Username and password are both required for UserName authentication method in {0}")]
    UserPassRequired(Dsn),
    #[error("config file not found: {0}")]
    FileNotFound(String),
    #[error("file parse error: {0}")]
    FileParseFound(String),
    #[error("config file content is empty in {0}")]
    EmptyConfig(String),
    #[error("node config length is not 4, length is {0}")]
    NodeConfig(String),
    #[error("Parse integer error from {1} while parsing parameter {0}: {2:?}")]
    ParseNumberError(&'static str, String, ParseIntError),
    #[error("Parse param error from {1} while parsing parameter {0}")]
    ParseError(&'static str, String),
}

#[derive(Debug, serde::Serialize)]
pub struct OPCConfig {
    opc_type: OpcType,
    debug: bool,
    #[serde(skip)]
    /// use receviced time as ts cloumn value when config true
    use_received_time: bool,
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
    limit: Option<i64>,
    ua: Option<UaCollectConfig>,
    da: Option<DaCollectConfig>,
}

#[derive(Debug, serde::Serialize)]
struct UaCollectConfig {
    collect_mode: CollectMode,
    nodes: Vec<UANodeConfig>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum CollectMode {
    OBSERVE,
    SUBSCRIBE,
}

impl FromStr for CollectMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "observe" => Ok(Self::OBSERVE),
            "subscribe" => Ok(Self::SUBSCRIBE),
            _ => Err(s.to_string()),
        }
    }
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

/// OPC connector mode
enum OPCConfigMode {
    /// just get points
    Points,
    /// collect point data
    Collect,
}

impl OPCConfig {
    fn new(mut dsn: Dsn, ipc_port: u16, config_mode: OPCConfigMode) -> Result<Self, OpcError> {
        debug_assert!(dsn.driver == "opc" || dsn.driver == "opcua" || dsn.driver == "opcda");
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
        let mut opc_type;
        let connect;
        let collect;
        let mut param_mapping = HashMap::new();
        let mut table_info: HashMap<String, Vec<(String, String)>> = HashMap::new();
        match dsn.driver.as_str() {
            "opc" => {
                if dsn.protocol.is_none() {
                    return Err(OpcError::ProtocolNotFound(dsn));
                }
            }
            "opcua" => {
                dsn.protocol.replace("ua".to_string());
            }
            "opcda" => {
                dsn.protocol.replace("da".to_string());
            }
            _ => unreachable!(),
        }
        let interval = parse_int_at!("interval");
        let limit = parse_int_at!("limit");
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

                let node_vec: Vec<String> = if let OPCConfigMode::Points = config_mode {
                    vec![]
                } else {
                    get_string_vec_from_param_or_file(&mut dsn, "ua.nodes")
                        .map_err(|s| OpcError::FileParseFound(s))?
                };
                let mut ua_node_config_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    if pair.len() != 4 {
                        return Err(OpcError::NodeConfig(pair.len().to_string()));
                    }
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
                let collect_mode = dsn.remove("collect_mode").unwrap_or("observe".to_string());
                let collect_ua_config = UaCollectConfig {
                    collect_mode: collect_mode
                        .parse::<CollectMode>()
                        .map_err(|err| OpcError::ParseError("collect_mode", err))?,
                    nodes: ua_node_config_vec,
                };
                collect = CollectConfig {
                    interval,
                    limit,
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
                let node_vec: Vec<String> = if let OPCConfigMode::Points = config_mode {
                    vec![]
                } else {
                    get_string_vec_from_param_or_file(&mut dsn, "da.tags")
                        .map_err(|s| OpcError::FileParseFound(s))?
                };
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
                    limit,
                    ua: None,
                    da: Some(DaCollectConfig { tags: da_nodes_vec }),
                }
            }
            _ => {
                panic!()
                // bail!("opc config has wrong protocol");
            }
        }
        if dsn.remove("fake").is_some() {
            opc_type = OpcType::FAKE;
        }
        let remote = format!("127.0.0.1:{ipc_port}");
        let concurrent = parse_int_at!("concurrent");
        let batch_size = parse_int_at!("batch_size");
        let batch_timeout = parse_int_at!("batch_timeout");
        let debug = if let Some(v) = dsn
            .remove("debug")
            .map(|v| {
                v.parse::<bool>()
                    .map_err(|err| OpcError::ParseError("debug", v))
            })
            .transpose()?
        {
            v
        } else {
            false
        };
        let use_received_time = if let Some(v) = dsn
            .remove("use_received_time")
            .map(|v| {
                v.parse::<bool>()
                    .map_err(|err| OpcError::ParseError("use_received_time", v))
            })
            .transpose()?
        {
            v
        } else {
            false
        };

        let report = ReportConfig {
            remote,
            concurrent,
            batch_size,
            batch_timeout,
        };
        Ok(OPCConfig {
            opc_type,
            debug,
            use_received_time,
            connect,
            collect,
            report,
            param_mapping,
            table_info,
        })
    }

    pub async fn parse_tables_with(&self, taos: &Taos) -> anyhow::Result<OpcTableConfig> {
        let mut ts_cloumn_name_map = HashMap::new();
        for (table_name, field_info) in &self.table_info {
            let res = taos.describe(&table_name).await;
            if res.is_err() {
                // table not exists, will create normal table
                let mut sql = if self.use_received_time {
                    ts_cloumn_name_map.insert(
                        table_name.clone(),
                        (
                            String::from(DEFAULT_TS_COLUMN_NAME),
                            Some(String::from(DEFAULT_SERVER_TS_COLUMN_NAME)),
                        ),
                    );
                    log::info!("table {table_name} use `ts` as ts column, use `server_ts` as second ts column");
                    format!("CREATE TABLE IF NOT EXISTS {table_name} (`{DEFAULT_TS_COLUMN_NAME}` TIMESTAMP, {DEFAULT_SERVER_TS_COLUMN_NAME} TIMESTAMP ")
                } else {
                    ts_cloumn_name_map.insert(
                        table_name.clone(),
                        (String::from(DEFAULT_TS_COLUMN_NAME), None),
                    );
                    log::info!("table {table_name} use `ts` as ts column");
                    format!("CREATE TABLE IF NOT EXISTS {table_name} (`{DEFAULT_TS_COLUMN_NAME}` TIMESTAMP")
                };
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
                desc.iter().for_each(|c: &ColumnMeta| match c {
                    ColumnMeta::Column(d) => {
                        field_map.insert(d.field.clone(), d.ty.to_string());
                    }
                    _ => (),
                });
                // insert ts column
                if self.use_received_time {
                    if desc.len() < 2 || desc[1].ty != Ty::Timestamp {
                        anyhow::bail!("table: {} column type not match[len < 2 or second column is not timestamp]", table_name);
                    }
                    log::info!(
                        "table {table_name} use `{}` as ts column, use `{}` as sever ts column",
                        desc[0].field,
                        desc[1].field
                    );
                    ts_cloumn_name_map.insert(
                        table_name.clone(),
                        (desc[0].field.clone(), Some(desc[1].field.clone())),
                    );
                } else {
                    log::info!("table {table_name} use `{}` as ts column", desc[0].field);
                    ts_cloumn_name_map.insert(table_name.clone(), (desc[0].field.clone(), None));
                }
                if desc.is_stable() {
                    // child table.
                    for (field, field_type) in field_info {
                        let field_get = field_map.get(field);
                        if field_get.is_none() {
                            anyhow::bail!("field: {} not found in table: {}", field, table_name);
                        }
                        if !check_field_type(field_get.unwrap(), field_type.to_ascii_lowercase()) {
                            anyhow::bail!(
                                "field: {} type: {} not match in child table: {} which type is {}",
                                field,
                                field_type,
                                table_name,
                                field_get.unwrap()
                            );
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
                        } else if !check_field_type(
                            field_get.unwrap(),
                            field_type.to_ascii_lowercase(),
                        ) {
                            anyhow::bail!(
                                "field: {} type: {} not match in normal table: {} which type is {} ",
                                field,
                                field_type,
                                table_name,
                                field_get.unwrap()
                            );
                        }
                    }
                    if columns_to_add.len() > 0 {
                        for (field_name, field_type) in columns_to_add {
                            let alter_sql = format!(
                                "ALTER TABLE {table_name} ADD COLUMN `{field_name}` {field_type}"
                            );
                            log::info!("alter table sql: {}", alter_sql);
                            taos.exec(alter_sql).await?;
                        }
                    }
                }
            }
        }
        let c = OpcTableConfig {
            table_info: self.param_mapping.clone(),
            ts_cloumn_name: ts_cloumn_name_map.clone(),
            use_received_time: self.use_received_time,
        };
        Ok(c)
    }
}

pub(super) fn get_string_vec_from_param_or_file(
    dsn: &mut Dsn,
    key: &str,
) -> Result<Vec<String>, String> {
    if let Some(nodes) = dsn.remove(key) {
        let (files, mut node_config): (Vec<_>, Vec<_>) = nodes
            .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .partition(|v| v.starts_with("@"));
        dbg!(&files, &node_config);
        for file in files {
            let f = std::fs::File::open(&file[1..]);
            if f.is_err() {
                log::warn!("file: {} read error", file);
                continue;
                // return Err("file read error".to_string());
            }
            let buf = std::io::BufReader::new(f.unwrap());
            let mut file_data = buf.lines().collect_vec();
            // remove header
            if file_data.remove(0).is_err() {
                log::warn!("file: {} content length < 1", file);
            }

            node_config.extend(
                file_data
                    .iter()
                    .filter_map(|r| r.as_ref().ok())
                    .map(|s| s.replace(",", "::")),
            );
        }
        if node_config.len() == 0 {
            log::warn!("node config is empty");
            // return Err(format!("node config set but is empty: {nodes}"));
        }
        return Result::Ok(node_config);
    }
    // log::warn!("node config is empty");
    return Err("Nodes not set".to_string());
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

const EXE: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "taosx-opc.exe"
        } else {
            "taosx-opc"
        }
    }
};

fn exe_path() -> PathBuf {
    super::get_plugin_dir("opc").join(EXE)
}

pub fn info() -> Result<(&'static str, PathBuf, String), std::io::Error> {
    let path = exe_path();
    let output = std::process::Command::new(&path)
        .arg("version")
        .output()?;
    Ok((
        "opc",
        path,
        String::from_utf8_lossy(&output.stdout).to_string(),
    ))
}
pub(crate) async fn opc_config_from(taos: &Taos, dsn: &Dsn, port: u16) -> anyhow::Result<OpcTableConfig> {
    let config = OPCConfig::new(dsn.clone(), port, OPCConfigMode::Collect)?;
    config.parse_tables_with(taos).await
}
pub fn opc_config_blocking(taos: &Taos, dsn: &Dsn, port: u16) -> anyhow::Result<OPCConfig> {
    let config = OPCConfig::new(dsn.clone(), port, OPCConfigMode::Collect)?;
    Ok(config)
}

pub(crate) const DEFAULT_TS_COLUMN_NAME: &str = "ts";
pub(crate) const DEFAULT_SERVER_TS_COLUMN_NAME: &str = "server_ts";

#[instrument(skip(port_pool))]
pub async fn opc_to_taos(
    from: Dsn,
    actions: Vec<Action>,
    to: Dsn,
    jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<()> {
    println!("# loading plugin: OPC");
    if to.subject.is_none() {
        Err(OpcError::DatabaseIsRequired(to.clone()))?;
    }
    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for OPC connection"))?;
    let config = OPCConfig::new(from, ipc_port, OPCConfigMode::Collect)?;

    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    log::info!("Using opc config file {} \n{}", config_path.display(), toml);

    let mut table_config = None;
    let connector = match config.opc_type {
        OpcType::FAKE => None,
        OpcType::OPCDA => Some("opc_da"),
        OpcType::OPCUA => Some("opc_ua"),
    };
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let ipc = if with_agent.is_none() {
        let target_pool = TaosBuilder::from_dsn(&to)?.pool()?;
        let taos = target_pool.get().await?;
        let target_pool_for_ipc = target_pool.clone();

        table_config.replace(config.parse_tables_with(&taos).await?);
        sink::listen_tcp_socket(
            target_pool_for_ipc,
            config.report.remote,
            sender,
            table_config,
            cancel.clone(),
            with_agent,
            None,
            connector,
            transferred.clone(),
        )?
    } else {
        sink::listen_tcp_socket_with_agent(
            config.report.remote,
            sender,
            table_config,
            cancel.clone(),
            with_agent.unwrap(),
        )?
    };

    let port_pool = port_pool.clone();
    let mut command = tokio::process::Command::new(exe_path());
    let child = command
        .arg("collect")
        .arg(format!("--conf={}", &config_path.display()))
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());
    {
        let mut child = child.spawn()?;
        tokio::spawn(async move {
            tokio::select! {
                status = child.wait() => {
                    let status = status?;
                    log::info!("OPC exit with status {}", status);
                    if !status.success() {
                        let _ = ipc.send(());
                        anyhow::bail!("OPC exist with status {}", status);
                        // anyhow::bail!("OPC error: {}", child.stderr.map(|err| String::from_utf8_lossy(&err) ).unwrap_or("".into()));
                    }
                },
                // _ = tokio::signal::ctrl_c() => {
                //     log::info!("Ctrl+C triggered, cancel tasks");
                //     cancel.cancel();
                // },
                err = receiver.recv() => {
                    log::info!("have received worker thread panicked message, terminate child process");
                    if let Some(err) = err {
                        let _ = ipc.send(());
                        anyhow::bail!("OPC writer error: {err}");
                    }
                },
                _ = cancel.cancelled() => {
                    log::info!("opc task cancelled");
                },
            };
            ipc.send(())?;
            let _ = child.kill().await;
            // terminate_child_process(pid)?;
            log::info!("OPC to taos task done");
            temp_path.close().unwrap();
            port_pool.put(ipc_port);
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }).await??;
    }
    Ok(())
}

/// check field type
/// return true if matched
/// if type equals return true else false
/// if type is binary|varchar|nchar check whether type configed contains those characters
fn check_field_type(field_type_config: &String, field_type: String) -> bool {
    let field_type_config = field_type_config.to_ascii_lowercase();
    if field_type.contains("binary")
        || field_type.contains("varchar")
        || field_type.contains("nchar")
    {
        field_type_config.contains("binary")
            || field_type_config.contains("varchar")
            || field_type_config.contains("nchar")
    } else if field_type_config == field_type {
        true
    } else {
        false
    }
}

#[derive(Clone, Debug)]
pub struct OpcTableConfig {
    /// table_info: table_name, Vec<(field, type)>
    pub(crate) table_info: HashMap<String, (String, String, IpcDataType)>,

    /// table_name, ts column
    pub(crate) ts_cloumn_name: HashMap<String, (String, Option<String>)>,

    pub(crate) use_received_time: bool,
}

pub async fn opc_datasets(req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let from: Dsn = req.from.parse().unwrap();
    if req.categories.is_empty() {
        anyhow::bail!("categories is empty");
    }

    let config = OPCConfig::new(from.clone(), 0, OPCConfigMode::Points)?;
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
    let mut command = async_process::Command::new(exe_path());
    let output = command
        .arg("points")
        .arg(format!("--conf={}", &config_path.display()))
        .stdout(async_process::Stdio::piped())
        .stderr(async_process::Stdio::piped())
        .output()
        .await
        .context("Start OPC collector error")?;
    // dbg!(output);
    log::info!("OPC exit with status {}", output.status);

    temp_path.close()?;
    // let json = String::from_utf8_lossy(&output.stdout);
    let res: Vec<DataSet> = serde_json::from_slice(&output.stdout)?;
    log::debug!(
        "opc datasets : {}",
        serde_json::to_string(&res).unwrap_or("".to_string())
    );
    let options = vec![
        OptionSet {
            name: "table".to_string(),
            description: Some("Table name".to_string()),
            required: true,
        },
        OptionSet {
            name: "field".to_string(),
            description: Some("Field name".to_string()),
            required: true,
        },
        OptionSet {
            name: "type".to_string(),
            description: Some("Field type".to_string()),
            required: true,
        },
    ];
    let format = Some("{id}::{table}::{field}::{type}".to_string());
    if let Some(pattern) = req.pattern.as_deref() {
        let regex = regex::Regex::from_str(pattern)?;
        // regex.is_match(text)
        let res = res
            .into_iter()
            .filter(|set| {
                regex.is_match(&set.id)
                    || set
                        .name
                        .as_deref()
                        .map(|s| regex.is_match(s))
                        .unwrap_or(false)
            })
            .map(|mut set| {
                set.category = Some(req.categories[0].clone());
                set.options = Some(options.clone());
                set.format = format.clone();
                set
            })
            .skip(req.offset)
            .take(req.limit)
            .collect_vec();
        Ok(res)
    } else {
        Ok(res
            .into_iter()
            .map(|mut set| {
                set.category = Some(req.categories[0].clone());
                set.options = Some(options.clone());
                set.format = format.clone();
                set
            })
            .skip(req.offset)
            .take(req.limit)
            .collect())
    }
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
        debug: true,
        use_received_time: true,
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
            limit: Some(10),
            ua: Some(UaCollectConfig {
                collect_mode: "observe"
                    .to_string()
                    .parse::<CollectMode>()
                    .map_err(|err| OpcError::ParseError("collect_mode", err))?,
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
    assert_eq!(
        r#"opc_type = "opcua"
debug = true

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
limit = 10

[collect.ua]
collect_mode = "observe"

[[collect.ua.nodes]]
id = "1"
value_type = "DOUBLE"

[[collect.da.tags]]
tag = "123"
value_type = "VARCHAR"

[report]
remote = "remote.remote"
concurrent = 10
batch_timeout = 100
"#,
        toml
    );
    Ok(())
}
#[tokio::test]
async fn test_get_string_vec_from_param_or_file() -> anyhow::Result<()> {
    use taos::IntoDsn;
    let mut dsn = "opc+ua://Win10-2021XIVKQ:53530/OPCUA/SimulationServer?ua.nodes=ns=3;i=1004::ntb1::c0::double,ns=3;i=1008::ntb1::c1::double".into_dsn()?;
    let vec_string = get_string_vec_from_param_or_file(&mut dsn, "ua.nodes")
        .map_err(|s| OpcError::FileParseFound(s))?;
    assert_eq!(
        vec_string,
        vec![
            String::from("ns=3;i=1004::ntb1::c0::double"),
            String::from("ns=3;i=1008::ntb1::c1::double")
        ]
    );
    let mut dsn = "opc+ua://Win10-2021XIVKQ:53530/OPCUA/SimulationServer?ua.nodes=ns=3;i=1004::ntb1::c0::double,ns=3;i=1008::ntb1::c1::double,@/Users/zmlgirl/Downloads/test_opc.csv".into_dsn()?;
    let vec_string = get_string_vec_from_param_or_file(&mut dsn, "ua.nodes")
        .map_err(|s| OpcError::FileParseFound(s))?;
    assert_eq!(
        vec_string,
        vec![
            String::from("ns=3;i=1004::ntb1::c0::double"),
            String::from("ns=3;i=1008::ntb1::c1::double"),
            String::from("ns=2;i=2::ntb2::c1::double"),
            String::from("ns=2;i=3::ntb3::c2::int")
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_agent() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    pretty_env_logger::init();
    let mut opc = "opc+ua://192.168.0.133:53530/OPCUA/SimulationServer?\
    ua.nodes=ns=10;i=1004::t1::c1::double&connect_timeout=5&request_timeout=5&\
    concurrent=1&batch_size=5&batch_timeout=5&debug=true";
    let mut target = "taos:///opcua";
    opc_to_taos(
        opc.parse().unwrap(),
        vec![],
        target.parse().unwrap(),
        1,
        &PortPool::default(),
        CancellationToken::new(),
        Some((2, "http://127.0.0.1:6051".into(), "".into())),
        None,
    )
    .await?;
    Ok(())
}
