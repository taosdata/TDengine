use std::{
    collections::{HashMap, HashSet}, fs, io::prelude::*, num::ParseIntError, path::PathBuf, str::FromStr,
    sync::Arc, time::Duration, 
};

use file_rotate::{
    compression::Compression,
    suffix::{AppendTimestamp, DateFrom, FileLimit},
    ContentLimit, FileRotate, TimeFrequency,
};

use anyhow::Context;
use itertools::Itertools;
use taos::{
    AsyncTBuilder, Dsn, Taos, TaosBuilder, Ty, AsyncQueryable,
};
use taosx_ipc::{types::OptionSet, prelude::IpcDataType};
use tokio::io::AsyncBufReadExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{plugins::sink, utils::{port_pool::PortPool, get_string_content_from_file_path}, Action, DataSet, DataSetsReq, Transferred};

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
    #[error("node config error {0}")]
    NodeConfig(String),
    #[error("Parse integer error from {1} while parsing parameter {0}: {2:?}")]
    ParseNumberError(&'static str, String, ParseIntError),
    #[error("Parse param error from {1} while parsing parameter {0}")]
    ParseError(&'static str, String),
    #[error("plugin not found: {0}")]
    ExeNotFound(String),
    #[error("{0} config error: {1}")]
    ConfigError(&'static str, String),
}

#[derive(Debug, serde::Serialize)]
pub struct OPCConfig {
    opc_type: OpcType,
    debug: bool,
    // #[serde(skip)]
    /// use receviced time as ts cloumn value when config true
    // use_received_time: bool,
    connect: ConnectConfig,
    points: Option<PointsConfig>,
    collect: CollectConfig,
    report: ReportConfig,

    #[serde(skip)]
    param_mapping: HashMap<String, PointConfig>,
    // #[serde(skip)]
    /// table_info: table_name, Vec<(field, type)>
    // table_info: HashMap<String, Vec<(String, String)>>,
    #[serde(skip)]
    opc_table_config: Option<TableConfig>,
}

// #[derive(Clone, serde::Deserialize, Debug, serde::Serialize)]
// pub struct StableConfig {
//     pub stable: Option<String>,
//     pub stable_prefix: Option<String>,
// }

// #[derive(Clone, serde::Deserialize, Debug, serde::Serialize)]
// pub struct StableColumnConfig {
//     pub column_configs: Vec<ColumnConfig>,
//     pub tag_configs: Option<Vec<ColumnConfig>>,
// }

#[derive(Clone, serde::Deserialize, Debug, serde::Serialize)]
pub struct TableConfig {
    pub stable_prefix: Option<String>,
    pub column_configs: Vec<ColumnConfig>,
    pub tag_configs: Option<Vec<TagConfig>>,
}

#[derive(Clone, serde::Deserialize, Debug, serde::Serialize)]
pub struct ColumnConfig {
    pub column_name: String,
    pub column_type: Option<Ty>,
    pub column_alias: Option<String>,
    pub is_primary_key: bool,
}

#[derive(Clone, serde::Deserialize, Debug, serde::Serialize)]
pub struct TagConfig {
    pub column_name: String,
    pub column_type: IpcDataType,
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
struct PointsConfig {
    limit: usize,
    regex: Option<String>,
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
    // value_type: String,
}

#[derive(Debug, serde::Serialize)]
struct DaCollectConfig {
    tags: Vec<DaNodeConfig>,
}

#[derive(Debug, serde::Serialize)]
struct DaNodeConfig {
    tag: String,
    // value_type: String,
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
    async fn new(mut dsn: Dsn, ipc_port: u16, config_mode: OPCConfigMode, taos: Option<&Taos>) -> Result<Self, OpcError> {
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
        let use_csv_config = if let Some(assert) = dsn.remove("use_csv_config") {
            match assert.as_str() {
                "false" => false,
                "" | "true" => true,
                _ => return Err(OpcError::ConfigError("use_csv_config", "should config true or false".to_string())),
            }
        } else {
            false
        };
        let mut opc_table_config = None;
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

                let certificate = if let Some(cert) = dsn.remove("certificate") {
                    get_string_content_from_file_path(&cert)
                } else {
                    None
                };
                let private_key = if let Some(private_key) = dsn.remove("private_key") {
                    get_string_content_from_file_path(&private_key)
                } else {
                    None
                };

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
                } else if use_csv_config {
                    let res =  generate_opcconfig_from_csv(&mut dsn, "ua.nodes").await.map_err(|err| OpcError::ConfigError("ua.nodes", err.to_string()))?;
                    opc_table_config = Some(res.0);
                    for child_table_name in res.2.iter() {
                        let drop_sql = format!("DROP TABLE IF EXISTS {child_table_name}");
                        taos.unwrap().exec(drop_sql).await.map_err(|err| OpcError::ConfigError("ua.nodes", err.to_string()))?;
                    }
                    res.1
                } else {
                    get_string_vec_from_param_or_file(&mut dsn, "ua.nodes")
                        .map_err(|s| OpcError::FileParseFound(s))?
                };
                let mut ua_node_config_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    if pair.len() != 2 {
                        let pair = pair.join("::");
                        return Err(OpcError::NodeConfig(format!("node config: {pair} split result len is not 2")));
                    }
                    let id = String::from(pair[0]);
                    let code = String::from(pair[1]);
                    let ua_node_config = UANodeConfig {
                        id: id.clone(),
                    };
                    if !use_csv_config {
                        param_mapping.insert(
                            id,
                            PointConfig {
                                code,
                                stable: None,
                                enabled: None,
                                tag_values: None,
                                value_type: None,
                            }
                        );
                    }
                    ua_node_config_vec.push(ua_node_config);
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
                let server = dsn.subject.clone();
                if server.is_none() {
                    return Err(OpcError::ConfigError("subject", format!("should config subject for opc da")));
                }
                let nodes = dsn.addresses.clone();
                if nodes.is_empty() {
                    return Err(OpcError::ConfigError("host", format!("should config at least one host")));
                }
                let nodes = nodes.into_iter().map(|addr| addr.host.unwrap().clone())
                    .collect_vec();
                let connect_da_config = DaConnectConfig { server: server.unwrap(), nodes };
                connect = ConnectConfig {
                    ua: None,
                    da: Some(connect_da_config),
                };
                let node_vec: Vec<String> = if let OPCConfigMode::Points = config_mode {
                    vec![]
                } else if use_csv_config {
                    let res =  generate_opcconfig_from_csv(&mut dsn, "da.tags").await.map_err(|err| OpcError::ConfigError("da.tags", err.to_string()))?;
                    opc_table_config = Some(res.0);
                    for child_table_name in res.2.iter() {
                        let drop_sql = format!("DROP TABLE IF EXISTS {child_table_name}");
                        taos.unwrap().exec(drop_sql).await.map_err(|err| OpcError::ConfigError("ua.nodes", err.to_string()))?;
                    }
                    res.1
                } else {
                    get_string_vec_from_param_or_file(&mut dsn, "da.tags")
                        .map_err(|s| OpcError::FileParseFound(s))?
                };
                
                let mut da_nodes_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    if pair.len() != 2 {
                        let pair = pair.join("::");
                        return Err(OpcError::NodeConfig(format!("node config: {pair} split result len is not 2")));
                    }
                    let tag = String::from(pair[0]);
                    let code = String::from(pair[1]);
                    da_nodes_vec.push(DaNodeConfig {
                        tag: tag.clone(),
                    });
                    if !use_csv_config {
                        param_mapping.insert(
                            tag,
                            PointConfig {
                                code,
                                stable: None,
                                enabled: None,
                                tag_values: None,
                                value_type: None,
                            }
                        );
                    }
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

        let report = ReportConfig {
            remote,
            concurrent,
            batch_size,
            batch_timeout,
        };
        let table_config: Option<TableConfig>;
        if opc_table_config.is_none() {
            let config = dsn.remove("opc_table_config");
            if config.is_none() {
                return Err(OpcError::ConfigError("opc_table_config", "should config opc_table_config or use csv config file".to_string()));
            }
            table_config = Some(serde_json::from_str(config.unwrap().as_str()).map_err(|v| OpcError::ParseError("opc_table_config", v.to_string()))?);
        } else {
            let opc_table_config = opc_table_config.unwrap();
            table_config = Some(opc_table_config.table_config.clone());
            param_mapping = opc_table_config.id_code_map.clone();
        }
        Ok(OPCConfig {
            opc_type,
            debug,
            points: None,
            connect,
            collect,
            report,
            param_mapping,
            opc_table_config: table_config,
        })
    }

    pub async fn parse_tables_with(&self, taos: &Taos) -> anyhow::Result<OpcTableConfig> {
        let id_code_map = self.param_mapping.iter().map(|(id, code)| {
            (id.clone(), code.clone())
        }).collect();
        let c = OpcTableConfig {
            id_code_map,
            table_config: self.opc_table_config.clone().unwrap(),
        };
        Ok(c)
    }
}


const CSV_CONFIG_COLUMNS: [&str; 4] = ["point_id", "tbname", "type", "stable"];

pub use tokio_stream::StreamExt;
/// return opctableconfig, node_config, tables_to_drop
pub async fn generate_opcconfig_from_csv(dsn: &mut Dsn, key: &str) -> anyhow::Result<(OpcTableConfig, Vec<String>, Vec<String>)> {
    if let Some(nodes) = dsn.remove(key) {
        let (files, _): (Vec<_>, Vec<_>) = nodes
            .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .partition(|v| v.starts_with("@"));
        let mut id_code_map = HashMap::new(); // id, (code for sub-table name, stable)
        let mut tag_config = Vec::new();
        let mut column_config = Vec::new();
        let mut node_config_old = Vec::new();
        let mut tables_to_drop = Vec::new();
        for file in files {
            log::info!("current log: {}", std::env::current_dir().unwrap().to_str().unwrap());
            if !file.ends_with(".csv") {
                anyhow::bail!("file {file} is not a csv config");
            }
            let mut rdr = csv_async::AsyncReader::from_reader(tokio::fs::File::open(&file[1..]).await?);
            let mut records = rdr.records();
            // skip first line(desc)
            let header = records.next().await; 
            if header.is_none() {
                log::warn!("file {file} should have 2 lines at least");
                continue;
            }
            let header = header.unwrap()?;
            // header parse
            let mut column_map = HashMap::new();
            let mut column = 0;
            let temp_column = CSV_CONFIG_COLUMNS.iter().map(|s| s.to_string()).collect_vec().clone();
            let mut column_set: HashSet<&String> = HashSet::from_iter(temp_column.iter());
            for column_name in header.iter() {
                column_map.insert(column, column_name.clone());
                if column_name.starts_with("tag") {
                    // is tag config tag::type::name e.g. tag::varchar(123)::unit
                    let split_tag = column_name.split("::").collect_vec();
                    if split_tag.len() != 3 {
                        anyhow::bail!("file {file} column {column_name} config error, pattern is tag::type::name");
                    }
                    let column_type = IpcDataType::from_str(split_tag.get(1).unwrap()).map_err(|err| anyhow::Error::msg(err))?;
                    tag_config.push(TagConfig {
                        column_name: split_tag.get(2).unwrap().to_string(),
                        column_type,
                    });
                }
                column += 1;
                log::info!("&column_name to remove: {}", column_name);
                column_set.remove(&column_name.to_string());
            }
            if column_set.len() != 0 {
                anyhow::bail!("csv config miss column: {}", column_set.iter().next().unwrap());
            }
            let mut line = 3;
            let mut column_config_init = false;
            while let Some(record) = records.next().await {
                match record {
                    Ok(record) => {
                        let mut record_map = HashMap::new(); // column_name, column_data
                        let mut tag_values = Vec::new();
                        for (index, column_name) in column_map.iter() {
                            let data = record.get(index.clone()).unwrap();
                            if column_name.starts_with("tag::") {
                                tag_values.push(data.to_string());
                            } else {
                                record_map.insert(column_name.to_string(), data.to_string());
                            }
                        }
                        
                        let stable = record_map.get("stable").unwrap().clone();
                        if record_map.get("tbname").unwrap().contains("{") { // maybe should use pattern match?
                            // should be a expression d00{point_id}_{tag1}_{tag2}
                            // TODO PATTERN HANDLE and reset tbname
                        }
                        let code = record_map.get("tbname").unwrap();
                        let enabled_column = record_map.get("enabled");
                        if enabled_column.is_some() {
                            let enabled = enabled_column.unwrap();
                            if enabled == "0" {
                                // warn: should delete child table (stable_code)
                                tables_to_drop.push(format!("{stable}_{code}"));
                            }
                        }
                        let column_type = IpcDataType::from_str(record_map.get("type").unwrap()).map_err(|err| anyhow::Error::msg(err))?;
                        if !column_config_init {
                            column_config.push(ColumnConfig {
                                column_name: "value".to_string(),
                                column_type: None,
                                column_alias: Some(record_map.get("value_col").ok_or("val".to_string()).unwrap().clone()),
                                is_primary_key: false,
                            });
                            column_config.push(ColumnConfig {
                                column_name: "quality".to_string(),
                                column_type: Some(Ty::Int),
                                column_alias: Some(record_map.get("quality_col").ok_or("quality".to_string()).unwrap().clone()),
                                is_primary_key: false,
                            });
                            let received_time_col = record_map.get("received_time_col");
                            let mut has_primary_key = false;
                            if received_time_col.is_some() {
                                has_primary_key = true;
                                column_config.push(ColumnConfig {
                                    column_name: "received_time".to_string(),
                                    column_type: Some(Ty::Timestamp),
                                    column_alias: Some(record_map.get("received_time_col").ok_or("received_time".to_string()).unwrap().clone()),
                                    is_primary_key: has_primary_key,
                                });
                            }
                            column_config.push(ColumnConfig {
                                column_name: "original_time".to_string(),
                                column_type: Some(Ty::Timestamp),
                                column_alias: Some(record_map.get("ts_col").ok_or("ts".to_string()).unwrap().clone()),
                                is_primary_key: !has_primary_key,
                            });
                            column_config_init = true;
                        }
                        
                        let enabled = record_map.get("enabled");
                        let enabled = if enabled.is_none() {
                            None
                        } else {
                            if enabled.unwrap() == "1" {
                                Some(true)
                            } else {
                                Some(false)
                            }
                        };
                        let point_id = record_map.get("point_id").unwrap();
                        
                        let tag_values = if tag_values.len() == 0 {
                            None
                        } else {
                            Some(tag_values)
                        };
                        id_code_map.insert(point_id.clone(), 
                            PointConfig {
                                code: code.clone(),
                                stable: Some(stable),
                                tag_values,
                                value_type: Some(column_type),
                                enabled,
                            });
                            node_config_old.push(format!("{point_id}::{code}"))
                    },
                    Err(e) => log::warn!("line {line} have different with other previous lines ", )
                }
                line += 1;
            }
        }
        let tag_configs = if tag_config.len() == 0 {
            None
        } else {
            Some(tag_config)
        };
        return Ok((OpcTableConfig { id_code_map, 
            table_config: TableConfig { stable_prefix: None, column_configs: column_config, tag_configs } }, 
            node_config_old, 
            tables_to_drop,
        ));
    }
    anyhow::bail!("should config {key}");
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
        // dbg!(&files, &node_config);
        for file in files {
            log::info!("current log: {}", std::env::current_dir().unwrap().to_str().unwrap());
            let f = std::fs::File::open(&file[1..]);
            if f.is_err() {
                log::warn!("file: {} read error, cause: {}", &file[1..], f.err().unwrap());
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

const LOG_FILE: &str = "opc.log";

fn log_path() -> PathBuf {
    super::get_log_dir("opc")
}

pub fn info() -> Result<(&'static str, PathBuf, String), std::io::Error> {
    let path = exe_path();
    let output = std::process::Command::new(&path).arg("version").output()?;
    Ok((
        "opc",
        path,
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}
pub(crate) async fn opc_config_from(
    taos: &Taos,
    dsn: &Dsn,
    port: u16,
) -> anyhow::Result<OpcTableConfig> {
    let config = OPCConfig::new(dsn.clone(), port, OPCConfigMode::Collect, Some(taos)).await?;
    config.parse_tables_with(taos).await
}
pub fn opc_config_blocking(taos: &Taos, dsn: &Dsn, port: u16) -> anyhow::Result<OPCConfig> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let config = OPCConfig::new(dsn.clone(), port, OPCConfigMode::Collect, Some(taos)).await?;
        Ok(config)
    })
}

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

    let exe_exists = std::path::Path::new(&exe_path()).exists();
    if !exe_exists {
        log::error!("plugin not found {}", exe_path().to_str().unwrap());
        Err(OpcError::ExeNotFound(format!("{}", exe_path().to_str().unwrap())))?;
    }

    if to.subject.is_none() {
        Err(OpcError::DatabaseIsRequired(to.clone()))?;
    }
    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for OPC connection"))?;
    let builder: TaosBuilder = TaosBuilder::from_dsn(&to)?;
    let taos = builder.build().await?;
    let config = OPCConfig::new(from, ipc_port, OPCConfigMode::Collect, Some(&taos)).await?;
    if config.opc_table_config.is_none() {
        anyhow::bail!("should config opc table config");
    }

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
        let builder = TaosBuilder::from_dsn(&to)?;
        #[cfg(not(feature = "disable-enterprise-only-validation"))]
        if !builder.is_enterprise_edition().await? {
            anyhow::bail!(
                "Only enterprise edition is supported. If it's not your case, please contact us."
            )
        }
        let target_pool = builder.pool()?;
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

    let mut log_path = log_path();
    fs::create_dir_all(&log_path)?;

    log::info!("log path created: {}", &log_path.display());

    log_path.push(LOG_FILE);

    log::info!("log file dir: {}", &log_path.display());

    let mut log_rotation = FileRotate::new(
        &log_path,
        AppendTimestamp::with_format(
            "%Y-%m-%d",
            FileLimit::Age(chrono::Duration::weeks(100)),
            DateFrom::DateYesterday,
        ),
        ContentLimit::Time(TimeFrequency::Daily),
        Compression::None,
        #[cfg(unix)]
        None,
    );

    let child = command
        .arg("collect")
        .arg(format!("--conf={}", &config_path.display()))
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped());
    {
        let mut child = child.spawn()?;

        let stderr = child.stderr.take().expect("Failed to capture stderr");
        tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(stderr);
            let mut line = String::new();
            loop {
                // Read a line from stderr
                let bytes_read = reader.read_line(&mut line).await.unwrap();
                if bytes_read == 0 {
                    break; // End of stream, exit the loop
                }
                // Write the line to log_rotation
                write!(log_rotation, "{}", line).unwrap();
                line.clear();
            }
            Ok::<(), std::io::Error>(())
        });

        tokio::spawn(async move {
            tokio::select! {
                status = child.wait() => {
                    let status = status?;
                    log::info!("OPC exit with {}", status);
                    if !status.success() {
                        let _ = ipc.send(());
                        anyhow::bail!("OPC exist with {}", status);
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

    // id, (code, stable, enabled)
    // code for child table name, stable maybe none when use ui config, casue stabel_prefix exists
    // when stable is none stable_prefix will be enabled
    pub(crate) id_code_map: HashMap<String, PointConfig>,

    pub(crate) table_config: TableConfig,
}

#[derive(Clone, Debug)]
pub(crate) struct PointConfig {
    pub code: String,
    pub stable: Option<String>,
    pub tag_values: Option<Vec<String>>,
    pub value_type: Option<IpcDataType>,
    pub enabled: Option<bool>,
}

pub async fn opc_datasets(req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let from: Dsn = req.from.parse().unwrap();
    if req.categories.is_empty() {
        anyhow::bail!("categories is empty");
    }

    let mut config = OPCConfig::new(from.clone(), 0, OPCConfigMode::Points, None).await?;
    let points_config = PointsConfig {
        limit: req.limit,
        regex: req.pattern.clone(),
    };
    config.points = Some(points_config);
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
    let mut command = tokio::process::Command::new(exe_path());
    let output = command
        .arg("points")
        .arg(format!("--conf={}", &config_path.display()))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .with_context(|| "Start OPC collector error")?;
    // dbg!(output);
    let mut log_path = log_path();
    log_path.push(LOG_FILE);

    let mut log_rotation = FileRotate::new(
        &log_path,
        AppendTimestamp::with_format(
            "%Y-%m-%d",
            FileLimit::Age(chrono::Duration::weeks(100)),
            DateFrom::DateYesterday,
        ),
        ContentLimit::Time(TimeFrequency::Daily),
        Compression::None,
        #[cfg(unix)]
        None,
    );

    write!(log_rotation, "{}", String::from_utf8_lossy(&output.stderr)).unwrap();

    log::info!("OPC exit with status {}", output.status);

    temp_path.close()?;
    // let json = String::from_utf8_lossy(&output.stdout);
    let res: Vec<DataSet> = serde_json::from_slice(&output.stdout)?;
    log::debug!(
        "opc datasets : {}",
        serde_json::to_string(&res).unwrap_or("".to_string())
    );
    let (option_set_code_display, option_set_code_desc) = if let Some(lang) = req.lang.clone() {
        match lang.as_str() {
            "zh" => ("编码".to_string(), "点位编码".to_string()),
            _ => ("Code".to_string(), "Point Code".to_string())
        }
    } else {
        ("Code".to_string(),"Point Code".to_string())
    };
    let options = vec![
        OptionSet {
            name: "code".to_string(),
            display: option_set_code_display,
            description: Some(option_set_code_desc),
            required: true,
        },
    ];
    let format = Some("{id}::{code}".to_string());
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
        PointConfig { code: "567".to_string(), stable: None, enabled: None, tag_values: None, value_type: None}
    );
    let mut column_configs = Vec::new();
    let column_config = ColumnConfig {
        column_name: String::from("received_time"),
        column_type: Some(Ty::Timestamp),
        column_alias: Some("ts".to_string()),
        is_primary_key: true,
    };
    column_configs.push(column_config);
    let column_config = ColumnConfig {
        column_name: String::from("original_time"),
        column_type: Some(Ty::Timestamp),
        column_alias: None,
        is_primary_key: false,
    };
    column_configs.push(column_config);
    let column_config = ColumnConfig {
        column_name: String::from("value"),
        column_type: Some(Ty::Timestamp),
        column_alias: None,
        is_primary_key: true,
    };
    column_configs.push(column_config);
    let opc_table_config = TableConfig {
        stable_prefix: Some("meters".to_string()),
        column_configs,
        tag_configs: None,
    };
    let config = OPCConfig {
        opc_type: OpcType::OPCUA,
        debug: true,
        points: Some(PointsConfig {
            limit: 32,
            regex: Some(String::from("123")),
        }),
        // use_received_time: true,
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
                    // value_type: String::from("DOUBLE"),
                }],
            }),
            da: Some(DaCollectConfig {
                tags: vec![DaNodeConfig {
                    tag: String::from("123"),
                    // value_type: String::from("VARCHAR"),
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
        // table_info: HashMap::new(),
        opc_table_config: Some(opc_table_config),
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

[points]
limit = 32
regex = "123"

[collect]
interval = 10
limit = 10

[collect.ua]
collect_mode = "observe"

[[collect.ua.nodes]]
id = "1"

[[collect.da.tags]]
tag = "123"

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
