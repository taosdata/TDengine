use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use base64::Engine;
use base64::engine::general_purpose;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::{AsyncQueryable, Dsn, Taos, Ty};
use tokio_stream::StreamExt;
use taosx_ipc::prelude::IpcDataType;
use crate::runners::opc::{generate_tbname_from_pattern, get_string_vec_from_param_or_file_for_opc, OpcError, parse_bool_param_from_dsn};

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
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

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TableConfig {
    pub stable_prefix: Option<String>,
    pub column_configs: Vec<ColumnConfig>,
    pub tag_configs: Option<Vec<TagConfig>>,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct ColumnConfig {
    pub column_name: String,
    pub column_type: Option<Ty>,
    pub column_alias: Option<String>,
    pub is_primary_key: bool,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TagConfig {
    pub column_name: String,
    pub column_type: IpcDataType,
}

#[derive(Debug, Serialize)]
struct ConnectConfig {
    ua: Option<UaConnectConfig>,
    da: Option<DaConnectConfig>,
}

#[derive(Debug, Serialize, Default)]
enum AuthMethod {
    Anonymous,
    UserName,
    #[default]
    Certificate,
}

#[derive(Debug, Serialize)]
struct UaConnectConfig {
    endpoint: String,
    connect_timeout: i64,
    request_timeout: i64,
    security_policy: String,
    security_mode: String,
    certificate: Option<String>,
    private_key: Option<String>,
    auth_method: AuthMethod,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Serialize)]
struct DaConnectConfig {
    server: String,
    nodes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct PointsConfig {
    limit: usize,
    regex: Option<String>,
}

#[derive(Debug, Serialize)]
struct CollectConfig {
    interval: Option<i64>,
    limit: Option<i64>,
    ua: Option<UaCollectConfig>,
    da: Option<DaCollectConfig>,
    dump: Option<DumpConfig>,
}

#[derive(Debug, Serialize)]
struct UaCollectConfig {
    collect_mode: CollectMode,
    nodes: Vec<UANodeConfig>,
    // dump: Option<DumpConfig>,
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
struct UANodeConfig {
    id: String,
    // value_type: String,
}

#[derive(Debug, Serialize)]
struct DumpConfig {
    enable: bool,
    path: Option<String>,
    keep: Option<usize>,
}

#[derive(Debug, Serialize)]
struct DaCollectConfig {
    tags: Vec<DaNodeConfig>,
    // dump: Option<DumpConfig>,
}

#[derive(Debug, Serialize)]
struct DaNodeConfig {
    tag: String,
    // value_type: String,
}

#[derive(Debug, Serialize)]
struct ReportConfig {
    remote: String,
    concurrent: Option<i64>,
    batch_size: Option<i64>,
    batch_timeout: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpcTableConfig {
    // id, (code, stable, enabled)
    // code for child table name, stable maybe none when use ui config, casue stabel_prefix exists
    // when stable is none stable_prefix will be enabled
    pub(crate) id_code_map: HashMap<String, PointConfig>,

    pub(crate) table_config: TableConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PointConfig {
    pub code: String,
    pub stable: Option<String>,
    pub tag_values: Option<HashMap<String, String>>,
    pub value_type: Option<IpcDataType>,
}

/// OPC connector mode
enum OPCConfigMode {
    /// just get points
    Points,
    /// collect point data
    Collect,
}

impl OPCConfig {
    pub(crate) async fn new(
        mut dsn: Dsn,
        ipc_port: u16,
        config_mode: OPCConfigMode,
        taos: Option<&Taos>,
    ) -> Result<Self, OpcError> {
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
        let csv_config_file = dsn.remove("csv_config_file");

        let mut opc_table_config = None;
        let dump_enable = parse_bool_param_from_dsn(&mut dsn, "enable")
            .map_err(|err| OpcError::ConfigError("enable", err.to_string()))?;
        let dump_config = if dump_enable.is_some() {
            let dump_enable = dump_enable.unwrap();
            if dump_enable {
                let path = dsn.remove("path");
                let keep = parse_int_at!("keep");
                if path.is_none() {
                    return Err(OpcError::ConfigError(
                        "path",
                        "should config dump path".to_string(),
                    ));
                }
                if keep.is_none() {
                    return Err(OpcError::ConfigError(
                        "keep",
                        "should config dump keep".to_string(),
                    ));
                }
                Some(DumpConfig {
                    enable: dump_enable,
                    path,
                    keep: Some(keep.unwrap() as usize),
                })
            } else {
                Some(DumpConfig {
                    enable: dump_enable,
                    path: None,
                    keep: None,
                })
            }
        } else {
            None
        };

        let select_all_points = dsn
            .remove("select_all_points")
            .map(|v| v.parse::<bool>().ok().unwrap_or(true))
            .unwrap_or(false);

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

                let connect_timeout = parse_int_at!("connect_timeout").unwrap_or(10);
                let request_timeout = parse_int_at!("request_timeout").unwrap_or(10);
                let security_policy = dsn.remove("security_policy").unwrap_or("None".to_string());
                let security_mode = dsn.remove("security_mode").unwrap_or("None".to_string());

                let certificate = if let Some(cert) = dsn.remove("certificate") {
                    Some(cert.trim_start_matches('@').to_string())
                    // get_string_content_from_param_value(&cert, true, false)
                    //     .map_err(|err| OpcError::ConfigError("certificate", err.to_string()))?
                } else {
                    None
                };
                let private_key = if let Some(private_key) = dsn.remove("private_key") {
                    Some(private_key.trim_start_matches('@').to_string())
                    // get_string_content_from_param_value(&private_key, true, false)
                    //     .map_err(|err| OpcError::ConfigError("private_key", err.to_string()))?
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
                } else if csv_config_file.is_some() {
                    let res = generate_opcconfig_from_csv(
                        "opcua",
                        csv_config_file.clone().unwrap().as_str(),
                    )
                        .await
                        .map_err(|err| OpcError::ConfigError("csv_config_file", err.to_string()))?;
                    opc_table_config = Some(res.0);
                    for child_table_name in res.2.iter() {
                        let drop_sql = format!("DROP TABLE IF EXISTS {child_table_name}");
                        tracing::info!("drop sql: {drop_sql}");
                        taos.unwrap().exec(drop_sql).await.map_err(|err| {
                            OpcError::ConfigError("csv_config_file", err.to_string())
                        })?;
                    }
                    res.1
                } else if select_all_points {
                    // TODO: all points returns empty.
                    // warn!("select_all_points is not implemented");
                    Vec::new()
                } else {
                    get_string_vec_from_param_or_file_for_opc(&mut dsn, "ua.nodes")
                        .map_err(|s| OpcError::FileParseFound(s))?
                };
                let mut ua_node_config_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    if pair.len() != 2 {
                        let pair = pair.join("::");
                        return Err(OpcError::NodeConfig(format!(
                            "node config: {pair} split result len is not 2"
                        )));
                    }
                    let id = String::from(pair[0]);
                    let code = String::from(pair[1]);
                    let ua_node_config = UANodeConfig { id: id.clone() };
                    if csv_config_file.is_none() {
                        param_mapping.insert(
                            id,
                            PointConfig {
                                code,
                                stable: None,
                                tag_values: None,
                                value_type: None,
                            },
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
                    dump: dump_config,
                };
            }
            Some("da") => {
                opc_type = OpcType::OPCDA;
                let server = dsn.subject.clone();
                if server.is_none() {
                    return Err(OpcError::ConfigError(
                        "subject",
                        format!("should config subject for opc da"),
                    ));
                }
                let nodes = dsn.addresses.clone();
                if nodes.is_empty() {
                    return Err(OpcError::ConfigError(
                        "host",
                        format!("should config at least one host"),
                    ));
                }
                let nodes = nodes
                    .into_iter()
                    .map(|addr| addr.host.unwrap().clone())
                    .collect_vec();
                let connect_da_config = DaConnectConfig {
                    server: server.unwrap(),
                    nodes,
                };
                connect = ConnectConfig {
                    ua: None,
                    da: Some(connect_da_config),
                };
                let node_vec: Vec<String> = if let OPCConfigMode::Points = config_mode {
                    vec![]
                } else if csv_config_file.is_some() {
                    let res = generate_opcconfig_from_csv(
                        "opcda",
                        csv_config_file.clone().unwrap().as_str(),
                    )
                        .await
                        .map_err(|err| OpcError::ConfigError("csv_config_file", err.to_string()))?;
                    opc_table_config = Some(res.0);
                    for child_table_name in res.2.iter() {
                        let drop_sql = format!("DROP TABLE IF EXISTS {child_table_name}");
                        tracing::info!("drop sql: {drop_sql}");
                        taos.unwrap().exec(drop_sql).await.map_err(|err| {
                            OpcError::ConfigError("csv_config_file", err.to_string())
                        })?;
                    }
                    res.1
                } else {
                    get_string_vec_from_param_or_file_for_opc(&mut dsn, "da.tags")
                        .map_err(|s| OpcError::FileParseFound(s))?
                };

                let mut da_nodes_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    if pair.len() != 2 {
                        let pair = pair.join("::");
                        return Err(OpcError::NodeConfig(format!(
                            "node config: {pair} split result len is not 2"
                        )));
                    }
                    let tag = String::from(pair[0]);
                    let code = String::from(pair[1]);
                    da_nodes_vec.push(DaNodeConfig { tag: tag.clone() });
                    if csv_config_file.is_none() {
                        param_mapping.insert(
                            tag,
                            PointConfig {
                                code,
                                stable: None,
                                tag_values: None,
                                value_type: None,
                            },
                        );
                    }
                }
                collect = CollectConfig {
                    interval,
                    limit,
                    ua: None,
                    da: Some(DaCollectConfig { tags: da_nodes_vec }),
                    dump: dump_config,
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
                    .map_err(|_| OpcError::ParseError("debug", v))
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
        if matches!(config_mode, OPCConfigMode::Points) {
            table_config = None;
        } else {
            if opc_table_config.is_none() {
                if select_all_points {
                    table_config = None;
                } else {
                    let config = dsn.remove("opc_table_config");
                    if config.is_none() {
                        return Err(OpcError::ConfigError(
                            "opc_table_config",
                            "should config opc_table_config or use csv config file".to_string(),
                        ));
                    }
                    table_config =
                        Some(serde_json::from_str(config.unwrap().as_str()).map_err(|v| {
                            OpcError::ParseError("opc_table_config", v.to_string())
                        })?);
                }
            } else {
                let opc_table_config = opc_table_config.unwrap();
                table_config = Some(opc_table_config.table_config.clone());
                param_mapping = opc_table_config.id_code_map.clone();
            }
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

    pub async fn parse_tables_with(&self, _taos: &Taos) -> anyhow::Result<OpcTableConfig> {
        let id_code_map = self
            .param_mapping
            .iter()
            .map(|(id, code)| (id.clone(), code.clone()))
            .collect();
        let c = OpcTableConfig {
            id_code_map,
            table_config: self.opc_table_config.clone().unwrap(),
        };
        Ok(c)
    }
}

const CSV_CONFIG_COLUMNS: [&str; 2] = ["point_id", "tbname"];

/// return opctableconfig, node_config, tables_to_drop
pub async fn generate_opcconfig_from_csv(
    ty: &str,
    csv_config_file: &str,
) -> anyhow::Result<(OpcTableConfig, Vec<String>, Vec<String>)> {
    let files_or_strings = csv_config_file
        .split(",")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    // .partition(|v| v.starts_with("@"));
    let mut id_code_map = HashMap::new(); // id, (code for sub-table name, stable)
    let mut tag_config = Vec::new();
    let mut column_config = Vec::new();
    let mut node_config_old = Vec::new();
    let mut tables_to_drop = Vec::new();
    let mut current_tag_names = Vec::new();
    let mut stable_prefix = None;
    for mut file in files_or_strings {
        tracing::info!(
            "current log: {}",
            std::env::current_dir().unwrap().to_str().unwrap()
        );
        // if !file.ends_with(".csv") {
        // anyhow::bail!("file {file} is not a csv config");
        // }
        let mut rdr;
        if !file.starts_with("@") {
            // TODO use mime instead
            let decoded = general_purpose::STANDARD.decode(&file)?;
            let mut temp_file = tempfile::NamedTempFile::new()?;
            let res = String::from_utf8(decoded)?;
            write!(temp_file, "{}", res)?;
            file = format!("@{}", temp_file.path().to_str().unwrap());
            rdr = csv_async::AsyncReader::from_reader(tokio::fs::File::open(&file[1..]).await?);
            temp_file.into_temp_path();
        } else {
            rdr = csv_async::AsyncReader::from_reader(tokio::fs::File::open(&file[1..]).await?);
        }
        // let mut
        let mut records = rdr.records();
        let header = records.next().await;
        // skip first line(desc)
        if header.is_none() {
            tracing::warn!("file {file} should have 2 lines at least");
            continue;
        }
        let header = header.unwrap()?;
        // header parse
        let mut column_map = HashMap::new();
        let mut column = 0;
        let temp_column = CSV_CONFIG_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .collect_vec()
            .clone();
        let mut column_set: HashSet<&String> = HashSet::from_iter(temp_column.iter());
        for column_name in header.iter() {
            column_map.insert(column, column_name);
            if column_name.starts_with("tag") {
                // is tag config tag::type::name e.g. tag::varchar(123)::unit
                let split_tag = column_name.split("::").collect_vec();
                if split_tag.len() != 3 {
                    anyhow::bail!(
                        "file {file} column {column_name} config error, pattern is tag::type::name"
                    );
                }
                let column_type =
                    IpcDataType::from_str(split_tag.get(1).unwrap()).map_err(|err| {
                        anyhow::Error::msg(format!("{err} should be a valid Data Type"))
                    })?;
                let tag_name = split_tag.get(2).unwrap().to_string();
                check_duplicated(&current_tag_names, None, &tag_name)?;
                current_tag_names.push(tag_name.clone());
                tag_config.push(TagConfig {
                    column_name: tag_name,
                    column_type,
                });
            }
            column += 1;
            column_set.remove(&column_name.to_string());
        }
        if column_set.len() != 0 {
            anyhow::bail!(
                "csv config miss column: {}",
                column_set.iter().next().unwrap()
            );
        }
        let mut line = 3;
        let mut column_config_init = false;
        while let Some(record) = records.next().await {
            match record {
                Ok(record) => {
                    let mut record_map = HashMap::new(); // column_name, column_data
                    let mut tag_values_map = HashMap::new();
                    for (index, column_name) in column_map.iter() {
                        let data = record.get(index.clone()).unwrap();
                        if column_name.starts_with("tag::") {
                            tag_values_map.insert(
                                column_name
                                    .split("::")
                                    .collect_vec()
                                    .get(2)
                                    .unwrap()
                                    .to_string(),
                                data.to_string(),
                            );
                        } else {
                            record_map.insert(column_name.to_string(), data.to_string());
                        }
                    }

                    let point_id = record_map.get_mut("point_id").unwrap();
                    let pointid = point_id.clone();
                    let tb_name = record_map.get_mut("tbname").unwrap();
                    if tb_name.contains("{") {
                        // maybe should use pattern match?
                        *tb_name = generate_tbname_from_pattern(ty, tb_name, &pointid);
                    }
                    let point_id = record_map.get("point_id").unwrap();
                    let stable = if let Some(stable_name) = record_map.get("stable") {
                        Some(stable_name.clone())
                    } else {
                        if stable_prefix.is_none() {
                            stable_prefix = Some(String::from("opc"));
                        }
                        None
                    };
                    let code = record_map.get("tbname").unwrap();
                    let enabled_column = record_map.get("enabled");
                    if enabled_column.is_some() {
                        let enabled = enabled_column.unwrap();
                        if enabled == "0" {
                            // warn: should delete subtable (stable_code)
                            tables_to_drop.push(format!("{code}"));
                            continue;
                        }
                    }
                    let column_type = if let Some(ty) = record_map.get("type") {
                        Some(
                            IpcDataType::from_str(ty)
                                .map_err(|err| anyhow::Error::msg(err.clone()))?,
                        )
                    } else {
                        None
                    };
                    let mut current_columns = Vec::new();
                    if !column_config_init {
                        let value_column_name = record_map
                            .get("value_col")
                            .unwrap_or(&"val".to_string())
                            .clone();
                        check_duplicated(
                            &current_tag_names,
                            Some(&current_columns),
                            &value_column_name,
                        )?;
                        current_columns.push(value_column_name.clone());
                        column_config.push(ColumnConfig {
                            column_name: "value".to_string(),
                            column_type: None,
                            column_alias: Some(value_column_name.clone()),
                            is_primary_key: false,
                        });
                        let quality_col_name = record_map
                            .get("quality_col")
                            .unwrap_or(&"quality".to_string())
                            .clone();
                        check_duplicated(
                            &current_tag_names,
                            Some(&current_columns),
                            &quality_col_name,
                        )?;
                        current_columns.push(quality_col_name.clone());
                        column_config.push(ColumnConfig {
                            column_name: "quality".to_string(),
                            column_type: Some(Ty::Int),
                            column_alias: Some(quality_col_name.clone()),
                            is_primary_key: false,
                        });
                        let received_ts_col = record_map
                            .get("received_ts_col")
                            .or(record_map.get("received_time_col"));
                        let mut has_primary_key = false;
                        if received_ts_col.is_some() {
                            let received_ts_col_name = record_map
                                .get("received_ts_col")
                                .or(record_map.get("received_time_col"))
                                .unwrap_or(&"received_ts".to_string())
                                .clone();
                            check_duplicated(
                                &current_tag_names,
                                Some(&current_columns),
                                &received_ts_col_name,
                            )?;
                            current_columns.push(received_ts_col_name.clone());
                            has_primary_key = true;
                            column_config.push(ColumnConfig {
                                column_name: "received_ts".to_string(),
                                column_type: Some(Ty::Timestamp),
                                column_alias: Some(received_ts_col_name),
                                is_primary_key: has_primary_key,
                            });
                        }
                        let ts_col_name = record_map
                            .get("ts_col")
                            .unwrap_or(&"ts".to_string())
                            .clone();
                        check_duplicated(&current_tag_names, Some(&current_columns), &ts_col_name)?;
                        current_columns.push(ts_col_name.clone());
                        column_config.push(ColumnConfig {
                            column_name: "original_ts".to_string(),
                            column_type: Some(Ty::Timestamp),
                            column_alias: Some(ts_col_name),
                            is_primary_key: !has_primary_key,
                        });
                        column_config_init = true;
                    }

                    let tag_values = if tag_values_map.len() == 0 {
                        None
                    } else {
                        Some(tag_values_map)
                    };
                    id_code_map.insert(
                        point_id.clone(),
                        PointConfig {
                            code: code.clone(),
                            stable: stable,
                            tag_values,
                            value_type: column_type,
                        },
                    );
                    node_config_old.push(format!("{point_id}::{code}"))
                }
                Err(_e) => tracing::warn!("line {line} have different with other previous lines ",),
            }
            line += 1;
        }
    }
    let tag_configs = if tag_config.len() == 0 {
        None
    } else {
        Some(tag_config)
    };
    return Ok((
        OpcTableConfig {
            id_code_map,
            table_config: TableConfig {
                stable_prefix,
                column_configs: column_config,
                tag_configs,
            },
        },
        node_config_old,
        tables_to_drop,
    ));
}

fn check_duplicated(
    current_tags: &Vec<String>,
    current_columns: Option<&Vec<String>>,
    column_name: &String,
) -> anyhow::Result<()> {
    if current_tags.contains(column_name) {
        anyhow::bail!("duplicated column or tag: {column_name}")
    }
    if current_columns.is_some() && current_columns.unwrap().contains(column_name) {
        anyhow::bail!("duplicated column or tag: {column_name}")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::*;

    #[tokio::test]
    async fn test_opc_config_to_toml() {
        let mut map = HashMap::new();
        map.insert(
            String::from("123"),
            PointConfig {
                code: "567".to_string(),
                stable: None,
                tag_values: None,
                value_type: None,
            },
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
                    connect_timeout: 10,
                    request_timeout: 20,
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
                dump: Some(DumpConfig {
                    enable: true,
                    path: Some("/usr/loacl/taosx/".to_string()),
                    keep: Some(10 as usize),
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

[collect.dump]
enable = true
path = "/usr/loacl/taosx/"
keep = 10

[report]
remote = "remote.remote"
concurrent = 10
batch_timeout = 100
"#,
            toml
        );
    }
}