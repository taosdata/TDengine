use crate::runners::log_rotation;
use crate::runners::opc::config::{OPCConfig, PointsMode};
use crate::runners::opc::model::OpcModelConfig;
use crate::utils::dsn::json_to_dsn;
use crate::{DataSet, DataSetsReq};
use anyhow::{bail, Context};
use csv::CsvParser;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::{io::prelude::*, path::PathBuf};
use taos::{Dsn, IntoDsn};
use taosx_ipc::prelude::IpcDataType;
use taosx_ipc::types::OptionSet;
use tempfile::NamedTempFile;

pub mod config;
pub mod csv;
pub mod model;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum OpcType {
    OPCUA,
    OPCDA,
    FAKE,
}

impl OpcType {
    /// valid dsn driver:
    /// opcua:// -> OPCUA
    /// opcda:// -> OPCDA
    /// fake:// -> FAKE
    /// opc+ua:// -> OPCUA
    /// opc+da:// -> OPCDA
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let fake = dsn.params.contains_key("fake");
        if fake {
            return Ok(Self::FAKE);
        }
        let opc_type = dsn.driver.to_lowercase();
        let protocol = dsn.protocol.clone();
        match opc_type.as_str() {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            "fake" => Ok(Self::FAKE),
            "opc" => match protocol.as_deref() {
                Some("ua") => Ok(Self::OPCUA),
                Some("da") => Ok(Self::OPCDA),
                _ => bail!("unknown opc protocol"),
            },
            _ => bail!("invalid opc type"),
        }
    }

    pub fn as_static_str(&self) -> &'static str {
        match self {
            OpcType::OPCUA => "opcua",
            OpcType::OPCDA => "opcda",
            OpcType::FAKE => "fake",
        }
    }
}

impl TryFrom<&str> for OpcType {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            "fake" => Ok(Self::FAKE),
            _ => bail!("invalid opc type: {}", value),
        }
    }
}

impl Display for OpcType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::OPCUA => "opcua",
            Self::OPCDA => "opcda",
            Self::FAKE => "fake",
        };
        write!(f, "{}", s)
    }
}

const EXE: &str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "taosx-opc.exe"
        } else {
            "taosx-opc"
        }
    }
};

/// taosx-opc executable path
pub fn exe_path() -> anyhow::Result<PathBuf> {
    let path = super::get_plugin_dir("opc").join(EXE);
    if !path.exists() {
        return Err(anyhow::anyhow!("opc plugin not found at: {:?}", path));
    }
    Ok(path)
}

/// taosx-opc version
pub fn info() -> anyhow::Result<(&'static str, PathBuf, String)> {
    let path = exe_path()?;
    let output = std::process::Command::new(&path).arg("version").output()?;
    Ok((
        "opc",
        path,
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}

/// OPC UA: <table_prefix>_{ns}_{id}_<table_suffix>
/// OPC DA: <table_prefix>_{tag_name/TagName}_<table_suffix>
fn generate_tbname_from_pattern(ty: &str, tb_name: &str, point_id: &str) -> String {
    let tbname = match ty {
        "opcua" => {
            // ns=13;i=1003
            // ns=6;s=Scalar_Instructions
            // ns=6;g=00000000-0000-0000-0000-000000009204
            // ns=6;b=CQIABQ==
            if let Some((ns, id)) = point_id.split_once(";") {
                let ns = if ns.contains("ns=") {
                    let (_, ns) = ns.split_once("=").unwrap();
                    ns
                } else {
                    ns
                };
                let id = if let Some((_, id)) = id.split_once('=') {
                    id
                } else {
                    id
                };
                assert!(!id.is_empty(), "id should not be empty: {}", point_id);
                tb_name.replace("{ns}", ns).replace("{id}", id)
            } else {
                assert!(!point_id.is_empty(), "id should not be empty: {}", point_id);
                tb_name.replace("{ns}", "0").replace("{id}", point_id)
            }
        }
        "opcda" => {
            if tb_name.contains("{TagName}") || tb_name.contains("{tag_name}") {
                let tag_index = point_id.rfind(".");
                let tag_name = if let Some(index) = tag_index {
                    // should be Device.DeviceType.TagName pattern
                    &point_id[index + 1..]
                } else {
                    point_id
                };
                let tb_name = tb_name.replace("{TagName}", tag_name);
                tb_name.replace("{tag_name}", tag_name)
            } else if tb_name.contains("{/tag_name}") {
                let tag_index = point_id.rfind("/");
                let tag_name = if let Some(index) = tag_index {
                    // should be Device/DeviceType/TagName pattern
                    &point_id[index + 1..]
                } else {
                    point_id
                };
                tb_name.replace("{/tag_name}", tag_name)
            } else if tb_name.contains("{id}") {
                tb_name.replace("{id}", point_id)
            } else if tb_name.contains("{_id}") {
                tb_name.replace("{_id}", &point_id.replace("/", "_"))
            } else {
                tb_name.to_string()
            }
        }
        _ => tb_name.to_string(),
    };

    tbname.replace(".", "_").replace("`", "_")
}

/// OPC UA: {ns} {id}
/// OPC DA: {tag_name/TagName}
fn generate_tag_value_from_pattern(ty: &str, tb_name: &str, point_id: &str) -> String {
    match ty {
        "opcua" => {
            // ns=13;i=1003
            // ns=6;s=Scalar_Instructions
            // ns=6;g=00000000-0000-0000-0000-000000009204
            // ns=6;b=CQIABQ==
            if let Some((ns, id)) = point_id.split_once(";") {
                let ns = if ns.contains("ns=") {
                    let (_, ns) = ns.split_once("=").unwrap();
                    ns
                } else {
                    ns
                };
                let id = if let Some((_, id)) = id.split_once('=') {
                    id
                } else {
                    id
                };
                assert!(!id.is_empty(), "id should not be empty: {}", point_id);
                tb_name.replace("{ns}", ns).replace("{id}", id)
            } else {
                assert!(!point_id.is_empty(), "id should not be empty: {}", point_id);
                tb_name.replace("{ns}", "0").replace("{id}", point_id)
            }
        }
        "opcda" => {
            if tb_name.contains("{TagName}") || tb_name.contains("{tag_name}") {
                let tag_index = point_id.rfind(".");
                let tag_name = if let Some(index) = tag_index {
                    // should be Device.DeviceType.TagName pattern
                    &point_id[index + 1..]
                } else {
                    point_id
                };
                let tb_name = tb_name.replace("{TagName}", tag_name);
                tb_name.replace("{tag_name}", tag_name)
            } else if tb_name.contains("{/tag_name}") {
                let tag_index = point_id.rfind("/");
                let tag_name = if let Some(index) = tag_index {
                    // should be Device/DeviceType/TagName pattern
                    &point_id[index + 1..]
                } else {
                    point_id
                };
                tb_name.replace("{/tag_name}", tag_name)
            } else if tb_name.contains("{id}") {
                tb_name.replace("{id}", point_id)
            } else if tb_name.contains("{_id}") {
                tb_name.replace("{_id}", &point_id.replace("/", "_"))
            } else {
                tb_name.to_string()
            }
        }
        _ => tb_name.to_string(),
    }
}
fn generate_stable_from_pattern(stable_expr: &str, value_type: &Option<IpcDataType>) -> String {
    let mut stable = stable_expr.to_string();
    if stable_expr.contains(".") {
        stable = stable.replace(".", "_");
    }

    if let Some(t) = value_type {
        stable = match t {
            IpcDataType::VarChar(_len) => stable.replace("{type}", "varchar"),
            IpcDataType::NChar(_len) => stable.replace("{type}", "nchar"),
            _ => stable.replace("{type}", &t.sql_repr().replace(" ", "_")),
        };
    }

    stable
}

/// 解析为文件路径: 如果以@开头，表示文件路径, 返回 None;
/// 否则，认为参数值是文件内容，写入临时文件后，返回 NamedTempFile。
fn get_temp_file(dsn: &Dsn, key: &str) -> Option<NamedTempFile> {
    dsn.get(key).and_then(|v| {
        if v.is_empty() || v.starts_with('@') {
            return None;
        }

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(v.as_bytes()).unwrap();
        Some(file)
    })
}

/// 获取 opc 点位
pub async fn opc_datasets(req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let req_clone = req.clone();
    let from = if let Some(from_json) = req_clone.from_json {
        json_to_dsn(&from_json)?
    } else if let Some(from) = req_clone.from {
        from.into_dsn()?
    } else {
        anyhow::bail!("from is required");
    };

    if req.categories.is_empty() {
        anyhow::bail!("categories is empty");
    }

    opc_datasets_impl(from).await
}

async fn opc_datasets_impl(from: Dsn) -> anyhow::Result<Vec<DataSet>> {
    let certificate = get_temp_file(&from, "certificate");
    let private_key = get_temp_file(&from, "private_key");
    let auth_certificate = get_temp_file(&from, "auth_certificate");
    let auth_private_key = get_temp_file(&from, "auth_private_key");

    let points_mode = PointsMode::from_dsn(&from)?;
    let opc_points = match points_mode {
        // 解析 csv 文件中的点位
        PointsMode::ByCsv => {
            let opc_type = OpcType::from_dsn(&from)?;
            let csv_files = OPCConfig::parse_csv_config_files(&from).ok_or(anyhow::anyhow!(
                "csv_config_file not found in dsn: {}",
                from.to_string()
            ))?;

            let parser = CsvParser::try_new(opc_type, csv_files)?;
            let model_config = parser.parse().await?;
            to_opc_dataset_vec(&model_config).await?
        }
        // 通过 taosx-opc points 命令获取点位
        PointsMode::ByCommand => {
            let mut config = OPCConfig::from_dsn_point_mode(&from)?;

            config.set_temp_filepath("certificate", certificate.as_ref())?;
            config.set_temp_filepath("private_key", private_key.as_ref())?;
            config.set_temp_filepath("auth_certificate", auth_certificate.as_ref())?;
            config.set_temp_filepath("auth_private_key", auth_private_key.as_ref())?;

            opc_datasets_by_command(&config).await?
        }
    };

    certificate.map(|f| f.close());
    private_key.map(|f| f.close());
    auth_certificate.map(|f| f.close());
    auth_private_key.map(|f| f.close());

    Ok(opc_points)
}

/// 从 model_config 中提取所有 OPC 点位
async fn to_opc_dataset_vec(model_config: &OpcModelConfig) -> anyhow::Result<Vec<DataSet>> {
    let mut datasets = vec![];
    for (point_id, point_config) in model_config.point_config_map.iter() {
        let point_type = point_config.value_type.as_ref().map(|v| v.to_string());
        let name = point_config.tag_values.as_ref().and_then(|tag_values| {
            tag_values.iter().find_map(|(tag_name, tag_value)| {
                if tag_name == "name" {
                    Some(tag_value.to_string())
                } else {
                    None
                }
            })
        });
        let table_config = model_config.table_config_map.get(point_id).unwrap();
        let display = table_config.enabled.unwrap_or(1).to_string();
        let options = vec![OptionSet {
            name: "enabled".to_string(),
            display,
            description: None,
            required: false,
        }];

        let ds = DataSet {
            id: point_id.clone(),
            name,
            category: None,
            r#type: point_type,
            options: Some(options),
            format: None,
        };
        datasets.push(ds);
    }

    Ok(datasets)
}

/// 通过执行 taosx-opc points 命令获取 opc 点位
async fn opc_datasets_by_command(config: &OPCConfig) -> anyhow::Result<Vec<DataSet>> {
    let toml =
        toml::to_string(&config).with_context(|| "toml to_string error encountered".to_string())?;
    let mut config_file = NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    tracing::info!(
        "execute: taosx-opc points, opc config: {}\n{}",
        config_path.display(),
        toml
    );

    let mut command = tokio::process::Command::new(exe_path()?);
    let output = command
        .arg("points")
        .arg(format!("--conf={}", &config_path.display()))
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .with_context(|| "Start OPC collector error")?;
    let mut log_path = super::get_log_dir("");
    std::fs::create_dir_all(&log_path)
        .with_context(|| format!("Log path {}", log_path.display()))?;
    log_path.push("opc.log");

    let mut log_rotation = log_rotation(&log_path, 700);

    write!(log_rotation, "{}", String::from_utf8_lossy(&output.stderr))
        .context("writing logs error")?;

    tracing::info!("opc_datasets OPC exit with status {}", output.status);
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        tracing::error!(
            plugin = "opc",
            module = "datasets",
            stdout = ?bytes::Bytes::from(output.stdout),
            "Get OPC datasets error:\n{}",
            error
        );
        let error = filter_opc_log(error.to_string()).await;

        let pattern =
            regex::Regex::new(r#"level=PANIC msg="(?P<msg>.*)" error="(?<error>.*)"#).unwrap();
        let matches = pattern.captures(&error);
        if let Some(matches) = matches {
            anyhow::bail!("{}: {}", &matches["msg"], &matches["error"]);
        } else {
            anyhow::bail!("Get OPC datasets error: {}", &error);
        }
    }
    temp_path.close()?;

    let res: Vec<DataSet> = serde_json::from_slice(&output.stdout)?;
    Ok(res)
}

/// 过滤 opc 错误日志，去掉 info 日志
pub async fn filter_opc_log<S: AsRef<str>>(error_log: S) -> String {
    let mut error = String::new();
    for line in error_log.as_ref().lines() {
        if line.contains(" info ") || line.contains(" trace ") || line.contains(" debug ") {
            continue;
        }
        error.push_str(line);
        error.push('\n');
    }
    error.trim_end().to_string() // remove last '\n'
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_get_temp_file() {
        std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());

        let dsn = Dsn::from_str("opcua://").unwrap();
        let file = get_temp_file(&dsn, "certificate");
        assert!(file.is_none());

        let dsn = Dsn::from_str("opcua://?certificate=").unwrap();
        let file = get_temp_file(&dsn, "certificate");
        assert!(file.is_none());

        let dsn = Dsn::from_str("opcua://?certificate=hello\nworld").unwrap();
        let mut file = get_temp_file(&dsn, "certificate").unwrap();
        let mut content = String::new();
        file.as_file_mut()
            .seek(std::io::SeekFrom::Start(0))
            .unwrap();
        file.read_to_string(&mut content).unwrap();
        assert_eq!(content, "hello\nworld");

        let dsn = Dsn::from_str("opcua://?certificate=@./tests/opc/certificate.crt").unwrap();
        let file = get_temp_file(&dsn, "certificate");
        assert!(file.is_none());
    }

    #[test]
    fn test_generate_tbname_from_pattern() {
        // OPC UA
        assert_eq!(
            generate_tbname_from_pattern("opcua", "t_{ns}_{id}", "ns=13;i=10003"),
            "t_13_10003"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcua", "t_{ns}_{id}", "ns=13;b=GCC"),
            "t_13_GCC"
        );
        assert_eq!(
            generate_tbname_from_pattern(
                "opcua",
                "t_{ns}_{id}",
                "ns=13;g=00000000-0000-0000-0000-000000009204"
            ),
            "t_13_00000000-0000-0000-0000-000000009204"
        );
        assert_eq!(
            generate_tbname_from_pattern(
                "opcua",
                "t_{ns}_{id}",
                r#"ns=3;s=Special_\"!§$%&/()=?`´\\+~*'#_-:.;,<>|@^°€µ{[]}"#
            ),
            r#"t_3_Special_\"!§$%&/()=?_´\\+~*'#_-:_;,<>|@^°€µ{[]}"#
        );

        // OPC DA
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{TagName}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{tag_name}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{/tag_name}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_EDCGQ_MP706AT_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{id}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_/ASSETS/AB/EDCGQ_MP706AT_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t{_id}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_ASSETS_AB_EDCGQ_MP706AT_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{TagName}", "02_LI7059.DACA.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{tag_name}", "02_LI7059.DACA.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{/tag_name}", "02_LI7059.DACA.PV"),
            "t_02_LI7059_DACA_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{id}", "02_LI7059.DACA.PV"),
            "t_02_LI7059_DACA_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{_id}", "02_LI7059.DACA.PV"),
            "t_02_LI7059_DACA_PV"
        );
    }

    #[test]
    fn test_opc_type() {
        let dsn = Dsn::from_str("opcua://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::OPCUA);

        let dsn = Dsn::from_str("opcda://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::OPCDA);

        let dsn = Dsn::from_str("opc+ua://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::OPCUA);

        let dsn = Dsn::from_str("opc+da://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::OPCDA);

        let dsn = Dsn::from_str("fake://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::FAKE);

        let dsn = Dsn::from_str("opc://?fake=true").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::FAKE);

        let dsn = Dsn::from_str("opc://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn);
        assert!(opc_type.is_err());
        assert_eq!("unknown opc protocol", opc_type.unwrap_err().to_string());
    }

    #[tokio::test]
    async fn test_filter_opc_log() {
        let log = r#"
10/28 18:48:50.269658 00071305 info "get max node per read success 10000" id=0 model=points
10/28 18:48:50.269698 00071305 panic "get all points error" error=invalid points regex: error parsing regexp: invalid or unsupported Perl syntax: `(?!` model=points
panic: (*logrus.Entry) 0xc00034aaf0
        "#;
        let expect = r#"
10/28 18:48:50.269698 00071305 panic "get all points error" error=invalid points regex: error parsing regexp: invalid or unsupported Perl syntax: `(?!` model=points
panic: (*logrus.Entry) 0xc00034aaf0"#.to_string();
        let res = filter_opc_log(log).await;
        assert_eq!(res, expect);
    }

    /// # Example
    /// ```shell
    /// OPC_SERVER="192.168.2.16:53530/OPCUA/SimulationServer" PLUGINS_HOME=/Users/yangzy/RustProjects/taosx/plugins LOGS_HOME=/Users/yangzy/taosx/log cargo nextest run -p taosx-core test_opc_datasets_by_command --nocapture --retries 0
    /// ```
    #[tokio::test]
    async fn test_opc_datasets_by_command() {
        if let Ok(opc_server) = std::env::var("OPC_SERVER") {
            let dsn = format!("opcua://{opc_server}?browse_name_pattern=\"数据块_1\".\"Tag\\d+\"",)
                .into_dsn()
                .unwrap();
            let config = OPCConfig::from_dsn_point_mode(&dsn).unwrap();

            let datasets = opc_datasets_by_command(&config).await.unwrap();
            dbg!(&datasets);
        }
    }
}
