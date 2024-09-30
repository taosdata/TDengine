use crate::utils::dsn::DsnParamGetter;
use anyhow::{anyhow, Context};
use std::str::FromStr;
use taos::Dsn;
use toml::value::Datetime;

use crate::get_data_dir;

#[derive(Debug, serde::Serialize)]
pub struct PiConfig {
    // system
    #[serde(rename = "PIServerName")]
    server_name: String,
    #[serde(rename = "PISystemName", skip_serializing_if = "Option::is_none")]
    system_name: Option<String>,
    #[serde(rename = "AFDatabaseName", skip_serializing_if = "Option::is_none")]
    database: Option<String>,
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
    pub ipc_stream: String,
    #[serde(rename = "SQLAPI")]
    pub sql_api: String,
    #[serde(rename = "TDDataBase")]
    td_database: String,
    // data set
    #[serde(rename = "TemplateForPIPoint")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_pi_point: Vec<String>,
    #[serde(rename = "TemplateForAFElement")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_af_element: Vec<String>,
    #[serde(rename = "ElementIDList")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub element_id_list: Vec<String>,
    #[serde(rename = "PointList")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub point_list: Vec<String>,
    // backfill param
    #[serde(rename = "ForBackfill")]
    pub for_backfill: bool, // 本配置是否针对 Backfill 任务
    #[serde(
        rename = "BackfillBreakpointFile",
        skip_serializing_if = "Option::is_none"
    )]
    backfill_breakpoint_file: Option<String>,
    #[serde(rename = "FromTDengineLastTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    from_tdengine_last_time: Option<bool>,
    #[serde(rename = "ToTDengineFirstTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    to_tdengine_first_time: Option<bool>,
    #[serde(rename = "BackfillStartTime", skip_serializing_if = "Option::is_none")]
    pub backfill_start_time: Option<Datetime>,
    #[serde(rename = "BackfillEndTime", skip_serializing_if = "Option::is_none")]
    pub backfill_end_time: Option<Datetime>,
    // log level
    #[serde(rename = "LogLevel", skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,
    #[serde(rename = "TaskID", skip_serializing_if = "Option::is_none")]
    task_id: Option<i64>,

    #[serde(rename = "SyncAddElement", skip_serializing_if = "Option::is_none")]
    sync_add_element: Option<bool>,

    #[serde(rename = "SyncDeleteElement", skip_serializing_if = "Option::is_none")]
    sync_delete_element: Option<bool>,

    #[serde(
        rename = "SyncUpdateAttribute",
        skip_serializing_if = "Option::is_none"
    )]
    sync_update_attribute: Option<bool>,

    #[serde(rename = "SyncUpdateData", skip_serializing_if = "Option::is_none")]
    sync_update_data: Option<bool>,

    #[serde(rename = "SyncDeleteData", skip_serializing_if = "Option::is_none")]
    sync_delete_data: Option<bool>,
}

impl PiConfig {
    pub fn parse_connection(
        dsn: &Dsn,
        td_database: String,
        ipc: u16,
        sql: u16,
    ) -> anyhow::Result<PiConfig> {
        let server_name = Self::parse_server_name(dsn)?;
        let pi_config = Self {
            server_name: server_name.clone(),
            system_name: Self::parse_system_name(dsn),
            database: Self::parse_database(dsn),
            pi_data_pipes_instances: Self::parse_pi_data_pipes_instances(dsn)?,
            af_data_pipes_instances: Self::parse_af_data_pipes_instances(dsn)?,
            max_wait_len: Self::parse_max_wait_len(dsn)?,
            update_interval: Self::parse_update_interval(dsn)?,
            max_backfill_range_days: Self::parse_max_backfill_range_days(dsn)?,
            ipc_stream: format!("127.0.0.1:{ipc}"),
            sql_api: format!("http://127.0.0.1:{sql}"),
            td_database,
            template_for_pi_point: Vec::new(),
            template_for_af_element: Vec::new(),
            element_id_list: Vec::new(),
            point_list: Vec::new(),
            for_backfill: false,
            backfill_breakpoint_file: None,
            from_tdengine_last_time: Self::parse_from_tdengine_last_time(dsn)?,
            to_tdengine_first_time: Self::parse_to_tdengine_first_time(dsn)?,
            backfill_start_time: Self::parse_backfill_start_time(dsn)?,
            backfill_end_time: Self::parse_backfill_end_time(dsn)?,
            log_level: Self::parse_log_level(dsn)?,
            task_id: None,
            sync_add_element: None,
            sync_delete_element: None,
            sync_update_attribute: None,
            sync_update_data: None,
            sync_delete_data: None,
        };

        Ok(pi_config)
    }
    pub async fn new(
        from: Dsn,
        td_database: String,
        ipc_port: u16,
        sql_port: u16,
        task_id: Option<i64>,
    ) -> anyhow::Result<PiConfig> {
        let server_name = Self::parse_server_name(&from)?;
        let system_name = Self::parse_system_name(&from);
        let database = Self::parse_database(&from);
        let pi_data_pipes_instances = Self::parse_pi_data_pipes_instances(&from)?;
        let af_data_pipes_instances = Self::parse_af_data_pipes_instances(&from)?;
        let max_wait_len = Self::parse_max_wait_len(&from)?;
        let update_interval = Self::parse_update_interval(&from)?;
        let max_backfill_range_days = Self::parse_max_backfill_range_days(&from)?;
        let mut backfill_breakpoint_file = None;
        let for_backfill = match from.driver.as_str() {
            "pibackfill" => {
                backfill_breakpoint_file = Self::check_backfill_breakpoint_file(task_id);
                true
            }
            _ => false,
        };
        let (element_id_list, point_list, template_list) = {
            let transform_config_file = from
                .params
                .get("transform_config_file")
                .ok_or(anyhow!("No param transform_config_file in from DSN"))?;
            let transform_config_file = transform_config_file.trim_start_matches('@');
            let config_file_full_path = get_data_dir()
                .join(transform_config_file)
                .display()
                .to_string();
            Self::parse_transform_config_file(config_file_full_path.as_str()).with_context(
                || {
                    format!(
                        "Failed to parse transform config file: {}",
                        transform_config_file
                    )
                },
            )?
        };

        let mut from_tdengine_last_time = Self::parse_from_tdengine_last_time(&from)
            .context("Failed to parse_from_tdengine_last_time")?;
        let mut to_tdengine_first_time = Self::parse_to_tdengine_first_time(&from)
            .context("Failed to parse_to_tdengine_first_time")?;
        let backfill_start_time = if let Some(backfill_start) =
            from.params.get("BackfillStartTime").map(|v| v.to_string())
        {
            if backfill_start == "auto" {
                from_tdengine_last_time.replace(true);
                None
            } else {
                let parsed_time = Datetime::from_str(backfill_start.as_str()).map_err(|err| {
                    anyhow!(
                        "invalid BackfillStartTime: {}, cause: {}",
                        backfill_start.clone(),
                        err.to_string()
                    )
                })?;
                Some(parsed_time)
            }
        } else {
            None
        };
        let backfill_end_time =
            if let Some(backfill_end) = from.params.get("BackfillEndTime").map(|v| v.to_string()) {
                if backfill_end == "auto" {
                    to_tdengine_first_time.replace(true);
                    None
                } else {
                    let parsed_time = Datetime::from_str(backfill_end.as_str()).map_err(|err| {
                        anyhow!(
                            "invalid BackfillEndTime: {}, cause: {}",
                            backfill_end.clone(),
                            err.to_string()
                        )
                    })?;
                    Some(parsed_time)
                }
            } else {
                None
            };

        if from_tdengine_last_time.eq(&Some(true)) && to_tdengine_first_time.eq(&Some(true)) {
            return Err(anyhow!(
                "Only one of the BackfillStartTime and BackfillEndTime can be automatically set"
            ));
        }

        let log_level = Self::parse_log_level(&from)?;

        let (template_for_pi_point, template_for_af_element) = if point_list.is_empty() {
            (Vec::new(), template_list)
        } else {
            (template_list, Vec::new())
        };
        let mut sync_add_element = None;
        let mut sync_delete_element = None;
        let mut sync_update_attribute = None;
        let mut sync_update_data = None;
        let mut sync_delete_data = None;
        if !for_backfill {
            sync_add_element = from.get_bool("sync_add_element")?;
            sync_delete_element = from.get_bool("sync_delete_element")?;
            sync_update_attribute = from.get_bool("sync_update_attribute")?;
            sync_update_data = from.get_bool("sync_update_data")?;
            sync_delete_data = from.get_bool("sync_delete_data")?;
        }
        Ok(Self {
            server_name,
            system_name,
            database,
            pi_data_pipes_instances,
            af_data_pipes_instances,
            max_wait_len,
            update_interval,
            max_backfill_range_days,
            ipc_stream: format!("127.0.0.1:{}", ipc_port),
            sql_api: format!("http://127.0.0.1:{}", sql_port),
            td_database,
            template_for_pi_point,
            template_for_af_element,
            element_id_list,
            for_backfill,
            backfill_breakpoint_file,
            point_list,
            from_tdengine_last_time,
            to_tdengine_first_time,
            backfill_start_time,
            backfill_end_time,
            log_level,
            task_id,
            sync_add_element,
            sync_delete_element,
            sync_update_attribute,
            sync_update_data,
            sync_delete_data,
        })
    }

    fn check_backfill_breakpoint_file(task_id: Option<i64>) -> Option<String> {
        let data_dir = get_data_dir();
        let breakpoint_file = data_dir
            .join("tasks")
            .join(task_id.unwrap().to_string())
            .join("breakpoints.csv");
        if breakpoint_file.exists() {
            Some(breakpoint_file.display().to_string())
        } else {
            None
        }
    }

    fn parse_server_name(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or(anyhow!("PIServerName is required"))
    }

    fn parse_system_name(dsn: &Dsn) -> Option<String> {
        dsn.params.get("PISystemName").map(|v| v.to_string())
    }

    fn parse_database(dsn: &Dsn) -> Option<String> {
        dsn.subject.clone()
    }

    fn parse_pi_data_pipes_instances(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        dsn.params
            .get("PIDataPipesInstances")
            .map(|v| {
                v.parse::<u32>().map_err(|err| {
                    anyhow!("invalid PIDataPipesInstances, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_af_data_pipes_instances(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        dsn.params
            .get("AFDataPipesInstances")
            .map(|v| {
                v.parse::<u32>().map_err(|err| {
                    anyhow!("invalid AFDataPipesInstances, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_max_wait_len(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        let mut max_wait_len = dsn
            .params
            .get("MaxWaitLen")
            .map(|v| {
                v.parse::<u32>()
                    .map_err(|err| anyhow!("invalid MaxWaitLen, cause: {}", err.to_string()))
            })
            .transpose()?;
        if max_wait_len.is_none() {
            max_wait_len = dsn
                .params
                .get("batch_size")
                .map(|v| {
                    v.parse::<u32>()
                        .map_err(|err| anyhow!("invalid batch_size, cause: {}", err.to_string()))
                })
                .transpose()?;
        }
        if let Some(mwl) = max_wait_len {
            if !(1..=10000).contains(&mwl) {
                return Err(anyhow!("MaxWaitLen should be in range 1..10000"));
            }
        }
        Ok(max_wait_len)
    }

    fn parse_update_interval(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        let mut update_interval = dsn
            .params
            .get("UpdateInterval")
            .map(|v| {
                v.parse::<u32>()
                    .map_err(|err| anyhow!("invalid UpdateInterval, cause: {}", err.to_string()))
            })
            .transpose()?;
        if update_interval.is_none() {
            update_interval = dsn
                .params
                .get("batch_timeout")
                .map(|v| {
                    v.parse::<u32>()
                        .map_err(|err| anyhow!("invalid batch_timeout, cause: {}", err.to_string()))
                        .map(|v| v * 1000)
                })
                .transpose()?;
        }
        if let Some(ui) = update_interval {
            if !(100..=60000).contains(&ui) {
                return Err(anyhow!("UpdateInterval should be in range 100..60000 ms"));
            }
        }
        Ok(update_interval)
    }

    fn parse_max_backfill_range_days(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        dsn.params
            .get("MaxBackfillRangeDays")
            .map(|s| {
                let result = parse_duration::parse(s);
                match result {
                    Ok(d) => Ok(Some(d.as_secs().div_ceil(60) as u32)),
                    Err(e) => Err(anyhow!("invalid max_backfill_range: {}, cause: {}", s, e)),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_from_tdengine_last_time(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        if dsn.params.get("BackfillStartTime").map(|s| s.as_str()) == Some("auto") {
            return Ok(Some(true));
        }
        dsn.params
            .get("FromTDengineLastTime")
            .map(|v| {
                v.parse::<bool>().map_err(|err| {
                    anyhow!("invalid FromTDengineLastTime, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_to_tdengine_first_time(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        if dsn.params.get("BackfillEndTime").map(|s| s.as_str()) == Some("auto") {
            return Ok(Some(true));
        }
        dsn.params
            .get("ToTDengineFirstTime")
            .map(|v| {
                v.parse::<bool>().map_err(|err| {
                    anyhow!("invalid ToTDengineFirstTime, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_backfill_start_time(dsn: &Dsn) -> anyhow::Result<Option<Datetime>> {
        match dsn.params.get("BackfillStartTime").map(|s| s.trim()) {
            Some("auto") | None => Ok(None),
            Some(v) => Self::parse_date_time(v)
                .map_err(|err| anyhow!("invalid BackfillStartTime, cause: {}", err.to_string()))
                .map(Some),
        }
    }

    fn parse_backfill_end_time(dsn: &Dsn) -> anyhow::Result<Option<Datetime>> {
        match dsn.params.get("BackfillEndTime").map(|s| s.as_str()) {
            Some("auto") | None => Ok(None),
            Some(v) => Self::parse_date_time(v)
                .map_err(|err| anyhow!("invalid BackfillEndTime, cause: {}", err.to_string()))
                .map(Some),
        }
    }

    fn parse_date_time(date_time: &str) -> anyhow::Result<Datetime> {
        let parsed_time = Datetime::from_str(date_time).map_err(|err| {
            anyhow!(
                "failed to parse date time: {}, cause: {}",
                date_time,
                err.to_string()
            )
        })?;

        Ok(parsed_time)
    }

    fn parse_log_level(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        dsn.params
            .get("LogLevel")
            .or(dsn.params.get("log_level"))
            .map(|v| match v.trim() {
                "trace" | "debug" | "info" | "warn" | "error" => Ok(v.trim().to_string()),
                _ => Err(anyhow!(
                    "invalid log_level, cause: provided `{v}`, but expects one of [trace, debug, info, warn, error]",
                )),
            })
            .transpose()
    }

    pub fn parse_transform_config_file(
        transform_config_file: &str,
    ) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>)> {
        let content = std::fs::read_to_string(transform_config_file)?;
        let mut element_id_list = Vec::new();
        let mut point_list = Vec::new();
        let mut template_list = std::collections::BTreeSet::<String>::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() >= 2 {
                let object_name = parts[0].to_lowercase();
                if object_name == "template" {
                    template_list.insert(parts[1].to_string());
                    continue;
                }
                let object_type = parts[1].to_lowercase();
                match object_type.as_str() {
                    "element" => {
                        if parts.len() < 4 {
                            return Err(anyhow!(
                                "Invalid transform config file, cause: ElementID is required"
                            ));
                        }
                        element_id_list.push(parts[3].to_string());
                    }
                    "point" => {
                        point_list.push(parts[0].to_string());
                    }
                    _ => {}
                }
            }
        }
        let template_list = template_list.into_iter().collect();
        Ok((element_id_list, point_list, template_list))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_server_name() {
        let dsn = Dsn::from_str("pi://WIN-2OA23UM12TN").unwrap();
        let config = PiConfig::parse_server_name(&dsn).unwrap();
        assert_eq!("WIN-2OA23UM12TN", config);

        let dsn = Dsn::from_str("pi://").unwrap();
        let config = PiConfig::parse_server_name(&dsn);
        assert!(config.is_err());
        assert_eq!("PIServerName is required", config.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_system_name() {
        let dsn = Dsn::from_str("pi://WIN-2OA23UM12TN").unwrap();
        let config = PiConfig::parse_system_name(&dsn);
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi://WIN-2OA23UM12TN?PISystemName=other").unwrap();
        let config = PiConfig::parse_system_name(&dsn);
        assert_eq!("other", config.unwrap());
    }

    #[test]
    fn test_parse_database() {
        let dsn = Dsn::from_str("pi:///Met1").unwrap();
        let config = PiConfig::parse_database(&dsn).unwrap();
        assert_eq!("Met1", config);

        let dsn = Dsn::from_str("pi://").unwrap();
        let config = PiConfig::parse_database(&dsn);
        assert_eq!(None, config);
    }

    #[test]
    fn test_parse_pi_data_pipes_instances() {
        let dsn = Dsn::from_str("pi:///Met1").unwrap();
        let config = PiConfig::parse_pi_data_pipes_instances(&dsn).unwrap();
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi:///Met1?PIDataPipesInstances=1").unwrap();
        let config = PiConfig::parse_pi_data_pipes_instances(&dsn).unwrap();
        assert_eq!(Some(1), config);

        let dsn = Dsn::from_str("pi:///Met1?PIDataPipesInstances=abc").unwrap();
        let config = PiConfig::parse_pi_data_pipes_instances(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid PIDataPipesInstances, cause: invalid digit found in string",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_af_data_pipes_instances() {
        let dsn = Dsn::from_str("pi:///Met1").unwrap();
        let config = PiConfig::parse_af_data_pipes_instances(&dsn).unwrap();
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi:///Met1?AFDataPipesInstances=1").unwrap();
        let config = PiConfig::parse_af_data_pipes_instances(&dsn).unwrap();
        assert_eq!(Some(1), config);

        let dsn = Dsn::from_str("pi:///Met1?AFDataPipesInstances=abc").unwrap();
        let config = PiConfig::parse_af_data_pipes_instances(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid AFDataPipesInstances, cause: invalid digit found in string",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_max_wait_len() {
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_max_wait_len(&dsn).unwrap();
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi:///?MaxWaitLen=1").unwrap();
        let config = PiConfig::parse_max_wait_len(&dsn).unwrap();
        assert_eq!(Some(1), config);

        let dsn = Dsn::from_str("pi:///?MaxWaitLen=abc").unwrap();
        let config = PiConfig::parse_max_wait_len(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid MaxWaitLen, cause: invalid digit found in string",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("pi:///?MaxWaitLen=0").unwrap();
        let config = PiConfig::parse_max_wait_len(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "MaxWaitLen should be in range 1..10000",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("pi:///?MaxWaitLen=10001").unwrap();
        let config = PiConfig::parse_max_wait_len(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "MaxWaitLen should be in range 1..10000",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    #[ignore]
    fn test_parse_update_interval() {
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_update_interval(&dsn).unwrap();
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi:///?UpdateInterval=100").unwrap();
        let config = PiConfig::parse_update_interval(&dsn).unwrap();
        assert_eq!(Some(100), config);

        let dsn = Dsn::from_str("pi:///?UpdateInterval=abc").unwrap();
        let config = PiConfig::parse_update_interval(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid UpdateInterval, cause: invalid digit found in string",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("pi:///?UpdateInterval=99").unwrap();
        let config = PiConfig::parse_update_interval(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "UpdateInterval should be in range 100..60000",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("pi:///?UpdateInterval=60001").unwrap();
        let config = PiConfig::parse_update_interval(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "UpdateInterval should be in range 100..60000",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_max_backfill_range_days() {
        // not set
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn).unwrap();
        assert_eq!(None, config);
        // error value
        let dsn = Dsn::from_str("pi:///?MaxBackfillRangeDays=abc").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid max_backfill_range: abc, cause: NoValueFoundError: no value found in the string \"abc\"",
            config.unwrap_err().to_string()
        );
        // use unit d
        let dsn = Dsn::from_str("pi:///?MaxBackfillRangeDays=2d").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn).unwrap();
        assert_eq!(Some(2880), config);
        // use unit d and h
        let dsn = Dsn::from_str("pi:///?MaxBackfillRangeDays=2d3h").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn).unwrap();
        assert_eq!(Some(3060), config);
        // use unit d and h and m
        let dsn = Dsn::from_str("pi:///?MaxBackfillRangeDays=2d3h4m").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn).unwrap();
        assert_eq!(Some(3064), config);
        // use unit d and h and m and s
        let dsn = Dsn::from_str("pi:///?MaxBackfillRangeDays=2d3h4m60s").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn).unwrap();
        assert_eq!(Some(3065), config);
        // use unit d and h and m and s
        let dsn = Dsn::from_str("pi:///?MaxBackfillRangeDays=2d3h4m65s").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn).unwrap();
        assert_eq!(Some(3066), config);
    }

    #[test]
    fn test_parse_from_tdengine_last_time() {
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_from_tdengine_last_time(&dsn).unwrap();
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi:///?FromTDengineLastTime=true").unwrap();
        let config = PiConfig::parse_from_tdengine_last_time(&dsn).unwrap();
        assert_eq!(Some(true), config);

        let dsn = Dsn::from_str("pi:///?FromTDengineLastTime=abc").unwrap();
        let config = PiConfig::parse_from_tdengine_last_time(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid FromTDengineLastTime, cause: provided string was not `true` or `false`",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_to_tdengine_first_time() {
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_to_tdengine_first_time(&dsn).unwrap();
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi:///?ToTDengineFirstTime=true").unwrap();
        let config = PiConfig::parse_to_tdengine_first_time(&dsn).unwrap();
        assert_eq!(Some(true), config);

        let dsn = Dsn::from_str("pi:///?ToTDengineFirstTime=abc").unwrap();
        let config = PiConfig::parse_to_tdengine_first_time(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid ToTDengineFirstTime, cause: provided string was not `true` or `false`",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_backfill_start_time() {
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_backfill_start_time(&dsn).unwrap();
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi:///?BackfillStartTime=2021-01-01 00:00:00").unwrap();
        let config = PiConfig::parse_backfill_start_time(&dsn).unwrap();
        assert_eq!("2021-01-01T00:00:00+08:00", config.unwrap().to_string());

        let dsn = Dsn::from_str("pi:///?BackfillStartTime=2021-01-01 00:00:00.000").unwrap();
        let config = PiConfig::parse_backfill_start_time(&dsn);
        assert!(config.is_err());
        assert_eq!("invalid BackfillStartTime, cause: failed to parse date time: 2021-01-01 00:00:00.000, cause: trailing input", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("pi:///?BackfillStartTime=auto").unwrap();
        let config = PiConfig::parse_backfill_start_time(&dsn).unwrap();
        assert_eq!(None, config);
        let from_tdengine_last_time = PiConfig::parse_from_tdengine_last_time(&dsn).unwrap();
        assert_eq!(Some(true), from_tdengine_last_time);
    }

    #[test]
    fn test_parse_backfill_end_time() {
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_backfill_end_time(&dsn).unwrap();
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi:///?BackfillEndTime=2021-01-01 00:00:00").unwrap();
        let config = PiConfig::parse_backfill_end_time(&dsn).unwrap();
        assert_eq!("2021-01-01T00:00:00+08:00", config.unwrap().to_string());

        let dsn = Dsn::from_str("pi:///?BackfillEndTime=2021-01-01 00:00:00.000").unwrap();
        let config = PiConfig::parse_backfill_end_time(&dsn);
        assert!(config.is_err());
        assert_eq!("invalid BackfillEndTime, cause: failed to parse date time: 2021-01-01 00:00:00.000, cause: trailing input", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("pi:///?BackfillEndTime=auto").unwrap();
        let config = PiConfig::parse_backfill_end_time(&dsn).unwrap();
        assert_eq!(None, config);
        let bool = PiConfig::parse_to_tdengine_first_time(&dsn).unwrap();
        assert_eq!(Some(true), bool);
    }

    #[test]
    fn test_parse_date_time() {
        let config = PiConfig::parse_date_time("2024-05-01T00:00:00+08:00").unwrap();
        println!("{:?}", config);
        assert_eq!("2024-05-01T00:00:00+08:00", config.to_string());
    }

    #[tokio::test]
    async fn test_config() {
        dbg!(std::env::current_dir().unwrap());
        let dsn = Dsn::from_str("pi://WIN-2OA23UM12TN/Met1?PISystemName=other&point_file=@../tests/pi/Points.csv&template_for_af_element_file=@../tests/pi/ElementTemplates2.csv").unwrap();
        let config = PiConfig::new(dsn, "taos".to_string(), 0, 0, None)
            .await
            .unwrap();
        dbg!(&config);

        let dsn: Dsn =
            Dsn::from_str("pi://WIN-2OA23UM12TN/Met1?PISystemName=other&point_file=app\napp\napp")
                .unwrap();
        let config2 = PiConfig::new(dsn, "taos".to_string(), 0, 0, None)
            .await
            .unwrap();
        dbg!(&config2);
    }
}
