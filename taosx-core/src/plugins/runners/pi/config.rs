use std::str::FromStr;

use chrono::{Local, NaiveDateTime};
use itertools::Itertools;
use taos::Dsn;
use toml::value::Datetime;

use taosx_ipc::types::DataSetsReq;

use crate::runners::get_string_from_param_or_file;
use crate::runners::pi::pi_datasets;

#[derive(Debug, serde::Serialize)]
pub struct PiConfig {
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
    #[serde(rename = "PointList")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    point_list: Vec<String>,
    // backfill param
    #[serde(rename = "FromTDengineLastTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    from_tdengine_last_time: Option<bool>,
    #[serde(rename = "ToTDengineFirstTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    to_tdengine_first_time: Option<bool>,
    #[serde(rename = "BackfillStartTime", skip_serializing_if = "Option::is_none")]
    backfill_start_time: Option<Datetime>,
    #[serde(rename = "BackfillEndTime", skip_serializing_if = "Option::is_none")]
    backfill_end_time: Option<Datetime>,
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
            system_name: Self::parse_system_name(dsn, server_name.clone()),
            database: Self::parse_database(dsn)?,
            pi_data_pipes_instances: Self::parse_pi_data_pipes_instances(dsn)?,
            af_data_pipes_instances: Self::parse_af_data_pipes_instances(dsn)?,
            max_wait_len: Self::parse_max_wait_len(dsn)?,
            update_interval: Self::parse_update_interval(dsn)?,
            max_backfill_range_days: Self::parse_max_backfill_range_days(dsn)?,
            ipc_stream: format!("127.0.0.1:{ipc}"),
            sql_api: format!("http://127.0.0.1:{sql}"),
            td_database,
            template_for_pi_point: Self::parse_template_for_pi_point(dsn),
            template_for_af_element: Self::parse_template_for_af_element(dsn),
            point_list: Self::parse_point_list(dsn)?,
            from_tdengine_last_time: Self::parse_from_tdengine_last_time(dsn)?,
            to_tdengine_first_time: Self::parse_to_tdengine_first_time(dsn)?,
            backfill_start_time: Self::parse_backfill_start_time(dsn)?,
            backfill_end_time: Self::parse_backfill_end_time(dsn)?,
        };

        Ok(pi_config)
    }
    pub async fn new(
        mut dsn: Dsn,
        td_database: String,
        ipc: u16,
        sql: u16,
        is_real_run: bool,
    ) -> anyhow::Result<PiConfig> {
        let server_name = Self::parse_server_name(&dsn)?;
        let system_name = Self::parse_system_name(&dsn, server_name.clone());
        let database = Self::parse_database(&dsn)?;
        let pi_data_pipes_instances = Self::parse_pi_data_pipes_instances(&dsn)?;
        let af_data_pipes_instances = Self::parse_af_data_pipes_instances(&dsn)?;
        let max_wait_len = Self::parse_max_wait_len(&dsn)?;
        let update_interval = Self::parse_update_interval(&dsn)?;
        let max_backfill_range_days = Self::parse_max_backfill_range_days(&dsn)?;

        let mut template_for_pi_point = Self::parse_template_for_pi_point(&dsn);
        let config_key = "template_for_pi_point_file";
        let config_category = "TemplateForPIPoint";
        if let Some(value) = dsn.get(config_key) {
            if value == "*" {
                let datasets = pi_datasets(&DataSetsReq {
                    from: dsn.to_string(),
                    categories: vec![config_category.to_string()],
                    pattern: None,
                    offset: 0,
                    limit: usize::MAX / 2 - 1,
                    via: None,
                    lang: None,
                })
                .await?;

                template_for_pi_point.extend(
                    datasets
                        .into_iter()
                        .map(|ds| ds.id)
                        .filter(|id| !id.is_empty()),
                );
            } else {
                template_for_pi_point.extend(
                    get_string_from_param_or_file(&mut dsn, config_key, false, Some(","))
                        .map_err(|err| {
                            anyhow::anyhow!("invalid {}, cause: {}", config_key, err.to_string())
                        })?
                        .unwrap_or_default()
                        .split([',', '\n'])
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string()),
                );
            }
        }

        let mut template_for_af_element = Self::parse_template_for_af_element(&dsn);
        let config_key = "template_for_af_element_file";
        let config_category = "TemplateForAFElement";
        if let Some(value) = dsn.get(config_key) {
            if value == "*" {
                let datasets = pi_datasets(&DataSetsReq {
                    from: dsn.to_string(),
                    categories: vec![config_category.to_string()],
                    pattern: None,
                    offset: 0,
                    limit: usize::MAX / 2 - 1,
                    via: None,
                    lang: None,
                })
                .await?;
                template_for_af_element.extend(
                    datasets
                        .into_iter()
                        .map(|ds| ds.id)
                        .filter(|id| !id.is_empty()),
                );
            } else {
                template_for_af_element.extend(
                    get_string_from_param_or_file(&mut dsn, config_key, false, Some(","))
                        .map_err(|err| {
                            anyhow::anyhow!("invalid {}, cause: {}", config_key, err.to_string())
                        })?
                        .unwrap_or_default()
                        .split([',', '\n'])
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string()),
                );
            }
        }

        let mut point_list = Self::parse_point_list(&dsn)?;
        let config_key = "point_file";
        let config_category = "PointList";
        if let Some(value) = dsn.get(config_key) {
            if value == "*" {
                let datasets = pi_datasets(&DataSetsReq {
                    from: dsn.to_string(),
                    categories: vec![config_category.to_string()],
                    pattern: None,
                    offset: 0,
                    limit: usize::MAX / 2 - 1,
                    via: None,
                    lang: None,
                })
                .await?;
                point_list.extend(
                    datasets
                        .into_iter()
                        .map(|ds| ds.id)
                        .filter(|id| !id.is_empty()),
                );
            } else {
                point_list.extend(
                    get_string_from_param_or_file(&mut dsn, "point_file", false, Some(","))
                        .map_err(|err| {
                            anyhow::anyhow!("invalid point_file, cause: {}", err.to_string())
                        })?
                        .unwrap_or_default()
                        .split([',', '\n'])
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string()),
                );
            }
        }

        if is_real_run
            && point_list.is_empty()
            && template_for_af_element.is_empty()
            && template_for_pi_point.is_empty()
        {
            return Err(anyhow::anyhow!("TemplateForPIPoint, TemplateForAFElement and PointList should config at least one of them"));
        }

        let mut from_tdengine_last_time = Self::parse_from_tdengine_last_time(&dsn)?;
        let mut to_tdengine_first_time = Self::parse_to_tdengine_first_time(&dsn)?;
        let backfill_start_time = if let Some(backfill_start) =
            dsn.params.get("BackfillStartTime").map(|v| v.to_string())
        {
            if backfill_start == "auto" {
                from_tdengine_last_time.replace(true);
                None
            } else {
                let parsed_time =
                    NaiveDateTime::parse_from_str(backfill_start.as_str(), "%Y-%m-%d %H:%M:%S")
                        .map_err(|err| {
                            anyhow::anyhow!(
                                "invalid BackfillStartTime: {}, cause: {}",
                                backfill_start.clone(),
                                err.to_string()
                            )
                        })?
                        .and_local_timezone(Local)
                        .unwrap();
                let parsed_time =
                    Datetime::from_str(parsed_time.to_rfc3339().as_str()).map_err(|err| {
                        anyhow::anyhow!(
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
            if let Some(backfill_end) = dsn.params.get("BackfillEndTime").map(|v| v.to_string()) {
                if backfill_end == "auto" {
                    to_tdengine_first_time.replace(true);
                    None
                } else {
                    let parsed_time =
                        NaiveDateTime::parse_from_str(backfill_end.as_str(), "%Y-%m-%d %H:%M:%S")
                            .map_err(|err| {
                                anyhow::anyhow!(
                                    "invalid BackfillEndTime: {}, cause: {}",
                                    backfill_end.clone(),
                                    err.to_string()
                                )
                            })?
                            .and_local_timezone(Local)
                            .unwrap();
                    let parsed_time = Datetime::from_str(parsed_time.to_rfc3339().as_str())
                        .map_err(|err| {
                            anyhow::anyhow!(
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
            return Err(anyhow::anyhow!(
                "Only one of the BackfillStartTime and BackfillEndTime can be automatically set"
            ));
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
            ipc_stream: format!("127.0.0.1:{}", ipc),
            sql_api: format!("http://127.0.0.1:{}", sql),
            td_database,
            template_for_pi_point,
            template_for_af_element,
            point_list,
            from_tdengine_last_time,
            to_tdengine_first_time,
            backfill_start_time,
            backfill_end_time,
        })
    }

    fn parse_server_name(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or(anyhow::anyhow!("PIServerName is required"))
    }

    fn parse_system_name(dsn: &Dsn, default_name: String) -> String {
        dsn.params
            .get("PISystemName")
            .map(|v| v.to_string())
            .unwrap_or(default_name)
    }

    fn parse_database(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.subject
            .clone()
            .ok_or(anyhow::anyhow!("Database name is required"))
    }

    fn parse_pi_data_pipes_instances(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        dsn.params
            .get("PIDataPipesInstances")
            .map(|v| {
                v.parse::<u32>().map_err(|err| {
                    anyhow::anyhow!("invalid PIDataPipesInstances, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_af_data_pipes_instances(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        dsn.params
            .get("AFDataPipesInstances")
            .map(|v| {
                v.parse::<u32>().map_err(|err| {
                    anyhow::anyhow!("invalid AFDataPipesInstances, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_max_wait_len(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        let max_wait_len = dsn
            .params
            .get("MaxWaitLen")
            .map(|v| {
                v.parse::<u32>().map_err(|err| {
                    anyhow::anyhow!("invalid MaxWaitLen, cause: {}", err.to_string())
                })
            })
            .transpose()?;
        if let Some(mwl) = max_wait_len {
            if mwl < 1 || mwl > 10000 {
                return Err(anyhow::anyhow!("MaxWaitLen should be in range 1..10000"));
            }
        }
        Ok(max_wait_len)
    }

    fn parse_update_interval(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        let update_interval = dsn
            .params
            .get("UpdateInterval")
            .map(|v| {
                v.parse::<u32>().map_err(|err| {
                    anyhow::anyhow!("invalid UpdateInterval, cause: {}", err.to_string())
                })
            })
            .transpose()?;
        if let Some(ui) = update_interval {
            if ui < 100 || ui > 60000 {
                return Err(anyhow::anyhow!(
                    "UpdateInterval should be in range 100..60000"
                ));
            }
        }
        Ok(update_interval)
    }

    fn parse_max_backfill_range_days(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        dsn.params
            .get("MaxBackfillRangeDays")
            .map(|v| {
                v.parse::<u32>().map_err(|err| {
                    anyhow::anyhow!("invalid MaxBackfillRangeDays, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_template_for_pi_point(dsn: &Dsn) -> Vec<String> {
        dsn.params
            .get("TemplateForPIPoint")
            .unwrap_or(&String::new())
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec()
    }

    fn parse_template_for_af_element(dsn: &Dsn) -> Vec<String> {
        dsn.params
            .get("TemplateForAFElement")
            .unwrap_or(&String::new())
            .split([',', '\n'])
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec()
    }

    fn parse_point_list(dsn: &Dsn) -> anyhow::Result<Vec<String>> {
        Ok(
            get_string_from_param_or_file(&mut dsn.clone(), "PointList", false, Some(","))
                .map_err(|err| anyhow::anyhow!("invalid PointList, cause: {}", err))?
                .unwrap_or_default()
                .split([',', '\n'])
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect_vec(),
        )
    }

    fn parse_from_tdengine_last_time(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        dsn.params
            .get("FromTDengineLastTime")
            .map(|v| {
                v.parse::<bool>().map_err(|err| {
                    anyhow::anyhow!("invalid FromTDengineLastTime, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_to_tdengine_first_time(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        dsn.params
            .get("ToTDengineFirstTime")
            .map(|v| {
                v.parse::<bool>().map_err(|err| {
                    anyhow::anyhow!("invalid ToTDengineFirstTime, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_backfill_start_time(dsn: &Dsn) -> anyhow::Result<Option<Datetime>> {
        dsn.params
            .get("BackfillStartTime")
            .map(|v| {
                Self::parse_date_time(v.as_str()).map_err(|err| {
                    anyhow::anyhow!("invalid BackfillStartTime, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_backfill_end_time(dsn: &Dsn) -> anyhow::Result<Option<Datetime>> {
        dsn.params
            .get("BackfillEndTime")
            .map(|v| {
                Self::parse_date_time(v.as_str()).map_err(|err| {
                    anyhow::anyhow!("invalid BackfillEndTime, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_date_time(date_time: &str) -> anyhow::Result<Datetime> {
        let parsed_time = NaiveDateTime::parse_from_str(date_time, "%Y-%m-%d %H:%M:%S")
            .map_err(|err| {
                anyhow::anyhow!(
                    "failed to parse date time: {}, cause: {}",
                    date_time,
                    err.to_string()
                )
            })?
            .and_local_timezone(Local)
            .unwrap();
        let parsed_time = Datetime::from_str(parsed_time.to_rfc3339().as_str()).map_err(|err| {
            anyhow::anyhow!(
                "failed to parse date time: {}, cause: {}",
                date_time,
                err.to_string()
            )
        })?;

        Ok(parsed_time)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use taos::Dsn;

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
        let config = PiConfig::parse_system_name(&dsn, "default".to_string());
        assert_eq!("default", config);

        let dsn = Dsn::from_str("pi://WIN-2OA23UM12TN?PISystemName=other").unwrap();
        let config = PiConfig::parse_system_name(&dsn, "default".to_string());
        assert_eq!("other", config);
    }

    #[test]
    fn test_parse_database() {
        let dsn = Dsn::from_str("pi:///Met1").unwrap();
        let config = PiConfig::parse_database(&dsn).unwrap();
        assert_eq!("Met1", config);

        let dsn = Dsn::from_str("pi://").unwrap();
        let config = PiConfig::parse_database(&dsn);
        assert!(config.is_err());
        assert_eq!("Database name is required", config.unwrap_err().to_string());
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
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn).unwrap();
        assert_eq!(None, config);

        let dsn = Dsn::from_str("pi:///?MaxBackfillRangeDays=100").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn).unwrap();
        assert_eq!(Some(100), config);

        let dsn = Dsn::from_str("pi:///?MaxBackfillRangeDays=abc").unwrap();
        let config = PiConfig::parse_max_backfill_range_days(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid MaxBackfillRangeDays, cause: invalid digit found in string",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_template_for_pi_point() {
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_template_for_pi_point(&dsn);
        assert!(config.is_empty());

        let dsn = Dsn::from_str("pi:///?TemplateForPIPoint=abc").unwrap();
        let config = PiConfig::parse_template_for_pi_point(&dsn);
        assert_eq!(vec!["abc"], config);

        let dsn = Dsn::from_str("pi:///?TemplateForPIPoint=abc,def").unwrap();
        let config = PiConfig::parse_template_for_pi_point(&dsn);
        assert_eq!(vec!["abc", "def"], config);
    }

    #[test]
    fn test_parse_template_for_af_element() {
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_template_for_af_element(&dsn);
        assert!(config.is_empty());

        let dsn = Dsn::from_str("pi:///?TemplateForAFElement=abc").unwrap();
        let config = PiConfig::parse_template_for_af_element(&dsn);
        assert_eq!(vec!["abc"], config);

        let dsn = Dsn::from_str("pi:///?TemplateForAFElement=abc,def").unwrap();
        let config = PiConfig::parse_template_for_af_element(&dsn);
        assert_eq!(vec!["abc", "def"], config);

        let dsn = Dsn::from_str("pi:///?TemplateForAFElement=abc,def\nghi").unwrap();
        let config = PiConfig::parse_template_for_af_element(&dsn);
        assert_eq!(vec!["abc", "def", "ghi"], config);
    }

    #[test]
    fn test_parse_point_list() {
        let dsn = Dsn::from_str("pi:///").unwrap();
        let config = PiConfig::parse_point_list(&dsn).unwrap();
        assert!(config.is_empty());

        let dsn = Dsn::from_str("pi:///?PointList=abc").unwrap();
        let config = PiConfig::parse_point_list(&dsn).unwrap();
        assert_eq!(vec!["abc"], config);

        let dsn = Dsn::from_str("pi:///?PointList=abc,def").unwrap();
        let config = PiConfig::parse_point_list(&dsn).unwrap();
        assert_eq!(vec!["abc", "def"], config);

        let dsn = Dsn::from_str("pi:///?PointList=abc,def\nghi").unwrap();
        let config = PiConfig::parse_point_list(&dsn).unwrap();
        assert_eq!(vec!["abc", "def", "ghi"], config);
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
    }

    #[test]
    fn test_parse_date_time() {
        let config = PiConfig::parse_date_time("2021-01-01 00:00:00").unwrap();
        assert_eq!("2021-01-01T00:00:00+08:00", config.to_string());

        let config = PiConfig::parse_date_time("2021-01-01 00:00:00.000");
        assert!(config.is_err());
        assert_eq!(
            "failed to parse date time: 2021-01-01 00:00:00.000, cause: trailing input",
            config.unwrap_err().to_string()
        );
    }

    #[tokio::test]
    async fn test_config() {
        dbg!(std::env::current_dir().unwrap());
        let dsn = Dsn::from_str("pi://WIN-2OA23UM12TN/Met1?PISystemName=other&point_file=@../tests/pi/Points.csv&template_for_af_element_file=@../tests/pi/ElementTemplates2.csv").unwrap();
        let config = PiConfig::new(dsn, "taos".to_string(), 0, 0, false)
            .await
            .unwrap();
        dbg!(&config);

        let dsn: Dsn =
            Dsn::from_str("pi://WIN-2OA23UM12TN/Met1?PISystemName=other&point_file=app\napp\napp")
                .unwrap();
        let config2 = PiConfig::new(dsn, "taos".to_string(), 0, 0, false)
            .await
            .unwrap();
        dbg!(&config2);
    }
}
