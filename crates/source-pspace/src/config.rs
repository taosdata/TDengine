use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use taos::Dsn;
use taosx_core::{
    config::AdvancedOptions,
    utils::{parse_datetime_in_dsn, parse_duration_in_dsn, parse_key_in_dsn},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PspaceConfig {
    pub connection: PspaceConnection,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<PspaceNodesConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub points: Option<PspacePointsConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub run: Option<PspaceTaskConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<PspaceReportConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub advanced_options: Option<AdvancedOptions>,
}

impl PspaceConfig {
    /// Create a builder that only parses the connection from DSN.
    /// Use `with_*` methods to selectively parse additional sections.
    pub fn builder(dsn: &Dsn) -> anyhow::Result<PspaceConfigBuilder> {
        let connection = PspaceConnection::try_from(dsn)?;
        Ok(PspaceConfigBuilder {
            connection,
            dsn: dsn.clone(),
            nodes: None,
            points: None,
            run: None,
            report: None,
            advanced_options: None,
        })
    }
}

pub struct PspaceConfigBuilder {
    connection: PspaceConnection,
    dsn: Dsn,
    nodes: Option<PspaceNodesConfig>,
    points: Option<PspacePointsConfig>,
    run: Option<PspaceTaskConfig>,
    report: Option<PspaceReportConfig>,
    advanced_options: Option<AdvancedOptions>,
}

impl PspaceConfigBuilder {
    /// Parse `nodes` section (root param) from DSN.
    pub fn with_nodes(mut self) -> anyhow::Result<Self> {
        let root =
            parse_key_in_dsn::<u64>(&self.dsn, "root").context("failed to parse `root` param")?;
        self.nodes = root.map(|r| PspaceNodesConfig { root: r });
        Ok(self)
    }

    /// Parse `points` section (point_name_pattern, include_data_type params) from DSN.
    pub fn with_points(mut self) -> anyhow::Result<Self> {
        let filter = parse_key_in_dsn::<String>(&self.dsn, "point_name_pattern")
            .context("failed to parse point_name_pattern")?;
        let include_data_type = parse_key_in_dsn::<bool>(&self.dsn, "include_data_type")
            .context("failed to parse include_data_type")?;

        if filter.is_some() || include_data_type.is_some() {
            self.points = Some(PspacePointsConfig {
                name_filter: filter,
                include_data_type,
                point_ids: None,
            });
        }
        Ok(self)
    }

    /// Parse `run` section (task config) from DSN.
    pub fn with_run(mut self) -> anyhow::Result<Self> {
        self.run = PspaceTaskConfig::try_from_dsn(&self.dsn)?;
        Ok(self)
    }

    /// Parse `report` section from DSN. Only effective when `run` is present.
    pub fn with_report(mut self) -> anyhow::Result<Self> {
        if self.run.is_some() {
            self.report = Some(PspaceReportConfig::try_from_dsn(&self.dsn)?);
        }
        Ok(self)
    }

    /// Parse `advanced_options` section from DSN.
    pub fn with_advanced_options(mut self) -> anyhow::Result<Self> {
        let opts = AdvancedOptions::from_dsn(&self.dsn)?;
        // Only set if at least one option is present
        if opts.log_level.is_some()
            || opts.read_concurrency.is_some()
            || opts.write_concurrency.is_some()
            || opts.batch_size.is_some()
            || opts.batch_timeout.is_some()
            || opts.keep_raw_data.is_some()
            || opts.keep_raw_data_days.is_some()
            || opts.keep_raw_data_dir.is_some()
        {
            self.advanced_options = Some(opts);
        }
        Ok(self)
    }

    /// Consume the builder and produce a `PspaceConfig`.
    pub fn build(self) -> anyhow::Result<PspaceConfig> {
        Ok(PspaceConfig {
            connection: self.connection,
            nodes: self.nodes,
            points: self.points,
            run: self.run,
            report: self.report,
            advanced_options: self.advanced_options,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PspaceConnection {
    pub server: String,   // pSpace server address
    pub port: u16,        // pSpace server port
    pub username: String, // pSpace username
    pub password: String, // pSpace password
    pub timeout_sec: u64, // Connection timeout in seconds
}

impl TryFrom<&Dsn> for PspaceConnection {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        let addr = dsn
            .addresses
            .first()
            .ok_or_else(|| anyhow::anyhow!("invalid dsn (missing host/port): {}", dsn))?;
        let server = addr
            .host
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("invalid dsn (missing host): {}", dsn))?
            .to_string();
        let port = addr.port.unwrap_or(5678);
        let username = dsn
            .username
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("invalid dsn (missing username): {}", dsn))?
            .to_string();
        let password = dsn
            .password
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("invalid dsn (missing password): {}", dsn))?
            .to_string();
        let timeout_sec = parse_duration_in_dsn(dsn, "connect_timeout")
            .context("invalid connect_timeout")?
            .map(|t| t.as_secs())
            .unwrap_or(30);

        Ok(Self {
            server,
            port,
            username,
            password,
            timeout_sec,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PspaceTaskMode {
    Query,
    Subscribe,
    QuerySync,
}

impl TryFrom<&str> for PspaceTaskMode {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "query" => Ok(PspaceTaskMode::Query),
            "subscribe" => Ok(PspaceTaskMode::Subscribe),
            "querysync" | "query_sync" => Ok(PspaceTaskMode::QuerySync),
            other => anyhow::bail!("unknown pspace task mode: {}", other),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PspaceNodesConfig {
    pub root: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PspacePointsConfig {
    pub name_filter: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_data_type: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub point_ids: Option<Vec<u64>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PspaceTaskConfig {
    pub mode: PspaceTaskMode,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub time_window: Option<i64>,
    pub time_excursion: Option<i64>,
    pub query_interval: Option<i64>,
}

impl PspaceTaskConfig {
    pub fn try_from_dsn(dsn: &Dsn) -> anyhow::Result<Option<Self>> {
        let mode = parse_key_in_dsn::<String>(dsn, "pspace_task_mode")
            .context("failed to parse pspace_task_mode")?
            .map(|s| PspaceTaskMode::try_from(s.as_str()))
            .transpose()
            .context("invalid pspace_task_mode")?;

        match mode {
            None => Ok(None),
            Some(PspaceTaskMode::Query) => {
                let start_time = parse_datetime_in_dsn(dsn, "start_time")?
                    .ok_or(anyhow::anyhow!("start_time is required on query task mode"))?;
                let end_time = parse_datetime_in_dsn(dsn, "end_time")?;
                let time_window_sec =
                    parse_duration_in_dsn(dsn, "time_window")?.map(|d| d.as_secs() as i64);
                Ok(Some(Self {
                    mode: PspaceTaskMode::Query,
                    start_time: Some(start_time),
                    end_time,
                    time_window: time_window_sec,
                    time_excursion: None,
                    query_interval: None,
                }))
            }
            Some(PspaceTaskMode::Subscribe) => Ok(Some(Self {
                mode: PspaceTaskMode::Subscribe,
                start_time: None,
                end_time: None,
                time_window: None,
                time_excursion: None,
                query_interval: None,
            })),
            Some(PspaceTaskMode::QuerySync) => {
                let start_time = parse_datetime_in_dsn(dsn, "start_time")?.ok_or(
                    anyhow::anyhow!("start_time is required on query_sync task mode"),
                )?;
                let end_time = parse_datetime_in_dsn(dsn, "end_time")?;
                let time_window_sec =
                    parse_duration_in_dsn(dsn, "time_window")?.map(|d| d.as_secs() as i64);
                let time_excursion_sec =
                    parse_duration_in_dsn(dsn, "time_excursion")?.map(|d| d.as_secs() as i64);
                let query_interval_sec =
                    parse_duration_in_dsn(dsn, "query_interval")?.map(|d| d.as_secs() as i64);
                Ok(Some(Self {
                    mode: PspaceTaskMode::QuerySync,
                    start_time: Some(start_time),
                    end_time,
                    time_window: time_window_sec,
                    time_excursion: time_excursion_sec,
                    query_interval: query_interval_sec,
                }))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PspaceReportConfig {
    pub remote: Option<String>,
}

impl PspaceReportConfig {
    pub fn try_from_dsn(_dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(Self { remote: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taos::IntoDsn;

    #[test]
    fn test_pspace_config() {
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?connect_timeout=10s"
            .into_dsn()
            .unwrap();
        let conn = PspaceConnection::try_from(&dsn).unwrap();

        assert_eq!(conn.server, "192.168.2.149");
        assert_eq!(conn.port, 8889);
        assert_eq!(conn.username, "admin");
        assert_eq!(conn.password, "admin888");
        assert_eq!(conn.timeout_sec, 10);

        let dsn = "pspace://admin:admin888@192.168.2.149:8889"
            .into_dsn()
            .unwrap();
        let conn = PspaceConnection::try_from(&dsn).unwrap();

        assert_eq!(conn.timeout_sec, 30);

        // parse nodes and points
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?root=100&point_name_pattern=.*temp.*"
            .into_dsn()
            .unwrap();
        let config = PspaceConfig::builder(&dsn)
            .unwrap()
            .with_nodes()
            .unwrap()
            .with_points()
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(config.nodes.as_ref().unwrap().root, 100);
        assert_eq!(
            config.points.as_ref().unwrap().name_filter.as_deref(),
            Some(".*temp.*")
        );
        assert_eq!(config.points.as_ref().unwrap().include_data_type, None);

        // parse points with include_data_type
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?root=100&point_name_pattern=.*temp.*&include_data_type=true"
            .into_dsn()
            .unwrap();
        let config = PspaceConfig::builder(&dsn)
            .unwrap()
            .with_nodes()
            .unwrap()
            .with_points()
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            config.points.as_ref().unwrap().include_data_type,
            Some(true)
        );

        // parse points with only include_data_type (no name_filter)
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?root=100&include_data_type=true"
            .into_dsn()
            .unwrap();
        let config = PspaceConfig::builder(&dsn)
            .unwrap()
            .with_nodes()
            .unwrap()
            .with_points()
            .unwrap()
            .build()
            .unwrap();
        assert!(config.points.is_some());
        assert_eq!(config.points.as_ref().unwrap().name_filter, None);
        assert_eq!(
            config.points.as_ref().unwrap().include_data_type,
            Some(true)
        );
    }

    #[test]
    fn test_pspace_task_config() {
        // Test Query mode with required start_time
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?\
            pspace_task_mode=query&\
            start_time=2024-01-01T00:00:00Z&\
            end_time=2024-01-02T00:00:00Z&\
            time_window=1h&\
            time_excursion=10m"
            .into_dsn()
            .unwrap();
        let config = PspaceTaskConfig::try_from_dsn(&dsn).unwrap().unwrap();
        assert!(matches!(config.mode, PspaceTaskMode::Query));
        assert_eq!(
            config.start_time.unwrap().to_rfc3339(),
            "2024-01-01T00:00:00+00:00"
        );
        assert_eq!(
            config.end_time.unwrap().to_rfc3339(),
            "2024-01-02T00:00:00+00:00"
        );
        assert_eq!(config.time_window, Some(3600));
        assert_eq!(config.time_excursion, None); // Query mode ignores time_excursion
        assert_eq!(config.query_interval, None);

        // Test Subscribe mode
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?pspace_task_mode=subscribe"
            .into_dsn()
            .unwrap();
        let config = PspaceTaskConfig::try_from_dsn(&dsn).unwrap().unwrap();
        assert!(matches!(config.mode, PspaceTaskMode::Subscribe));
        assert_eq!(config.start_time, None);
        assert_eq!(config.end_time, None);
        assert_eq!(config.time_window, None);
        assert_eq!(config.time_excursion, None);
        assert_eq!(config.query_interval, None);

        // Test QuerySync mode with all parameters
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?\
            pspace_task_mode=query_sync&\
            start_time=2024-01-01T00:00:00Z&\
            end_time=2024-01-02T00:00:00Z&\
            time_window=2h&\
            time_excursion=20m&\
            query_interval=30m"
            .into_dsn()
            .unwrap();
        let config = PspaceTaskConfig::try_from_dsn(&dsn).unwrap().unwrap();
        assert!(matches!(config.mode, PspaceTaskMode::QuerySync));
        assert_eq!(
            config.start_time.unwrap().to_rfc3339(),
            "2024-01-01T00:00:00+00:00"
        );
        assert_eq!(
            config.end_time.unwrap().to_rfc3339(),
            "2024-01-02T00:00:00+00:00"
        );
        assert_eq!(config.time_window, Some(7200));
        assert_eq!(config.time_excursion, Some(1200));
        assert_eq!(config.query_interval, Some(1800));

        // Test no mode specified (should return None)
        let dsn = "pspace://admin:admin888@192.168.2.149:8889"
            .into_dsn()
            .unwrap();
        let config = PspaceTaskConfig::try_from_dsn(&dsn).unwrap();
        assert_eq!(config, None);

        // Test Query mode without start_time should fail
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?pspace_task_mode=query"
            .into_dsn()
            .unwrap();
        let result = PspaceTaskConfig::try_from_dsn(&dsn);
        assert!(result.is_err());

        // Test QuerySync mode without start_time should fail
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?pspace_task_mode=query_sync"
            .into_dsn()
            .unwrap();
        let result = PspaceTaskConfig::try_from_dsn(&dsn);
        assert!(result.is_err());
    }

    #[test]
    fn test_pspace_config_serde() {
        // Test serialization of PspaceConfig to TOML
        let dsn = "pspace://admin:admin888@192.168.2.149:8889?batch_size=1000&batch_timeout=1&busy_threshold=100%25&child_table_expression=t_{tag_name}&concurrency=0&connect_timeout=30s&end_time=2026-02-27T00:00:00%2B08:00&health_check_window_in_second=0s&log_level=info&max_errors_in_window=10&max_queue_length=1000&point_config_mode=select_all_points&point_name_pattern=%5C北京%5C朝阳%5C*气温*&pspace_task_mode=query&query_interval=10s&root=150016&start_time=2026-02-01T00:00:00%2B08:00&super_table_expression=pspace_{type}&table_primary_key=original_ts&table_primary_key_alias=ts&time_excursion=0s&time_window=1d&value_col=val"
            .into_dsn()
            .unwrap();

        let config = PspaceConfig::builder(&dsn)
            .unwrap()
            .with_nodes()
            .unwrap()
            .with_points()
            .unwrap()
            .with_run()
            .unwrap()
            .build()
            .unwrap();

        // Verify connection config
        assert_eq!(config.connection.server, "192.168.2.149");
        assert_eq!(config.connection.port, 8889);
        assert_eq!(config.connection.username, "admin");
        assert_eq!(config.connection.password, "admin888");
        assert_eq!(config.connection.timeout_sec, 30);

        // Verify nodes config
        assert_eq!(config.nodes.as_ref().unwrap().root, 150016);

        // Verify points config
        assert_eq!(
            config.points.as_ref().unwrap().name_filter.as_deref(),
            Some("\\北京\\朝阳\\*气温*")
        );

        // Verify task config (mode is Query, not QuerySync)
        let run = config.run.as_ref().unwrap();
        assert!(matches!(run.mode, PspaceTaskMode::Query));
        assert_eq!(run.time_window, Some(86400)); // 1d
        assert_eq!(run.time_excursion, None); // Query mode ignores time_excursion
        assert_eq!(run.query_interval, None); // Query mode does not parse query_interval

        // Serialize to TOML and verify structure
        let toml_string = toml::to_string_pretty(&config).unwrap();
        // println!("Serialized TOML:\n{}", toml_string);

        // Verify connection section
        assert!(toml_string.contains("server = \"192.168.2.149\""));
        assert!(toml_string.contains("port = 8889"));
        assert!(toml_string.contains("username = \"admin\""));
        assert!(toml_string.contains("password = \"admin888\""));
        assert!(toml_string.contains("timeout_sec = 30"));

        // Verify nodes section
        assert!(toml_string.contains("root = 150016"));

        // Verify points section (name_filter uses single quotes for special characters)
        assert!(toml_string.contains("name_filter = '\\北京\\朝阳\\*气温*'"));

        // Verify run section (mode is serialized as "Query")
        assert!(toml_string.contains("mode = \"Query\""));
        assert!(toml_string.contains("time_window = 86400"));
        assert!(!toml_string.contains("time_excursion")); // Query mode omits time_excursion

        // Deserialize back from TOML and verify roundtrip
        let deserialized: PspaceConfig = toml::from_str(&toml_string).unwrap();
        assert_eq!(deserialized.connection.server, "192.168.2.149");
        assert_eq!(deserialized.connection.port, 8889);
        assert_eq!(deserialized.nodes.as_ref().unwrap().root, 150016);
        assert_eq!(
            deserialized.points.as_ref().unwrap().name_filter.as_deref(),
            Some("\\北京\\朝阳\\*气温*")
        );
        let run2 = deserialized.run.as_ref().unwrap();
        assert!(matches!(run2.mode, PspaceTaskMode::Query));
        assert_eq!(run2.time_window, Some(86400));
        assert_eq!(run2.time_excursion, None); // Query mode ignores time_excursion
        assert_eq!(run2.query_interval, None);
    }

    #[test]
    fn test_pspace_task_mode_try_from() {
        // All valid variants (case-insensitive)
        assert_eq!(
            PspaceTaskMode::try_from("query").unwrap(),
            PspaceTaskMode::Query
        );
        assert_eq!(
            PspaceTaskMode::try_from("Query").unwrap(),
            PspaceTaskMode::Query
        );
        assert_eq!(
            PspaceTaskMode::try_from("QUERY").unwrap(),
            PspaceTaskMode::Query
        );
        assert_eq!(
            PspaceTaskMode::try_from("subscribe").unwrap(),
            PspaceTaskMode::Subscribe
        );
        assert_eq!(
            PspaceTaskMode::try_from("Subscribe").unwrap(),
            PspaceTaskMode::Subscribe
        );
        assert_eq!(
            PspaceTaskMode::try_from("SUBSCRIBE").unwrap(),
            PspaceTaskMode::Subscribe
        );
        assert_eq!(
            PspaceTaskMode::try_from("querysync").unwrap(),
            PspaceTaskMode::QuerySync
        );
        assert_eq!(
            PspaceTaskMode::try_from("query_sync").unwrap(),
            PspaceTaskMode::QuerySync
        );
        assert_eq!(
            PspaceTaskMode::try_from("QuerySync").unwrap(),
            PspaceTaskMode::QuerySync
        );

        // Unknown value → error
        let err = PspaceTaskMode::try_from("invalid").unwrap_err();
        assert!(err.to_string().contains("unknown pspace task mode"));
    }

    #[test]
    fn test_pspace_connection_edge_cases() {
        let dsn = "pspace://admin:admin888@".into_dsn().unwrap();
        let err = PspaceConnection::try_from(&dsn).unwrap_err();
        assert!(err.to_string().contains("missing host"));

        // DSN with no user info at all → username is None → should fail
        let dsn = "pspace://127.0.0.1:5678".into_dsn().unwrap();
        let err = PspaceConnection::try_from(&dsn).unwrap_err();
        assert!(err.to_string().contains("missing username"));

        let dsn = "pspace://admin@127.0.0.1:5678".into_dsn().unwrap();
        let err = PspaceConnection::try_from(&dsn).unwrap_err();
        assert!(err.to_string().contains("missing password"));

        let dsn = "pspace://admin:admin888@127.0.0.1".into_dsn().unwrap();
        let conn = PspaceConnection::try_from(&dsn).unwrap();
        assert_eq!(conn.port, 5678);
    }

    #[test]
    fn test_pspace_config_builder_no_optional() {
        let dsn = "pspace://admin:admin888@192.168.2.149:8889"
            .into_dsn()
            .unwrap();
        let config = PspaceConfig::builder(&dsn).unwrap().build().unwrap();

        assert_eq!(config.connection.server, "192.168.2.149");
        assert!(config.nodes.is_none());
        assert!(config.points.is_none());
        assert!(config.run.is_none());
        assert!(config.report.is_none());
        assert!(config.advanced_options.is_none());

        // report without run → report should not be set
        let dsn = "pspace://admin:admin888@192.168.2.149:8889"
            .into_dsn()
            .unwrap();
        let config = PspaceConfig::builder(&dsn)
            .unwrap()
            .with_report()
            .unwrap()
            .build()
            .unwrap();
        assert!(config.report.is_none());

        // No filter params → points should be None
        let dsn = "pspace://admin:admin888@192.168.2.149:8889"
            .into_dsn()
            .unwrap();
        let config = PspaceConfig::builder(&dsn)
            .unwrap()
            .with_points()
            .unwrap()
            .build()
            .unwrap();
        assert!(config.points.is_none());
    }

    #[test]
    fn test_pspace_report_config() {
        let dsn = "pspace://admin:admin888@127.0.0.1:5678".into_dsn().unwrap();
        let report = PspaceReportConfig::try_from_dsn(&dsn).unwrap();
        assert!(report.remote.is_none());
    }
}
