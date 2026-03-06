use std::path::PathBuf;

use anyhow::Ok;
use anyhow::{Context, bail};
use taos::Dsn;
use taosx_core::sink::point::model::{
    ColumnConfig, CustomTag, GeneratePointMappingBy, PointConfig, PointConfigMap,
    PointMappingGenerator, PointMappingRule, PointModelConfig, SourceType, TableConfig,
    TableConfigMap,
};
use taosx_core::utils::parse_key_in_dsn;
use taosx_ipc::prelude::IpcDataType;

use crate::points::PspacePoint;

#[derive(Debug, Clone)]
pub enum PspaceModelConfig {
    Select(SelectArgs),
    Csv(CsvArgs),
}

impl TryFrom<&Dsn> for PspaceModelConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        let point_config_mode = parse_key_in_dsn::<String>(dsn, "point_config_mode")
            .context("failed to parse point_config_mode")?
            .unwrap_or_else(|| "select_all_points".to_string());

        match point_config_mode.as_str() {
            "select_all_points" => {
                let args = SelectArgs::try_from(dsn).context("failed to parse select mode args")?;
                Ok(PspaceModelConfig::Select(args))
            }
            "csv_config_file" => {
                let args = CsvArgs::try_from(dsn).context("failed to parse csv mode args")?;
                Ok(PspaceModelConfig::Csv(args))
            }
            other => bail!("unknown point_config_mode: {}", other),
        }
    }
}

impl PspaceModelConfig {
    // generate PointModelConfig
    pub async fn to_point_model_config(self, dsn: &Dsn) -> anyhow::Result<PointModelConfig> {
        match self {
            PspaceModelConfig::Select(select_args) => {
                let pspace_mapping_rule: PspacePointMappingRule = select_args.into();
                let point_mapping_rule = pspace_mapping_rule.inner.clone();

                let points = crate::list_points(dsn)
                    .await
                    .context("failed to list pSpace nodes")?;

                let (point_config_map, table_config_map) = pspace_mapping_rule.generate(points)?;

                let model_config = PointModelConfig {
                    source_type: SourceType::Pspace,
                    update_mode: None, // not support
                    generate_rule: Some(GeneratePointMappingBy::Rule(point_mapping_rule)),
                    point_config_map,
                    table_config_map,
                    node_config_map: None, // not support
                };

                Ok(model_config)
            }
            PspaceModelConfig::Csv(csv_args) => {
                let csv_config_file = csv_args.csv_config_file;
                let model_config =
                    crate::csv::parse_point_model_config_from_csv_file(&csv_config_file).await?;
                Ok(model_config)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct PspacePointMappingRule {
    inner: PointMappingRule,
}

impl PspacePointMappingRule {
    fn extra_custom_tags(&self, point_config: &mut PointConfig, pspace_point: &PspacePoint) {
        if let Some(tag_values) = point_config.tag_values.as_mut() {
            let name = pspace_point.name.as_str();
            let long_name = pspace_point.long_name.as_str();
            let description = pspace_point.desc.as_deref().unwrap_or("");

            for (_tag_name, tag_value) in tag_values.iter_mut() {
                *tag_value = tag_value.replace("{Name}", name);
                *tag_value = tag_value.replace("{LongName}", long_name);
                *tag_value = tag_value.replace("{Description}", description);
            }
        }
    }
}

impl PointMappingGenerator for PspacePointMappingRule {
    fn generate(
        &self,
        data: Vec<taosx_core::DataSet>,
    ) -> anyhow::Result<(PointConfigMap, TableConfigMap)> {
        let mut point_map = PointConfigMap::new();
        let mut table_map = TableConfigMap::new();

        for (index, dataset) in data.into_iter().enumerate() {
            let point_id = dataset.id.clone();

            let pspace_point = PspacePoint::try_from(dataset.clone())?;
            let point_type = pspace_point.data_type();

            let mut point_config =
                self.inner
                    .gen_point_config(index, point_id.clone(), point_type.clone())?;
            self.extra_custom_tags(&mut point_config, &pspace_point);

            let table_config = self.inner.gen_table_config(point_type)?;

            point_map.insert(point_id.clone(), point_config);
            table_map.insert(point_id, table_config);
        }

        Ok((point_map, table_map))
    }

    fn generate_single_point_mapping(
        &self,
        index: usize,
        point_id: String,
        point_type: Option<IpcDataType>,
    ) -> anyhow::Result<(PointConfig, TableConfig)> {
        self.inner
            .generate_single_point_mapping(index, point_id, point_type)
    }
}

#[derive(Debug, Clone)]
pub struct SelectArgs {
    super_table_expression: String,
    child_table_expression: String,
    table_primary_key: String,
    table_primary_key_alias: String,
    value_col: String,
    value_transform: Option<String>,
    quality_col: String,
    custom_tags: Vec<CustomTag>,
}

impl TryFrom<&Dsn> for SelectArgs {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        let super_table_expression = parse_key_in_dsn::<String>(dsn, "super_table_expression")
            .context("failed to parse super_table_expression")?
            .unwrap_or_else(|| "pspace_{type}".to_string());

        let child_table_expression = parse_key_in_dsn::<String>(dsn, "child_table_expression")
            .context("failed to parse child_table_expression")?
            .unwrap_or_else(|| "t_{point_id}".to_string());

        let table_primary_key = ColumnConfig::parse_primary_key(dsn)
            .context("failed to parse table_primary_key")?
            .unwrap_or(ColumnConfig::ORIGINAL_TS.to_string());

        let table_primary_key_alias = parse_key_in_dsn::<String>(dsn, "table_primary_key_alias")
            .context("failed to parse table_primary_key_alias")?
            .unwrap_or_else(|| default_primary_key_alias(&table_primary_key).to_string());

        let value_col = parse_key_in_dsn::<String>(dsn, "value_col")
            .context("failed to parse value_col")?
            .unwrap_or_else(|| "val".to_string());

        let value_transform = parse_key_in_dsn::<String>(dsn, "value_transform")
            .context("failed to parse value_transform")?;

        let quality_col = parse_key_in_dsn::<String>(dsn, "quality_col")
            .context("failed to parse quality_col")?
            .unwrap_or_else(|| "quality".to_string());

        let custom_tags = CustomTag::try_from_dsn(dsn)
            .context("failed to parse custom_tags")?
            .unwrap_or(default_pspace_custom_tags());

        Ok(Self {
            super_table_expression,
            child_table_expression,
            table_primary_key,
            table_primary_key_alias,
            value_col,
            value_transform,
            quality_col,
            custom_tags,
        })
    }
}

impl From<SelectArgs> for PspacePointMappingRule {
    fn from(value: SelectArgs) -> Self {
        Self {
            inner: PointMappingRule {
                source_type: SourceType::Pspace,
                stable_expression: value.super_table_expression,
                tbname_expression: value.child_table_expression,
                value_col: value.value_col,
                value_transform: value.value_transform,
                quality_col: value.quality_col,
                primary_key: value.table_primary_key,
                primary_key_alias: value.table_primary_key_alias,
                custom_tags: Some(value.custom_tags),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct CsvArgs {
    csv_config_file: PathBuf,
}

impl TryFrom<&Dsn> for CsvArgs {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        // csv_config_file in DSN may have a `@` prefix indicating a file path
        let raw = parse_key_in_dsn::<String>(dsn, "csv_config_file")
            .context("failed to parse csv_config_file")?
            .ok_or(anyhow::anyhow!("csv_config_file is required"))?;

        let path_str = raw.strip_prefix('@').unwrap_or(&raw);
        let csv_config_file = PathBuf::from(path_str);

        if !csv_config_file.exists() {
            bail!(
                "pSpace CSV config file not found: {}",
                csv_config_file.display()
            );
        }

        Ok(Self { csv_config_file })
    }
}

fn default_primary_key_alias(primary_key: &str) -> &'static str {
    match primary_key {
        ColumnConfig::ORIGINAL_TS => "ts",
        ColumnConfig::REQUEST_TS => "qts",
        ColumnConfig::RECEIVED_TS => "rts",
        _ => "ts",
    }
}

fn default_pspace_custom_tags() -> Vec<CustomTag> {
    vec![
        CustomTag {
            name: "Name".to_string(),
            data_type: IpcDataType::VarChar(1024),
            pattern: "{Name}".to_string(),
        },
        CustomTag {
            name: "LongName".to_string(),
            data_type: IpcDataType::VarChar(1024),
            pattern: "{LongName}".to_string(),
        },
        CustomTag {
            name: "Description".to_string(),
            data_type: IpcDataType::VarChar(1024),
            pattern: "{Description}".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use taos::IntoDsn;

    /// Helper to construct PspacePoint via JSON (since `data_type` field is private)
    fn make_pspace_point(
        id: u64,
        name: &str,
        ty: &str,
        long_name: &str,
        desc: Option<&str>,
    ) -> PspacePoint {
        let json = serde_json::json!({
            "id": id,
            "name": name,
            "type": ty,
            "long_name": long_name,
            "desc": desc,
        });
        serde_json::from_value(json).unwrap()
    }

    // ── default_primary_key_alias tests ────────────────────────────────

    #[test]
    fn test_default_primary_key_alias() {
        assert_eq!(default_primary_key_alias(ColumnConfig::ORIGINAL_TS), "ts");
        assert_eq!(default_primary_key_alias(ColumnConfig::REQUEST_TS), "qts");
        assert_eq!(default_primary_key_alias(ColumnConfig::RECEIVED_TS), "rts");
        assert_eq!(default_primary_key_alias("unknown_key"), "ts");
    }

    // ── default_pspace_custom_tags tests ───────────────────────────────

    #[test]
    fn test_default_pspace_custom_tags() {
        let tags = default_pspace_custom_tags();
        assert_eq!(tags.len(), 3);

        assert_eq!(tags[0].name, "Name");
        assert_eq!(tags[0].data_type, IpcDataType::VarChar(1024));
        assert_eq!(tags[0].pattern, "{Name}");

        assert_eq!(tags[1].name, "LongName");
        assert_eq!(tags[1].data_type, IpcDataType::VarChar(1024));
        assert_eq!(tags[1].pattern, "{LongName}");

        assert_eq!(tags[2].name, "Description");
        assert_eq!(tags[2].data_type, IpcDataType::VarChar(1024));
        assert_eq!(tags[2].pattern, "{Description}");
    }

    // ── PspaceModelConfig::TryFrom<&Dsn> tests ────────────────────────

    #[test]
    fn test_model_config_default_mode() {
        // No point_config_mode → defaults to select_all_points
        let dsn = "pspace://admin:admin888@127.0.0.1:5678".into_dsn().unwrap();
        let config = PspaceModelConfig::try_from(&dsn).unwrap();
        assert!(matches!(config, PspaceModelConfig::Select(_)));
    }

    #[test]
    fn test_model_config_select_mode() {
        let dsn = "pspace://admin:admin888@127.0.0.1:5678?point_config_mode=select_all_points"
            .into_dsn()
            .unwrap();
        let config = PspaceModelConfig::try_from(&dsn).unwrap();
        assert!(matches!(config, PspaceModelConfig::Select(_)));
    }

    #[test]
    fn test_model_config_csv_mode() {
        // Create a temp file so the file existence check passes
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "point_id,stable,tbname").unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let dsn = format!(
            "pspace://admin:admin888@127.0.0.1:5678?point_config_mode=csv_config_file&csv_config_file=@{}",
            path
        )
        .into_dsn()
        .unwrap();
        let config = PspaceModelConfig::try_from(&dsn).unwrap();
        assert!(matches!(config, PspaceModelConfig::Csv(_)));
    }

    #[test]
    fn test_model_config_unknown_mode() {
        let dsn = "pspace://admin:admin888@127.0.0.1:5678?point_config_mode=invalid_mode"
            .into_dsn()
            .unwrap();
        let err = PspaceModelConfig::try_from(&dsn).unwrap_err();
        assert!(err.to_string().contains("unknown point_config_mode"));
    }

    // ── SelectArgs::TryFrom<&Dsn> tests ────────────────────────────────

    #[test]
    fn test_select_args_defaults() {
        let dsn = "pspace://admin:admin888@127.0.0.1:5678".into_dsn().unwrap();
        let args = SelectArgs::try_from(&dsn).unwrap();

        assert_eq!(args.super_table_expression, "pspace_{type}");
        assert_eq!(args.child_table_expression, "t_{point_id}");
        assert_eq!(args.table_primary_key, ColumnConfig::ORIGINAL_TS);
        assert_eq!(args.table_primary_key_alias, "ts");
        assert_eq!(args.value_col, "val");
        assert!(args.value_transform.is_none());
        assert_eq!(args.quality_col, "quality");
        // Default custom tags
        assert_eq!(args.custom_tags.len(), 3);
        assert_eq!(args.custom_tags[0].name, "Name");
        assert_eq!(args.custom_tags[1].name, "LongName");
        assert_eq!(args.custom_tags[2].name, "Description");
    }

    #[test]
    fn test_select_args_custom_values() {
        let dsn = "pspace://admin:admin888@127.0.0.1:5678?\
            super_table_expression=my_stable_{type}&\
            child_table_expression=child_{point_id}&\
            table_primary_key=request_ts&\
            table_primary_key_alias=req_ts&\
            value_col=my_val&\
            value_transform=x*100&\
            quality_col=q"
            .into_dsn()
            .unwrap();
        let args = SelectArgs::try_from(&dsn).unwrap();

        assert_eq!(args.super_table_expression, "my_stable_{type}");
        assert_eq!(args.child_table_expression, "child_{point_id}");
        assert_eq!(args.table_primary_key, "request_ts");
        assert_eq!(args.table_primary_key_alias, "req_ts");
        assert_eq!(args.value_col, "my_val");
        assert_eq!(args.value_transform.as_deref(), Some("x*100"));
        assert_eq!(args.quality_col, "q");
    }

    #[test]
    fn test_select_args_primary_key_alias_defaults() {
        // request_ts → qts
        let dsn = "pspace://admin:admin888@127.0.0.1:5678?table_primary_key=request_ts"
            .into_dsn()
            .unwrap();
        let args = SelectArgs::try_from(&dsn).unwrap();
        assert_eq!(args.table_primary_key_alias, "qts");

        // received_ts → rts
        let dsn = "pspace://admin:admin888@127.0.0.1:5678?table_primary_key=received_ts"
            .into_dsn()
            .unwrap();
        let args = SelectArgs::try_from(&dsn).unwrap();
        assert_eq!(args.table_primary_key_alias, "rts");
    }

    // ── From<SelectArgs> for PspacePointMappingRule tests ──────────────

    #[test]
    fn test_select_args_to_mapping_rule() {
        let dsn = "pspace://admin:admin888@127.0.0.1:5678?\
            super_table_expression=st_{type}&\
            child_table_expression=ct_{point_id}&\
            value_col=v&\
            value_transform=x+1&\
            quality_col=q&\
            table_primary_key=request_ts&\
            table_primary_key_alias=req"
            .into_dsn()
            .unwrap();
        let args = SelectArgs::try_from(&dsn).unwrap();
        let rule: PspacePointMappingRule = args.into();

        assert_eq!(rule.inner.source_type, SourceType::Pspace);
        assert_eq!(rule.inner.stable_expression, "st_{type}");
        assert_eq!(rule.inner.tbname_expression, "ct_{point_id}");
        assert_eq!(rule.inner.value_col, "v");
        assert_eq!(rule.inner.value_transform.as_deref(), Some("x+1"));
        assert_eq!(rule.inner.quality_col, "q");
        assert_eq!(rule.inner.primary_key, "request_ts");
        assert_eq!(rule.inner.primary_key_alias, "req");
        assert!(rule.inner.custom_tags.is_some());
    }

    // ── PspacePointMappingRule::extra_custom_tags tests ────────────────

    #[test]
    fn test_extra_custom_tags_replacement() {
        let rule = PspacePointMappingRule {
            inner: PointMappingRule {
                source_type: SourceType::Pspace,
                stable_expression: "s".to_string(),
                tbname_expression: "t".to_string(),
                value_col: "val".to_string(),
                value_transform: None,
                quality_col: "quality".to_string(),
                primary_key: "original_ts".to_string(),
                primary_key_alias: "ts".to_string(),
                custom_tags: None,
            },
        };

        let point = make_pspace_point(1, "温度", "PS_ANALOG", r"\北京\温度", Some("温度传感器"));

        let mut tag_values = std::collections::HashMap::new();
        tag_values.insert("Name".to_string(), "{Name}".to_string());
        tag_values.insert("LongName".to_string(), "{LongName}".to_string());
        tag_values.insert("Description".to_string(), "{Description}".to_string());
        tag_values.insert("Mixed".to_string(), "prefix_{Name}_suffix".to_string());

        let mut pc = PointConfig {
            row_index: 0,
            code: "t_1".to_string(),
            stable: Some("s_float".to_string()),
            tag_values: Some(tag_values),
            value_type: None,
        };

        rule.extra_custom_tags(&mut pc, &point);

        let tags = pc.tag_values.as_ref().unwrap();
        assert_eq!(tags.get("Name").unwrap(), "温度");
        assert_eq!(tags.get("LongName").unwrap(), r"\北京\温度");
        assert_eq!(tags.get("Description").unwrap(), "温度传感器");
        assert_eq!(tags.get("Mixed").unwrap(), "prefix_温度_suffix");
    }

    #[test]
    fn test_extra_custom_tags_empty_description() {
        let rule = PspacePointMappingRule {
            inner: PointMappingRule {
                source_type: SourceType::Pspace,
                stable_expression: "s".to_string(),
                tbname_expression: "t".to_string(),
                value_col: "val".to_string(),
                value_transform: None,
                quality_col: "quality".to_string(),
                primary_key: "original_ts".to_string(),
                primary_key_alias: "ts".to_string(),
                custom_tags: None,
            },
        };

        let point = make_pspace_point(1, "温度", "PS_ANALOG", r"\北京\温度", None);

        let mut tag_values = std::collections::HashMap::new();
        tag_values.insert("Description".to_string(), "{Description}".to_string());

        let mut pc = PointConfig {
            row_index: 0,
            code: "t_1".to_string(),
            stable: None,
            tag_values: Some(tag_values),
            value_type: None,
        };

        rule.extra_custom_tags(&mut pc, &point);
        // desc is None → replaced with empty string
        assert_eq!(
            pc.tag_values.as_ref().unwrap().get("Description").unwrap(),
            ""
        );
    }

    #[test]
    fn test_extra_custom_tags_no_tag_values() {
        let rule = PspacePointMappingRule {
            inner: PointMappingRule {
                source_type: SourceType::Pspace,
                stable_expression: "s".to_string(),
                tbname_expression: "t".to_string(),
                value_col: "val".to_string(),
                value_transform: None,
                quality_col: "quality".to_string(),
                primary_key: "original_ts".to_string(),
                primary_key_alias: "ts".to_string(),
                custom_tags: None,
            },
        };

        let point = make_pspace_point(1, "温度", "PS_ANALOG", r"\北京\温度", None);

        let mut pc = PointConfig {
            row_index: 0,
            code: "t_1".to_string(),
            stable: None,
            tag_values: None, // no tags
            value_type: None,
        };

        // Should not panic
        rule.extra_custom_tags(&mut pc, &point);
        assert!(pc.tag_values.is_none());
    }

    // ── CsvArgs tests ──────────────────────────────────────────────────

    #[test]
    fn test_csv_args_with_at_prefix() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, "test").unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let dsn = format!(
            "pspace://admin:admin888@127.0.0.1:5678?csv_config_file=@{}",
            path
        )
        .into_dsn()
        .unwrap();
        let args = CsvArgs::try_from(&dsn).unwrap();
        assert_eq!(args.csv_config_file.to_string_lossy(), path);
    }

    #[test]
    fn test_csv_args_without_at_prefix() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, "test").unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let dsn = format!(
            "pspace://admin:admin888@127.0.0.1:5678?csv_config_file={}",
            path
        )
        .into_dsn()
        .unwrap();
        let args = CsvArgs::try_from(&dsn).unwrap();
        assert_eq!(args.csv_config_file.to_string_lossy(), path);
    }

    #[test]
    fn test_csv_args_missing_param() {
        let dsn = "pspace://admin:admin888@127.0.0.1:5678".into_dsn().unwrap();
        let err = CsvArgs::try_from(&dsn).unwrap_err();
        assert!(err.to_string().contains("csv_config_file is required"));
    }

    #[test]
    fn test_csv_args_file_not_found() {
        let dsn =
            "pspace://admin:admin888@127.0.0.1:5678?csv_config_file=/nonexistent/path/file.csv"
                .into_dsn()
                .unwrap();
        let err = CsvArgs::try_from(&dsn).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }
}
