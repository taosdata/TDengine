use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, bail};
use csv::{ReaderBuilder, StringRecord, Trim};
use taos::Ty;
use taosx_core::sink::point::model::{
    ColumnConfig, PointConfig, PointModelConfig, SourceType, TableConfig,
    generate_stable_from_pattern, generate_tbname_from_pattern,
};
use taosx_ipc::prelude::IpcDataType;

pub async fn parse_point_model_config_from_csv_file(
    path: &Path,
) -> anyhow::Result<PointModelConfig> {
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read pSpace CSV file: {}", path.display()))?;
    parse_point_model_config_from_csv_content(&content)
}

pub fn parse_point_model_config_from_csv_content(
    content: &str,
) -> anyhow::Result<PointModelConfig> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .trim(Trim::All)
        .from_reader(content.as_bytes());

    let headers = rdr
        .headers()
        .context("failed to read pSpace CSV headers")?
        .clone();

    let mut col_idx = HashMap::new();
    for (index, h) in headers.iter().enumerate() {
        col_idx.insert(h.trim().to_string(), index);
    }

    let point_id_idx = required_column(&col_idx, "point_id")?;
    let stable_idx = required_column(&col_idx, "stable")?;
    let tbname_idx = required_column(&col_idx, "tbname")?;

    let value_col_idx = col_idx.get("value_col").copied();
    let value_transform_idx = col_idx.get("value_transform").copied();
    let value_type_idx = col_idx.get("type").copied();
    let quality_col_idx = col_idx.get("quality_col").copied();
    let ts_col_idx = col_idx.get("ts_col").copied();
    let ts_transform_idx = col_idx.get("ts_transform").copied();
    let request_ts_col_idx = col_idx.get("request_ts_col").copied();
    let request_ts_transform_idx = col_idx.get("request_ts_transform").copied();
    let received_ts_col_idx = col_idx.get("received_ts_col").copied();
    let received_ts_transform_idx = col_idx.get("received_ts_transform").copied();

    let tag_columns = parse_tag_columns(&headers)?;

    let mut model_config = PointModelConfig {
        source_type: SourceType::Pspace,
        update_mode: None,
        generate_rule: None,
        point_config_map: Default::default(),
        table_config_map: Default::default(),
        node_config_map: None,
    };

    let mut row_count = 0usize;
    for (index, row) in rdr.records().enumerate() {
        let row =
            row.with_context(|| format!("failed to parse pSpace CSV at row {}", index + 2))?;

        let point_id = field_required(&row, point_id_idx, "point_id")?.to_string();
        let stable_expr = field_required(&row, stable_idx, "stable")?;
        let tbname_expr = field_required(&row, tbname_idx, "tbname")?;

        let value_type = field_optional(&row, value_type_idx)
            .map(|v| {
                IpcDataType::from_str(v).map_err(|e| {
                    anyhow::anyhow!("invalid data type for point_id {}: {}", point_id, e)
                })
            })
            .transpose()?;
        let stable = generate_stable_from_pattern(stable_expr, &value_type);
        let tbname = generate_tbname_from_pattern("pspace", tbname_expr, &point_id);

        let tag_values = build_tag_values(&row, &tag_columns);

        let point_config = PointConfig {
            row_index: index,
            code: tbname,
            stable: Some(stable),
            tag_values,
            value_type: value_type.clone(),
        };

        let value_col_alias = field_optional(&row, value_col_idx)
            .unwrap_or("val")
            .to_string();
        let value_transform = field_optional(&row, value_transform_idx).map(str::to_string);
        let quality_col_alias = field_optional(&row, quality_col_idx)
            .unwrap_or("quality")
            .to_string();

        let ts_col = field_optional(&row, ts_col_idx).map(str::to_string);
        let ts_transform = field_optional(&row, ts_transform_idx).map(str::to_string);
        let request_ts_col = field_optional(&row, request_ts_col_idx).map(str::to_string);
        let request_ts_transform =
            field_optional(&row, request_ts_transform_idx).map(str::to_string);
        let received_ts_col = field_optional(&row, received_ts_col_idx).map(str::to_string);
        let received_ts_transform =
            field_optional(&row, received_ts_transform_idx).map(str::to_string);

        let mut column_configs = vec![
            ColumnConfig {
                name: ColumnConfig::VALUE.to_string(),
                r#type: value_type.map(|v| v.ty()),
                alias: Some(value_col_alias),
                transform: value_transform,
                is_primary_key: false,
            },
            ColumnConfig {
                name: ColumnConfig::QUALITY.to_string(),
                r#type: Some(Ty::Int),
                alias: Some(quality_col_alias),
                transform: None,
                is_primary_key: false,
            },
        ];

        let mut has_primary_key = false;
        if let Some(alias) = ts_col {
            column_configs.push(ColumnConfig {
                name: ColumnConfig::ORIGINAL_TS.to_string(),
                r#type: Some(Ty::Timestamp),
                alias: Some(alias),
                transform: ts_transform,
                is_primary_key: true,
            });
            has_primary_key = true;
        }

        if let Some(alias) = request_ts_col {
            column_configs.push(ColumnConfig {
                name: ColumnConfig::REQUEST_TS.to_string(),
                r#type: Some(Ty::Timestamp),
                alias: Some(alias),
                transform: request_ts_transform,
                is_primary_key: !has_primary_key,
            });
            if !has_primary_key {
                has_primary_key = true;
            }
        }

        if let Some(alias) = received_ts_col {
            column_configs.push(ColumnConfig {
                name: ColumnConfig::RECEIVED_TS.to_string(),
                r#type: Some(Ty::Timestamp),
                alias: Some(alias),
                transform: received_ts_transform,
                is_primary_key: !has_primary_key,
            });
            if !has_primary_key {
                has_primary_key = true;
            }
        }

        if !has_primary_key {
            column_configs.push(ColumnConfig {
                name: ColumnConfig::ORIGINAL_TS.to_string(),
                r#type: Some(Ty::Timestamp),
                alias: Some("ts".to_string()),
                transform: None,
                is_primary_key: true,
            });
        }

        let tag_configs = if tag_columns.is_empty() {
            None
        } else {
            Some(
                tag_columns
                    .iter()
                    .map(
                        |(_, data_type, name)| taosx_core::sink::point::model::TagConfig {
                            name: name.clone(),
                            r#type: data_type.clone(),
                        },
                    )
                    .collect(),
            )
        };

        let table_config = TableConfig {
            enabled: Some(1),
            stable_prefix: None,
            column_configs,
            tag_configs,
        };

        model_config
            .point_config_map
            .insert(point_id.clone(), point_config);
        model_config.table_config_map.insert(point_id, table_config);
        row_count += 1;
    }

    if row_count == 0 {
        bail!("empty pSpace CSV file");
    }

    Ok(model_config)
}

fn required_column(col_idx: &HashMap<String, usize>, column_name: &str) -> anyhow::Result<usize> {
    col_idx.get(column_name).copied().ok_or(anyhow::anyhow!(
        "{} column is required in pSpace CSV",
        column_name
    ))
}

fn field_required<'a>(
    row: &'a StringRecord,
    index: usize,
    column_name: &str,
) -> anyhow::Result<&'a str> {
    row.get(index)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or(anyhow::anyhow!(
            "{} cannot be empty in pSpace CSV",
            column_name
        ))
}

fn field_optional(row: &StringRecord, index: Option<usize>) -> Option<&str> {
    index
        .and_then(|i| row.get(i))
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

fn parse_tag_columns(headers: &StringRecord) -> anyhow::Result<Vec<(usize, IpcDataType, String)>> {
    let mut tags = Vec::new();
    for (index, h) in headers.iter().enumerate() {
        let name = h.trim();
        if !name.starts_with("tag::") {
            continue;
        }

        let parts: Vec<&str> = name.splitn(3, "::").collect();
        if parts.len() != 3 {
            bail!("invalid tag column format: {}", name);
        }

        let data_type = IpcDataType::from_str(parts[1])
            .map_err(|e| anyhow::anyhow!("invalid tag type in column {}: {}", name, e))?;
        let tag_name = parts[2].trim();
        if tag_name.is_empty() {
            bail!("tag name cannot be empty: {}", name);
        }

        tags.push((index, data_type, tag_name.to_string()));
    }
    Ok(tags)
}

fn build_tag_values(
    row: &StringRecord,
    tag_columns: &[(usize, IpcDataType, String)],
) -> Option<HashMap<String, String>> {
    let mut values = HashMap::new();
    for (index, _type, name) in tag_columns {
        if let Some(v) = row.get(*index).map(str::trim)
            && !v.is_empty()
        {
            values.insert(name.clone(), v.to_string());
        }
    }
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taosx_core::sink::point::model::ColumnConfig;

    #[test]
    fn test_required_column() {
        // found required columns
        let mut map = HashMap::new();
        map.insert("point_id".to_string(), 0usize);
        map.insert("stable".to_string(), 1);
        assert_eq!(required_column(&map, "point_id").unwrap(), 0);
        assert_eq!(required_column(&map, "stable").unwrap(), 1);

        // Missing column
        let map = HashMap::new();
        let err = required_column(&map, "point_id").unwrap_err();
        assert!(err.to_string().contains("point_id"));
        assert!(err.to_string().contains("required"));
    }

    #[test]
    fn test_field_required() {
        // valid values
        let row = StringRecord::from(vec!["hello", "world"]);
        assert_eq!(field_required(&row, 0, "col_a").unwrap(), "hello");
        assert_eq!(field_required(&row, 1, "col_b").unwrap(), "world");

        // empty values
        let row = StringRecord::from(vec!["", "  "]);
        assert!(field_required(&row, 0, "col_a").is_err());
        assert!(field_required(&row, 1, "col_b").is_err());

        // missing index
        let row = StringRecord::from(vec!["  hello  "]);
        assert_eq!(field_required(&row, 0, "col").unwrap(), "hello");

        // index out of bounds
        let row = StringRecord::from(vec!["value"]);
        assert_eq!(field_optional(&row, Some(0)), Some("value"));

        // index out of bounds should return None for optional
        let row = StringRecord::from(vec!["value"]);
        assert_eq!(field_optional(&row, None), None);

        // empty string should be treated as None for optional
        let row = StringRecord::from(vec![""]);
        assert_eq!(field_optional(&row, Some(0)), None);

        // string with only whitespace should also be treated as None for optional
        let row = StringRecord::from(vec!["   "]);
        assert_eq!(field_optional(&row, Some(0)), None);
    }

    #[test]
    fn test_parse_tag_columns() {
        // valid tag columns
        let headers = StringRecord::from(vec![
            "point_id",
            "stable",
            "tag::VARCHAR(1024)::name",
            "tag::INT::sensor_id",
        ]);
        let tags = parse_tag_columns(&headers).unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].0, 2); // index
        assert_eq!(tags[0].1, IpcDataType::VarChar(1024));
        assert_eq!(tags[0].2, "name");
        assert_eq!(tags[1].0, 3);
        assert_eq!(tags[1].2, "sensor_id");

        // no tag columns
        let headers = StringRecord::from(vec!["point_id", "stable", "tbname"]);
        let tags = parse_tag_columns(&headers).unwrap();
        assert!(tags.is_empty());

        // Only 2 parts instead of 3
        let headers = StringRecord::from(vec!["tag::VARCHAR(1024)"]);
        let err = parse_tag_columns(&headers).unwrap_err();
        assert!(err.to_string().contains("invalid tag column format"));

        // Empty tag name
        let headers = StringRecord::from(vec!["tag::VARCHAR(1024)::"]);
        let err = parse_tag_columns(&headers).unwrap_err();
        assert!(err.to_string().contains("tag name cannot be empty"));
    }

    #[test]
    fn test_build_tag_values() {
        // with valid tag values
        let row = StringRecord::from(vec!["id1", "stable1", "tag_val1", "tag_val2"]);
        let tag_columns = vec![
            (2, IpcDataType::VarChar(1024), "name".to_string()),
            (3, IpcDataType::VarChar(1024), "desc".to_string()),
        ];
        let values = build_tag_values(&row, &tag_columns);
        assert!(values.is_some());
        let map = values.unwrap();
        assert_eq!(map.get("name").unwrap(), "tag_val1");
        assert_eq!(map.get("desc").unwrap(), "tag_val2");

        // empty tag values should be ignored
        let row = StringRecord::from(vec!["id1", "stable1", "", ""]);
        let tag_columns = vec![
            (2, IpcDataType::VarChar(1024), "name".to_string()),
            (3, IpcDataType::VarChar(1024), "desc".to_string()),
        ];
        let values = build_tag_values(&row, &tag_columns);
        assert!(values.is_none());

        // mix of valid and empty tag values → only valid ones should be included
        let row = StringRecord::from(vec!["id1", "stable1", "tag_val", ""]);
        let tag_columns = vec![
            (2, IpcDataType::VarChar(1024), "name".to_string()),
            (3, IpcDataType::VarChar(1024), "desc".to_string()),
        ];
        let values = build_tag_values(&row, &tag_columns).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values.get("name").unwrap(), "tag_val");

        // no tag columns → should return None
        let row = StringRecord::from(vec!["id1", "stable1"]);
        let tag_columns: Vec<(usize, IpcDataType, String)> = vec![];
        let values = build_tag_values(&row, &tag_columns);
        assert!(values.is_none());
    }

    #[test]
    fn test_parse_csv_minimal() {
        let csv = "\
point_id,stable,tbname
150017,pspace_{type},t_{point_id}
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        assert_eq!(config.source_type, SourceType::Pspace);
        assert_eq!(config.point_config_map.len(), 1);
        assert_eq!(config.table_config_map.len(), 1);

        // Check point config
        let pc = config.point_config_map.get("150017").unwrap();
        assert_eq!(pc.row_index, 0);
        assert_eq!(pc.code, "t_150017"); // {point_id} replaced
        assert!(pc.tag_values.is_none());

        // Check table config: should have value, quality, and default original_ts
        let tc = config.table_config_map.get("150017").unwrap();
        assert_eq!(tc.enabled, Some(1));
        assert!(tc.tag_configs.is_none()); // no tag columns

        let cols = &tc.column_configs;
        // value col
        assert_eq!(cols[0].name, ColumnConfig::VALUE);
        assert_eq!(cols[0].alias.as_deref(), Some("val"));
        assert!(!cols[0].is_primary_key);
        // quality col
        assert_eq!(cols[1].name, ColumnConfig::QUALITY);
        assert_eq!(cols[1].alias.as_deref(), Some("quality"));
        assert!(!cols[1].is_primary_key);
        // default primary key (no ts_col provided)
        assert_eq!(cols[2].name, ColumnConfig::ORIGINAL_TS);
        assert_eq!(cols[2].alias.as_deref(), Some("ts"));
        assert!(cols[2].is_primary_key);
    }

    #[test]
    fn test_parse_csv_full_columns() {
        let csv = "\
No.,point_id,stable,tbname,value_col,value_transform,type,quality_col,ts_col,ts_transform,request_ts_col,request_ts_transform,received_ts_col,received_ts_transform,tag::VARCHAR(1024)::name,tag::VARCHAR(1024)::LongName,tag::VARCHAR(1024)::Description
1,150017,pspace_{type},t_{point_id},val,,,quality,ts,,qts,,rts,,气温,\\北京\\气温,
2,150019,pspace_{type},t_{point_id},val,,,quality,ts,,qts,,rts,,气温,\\北京\\朝阳\\气温,
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        assert_eq!(config.point_config_map.len(), 2);
        assert_eq!(config.table_config_map.len(), 2);

        // First point
        let pc1 = config.point_config_map.get("150017").unwrap();
        assert_eq!(pc1.row_index, 0);
        assert!(pc1.tag_values.is_some());
        let tags = pc1.tag_values.as_ref().unwrap();
        assert_eq!(tags.get("name").unwrap(), "气温");
        assert_eq!(tags.get("LongName").unwrap(), r"\北京\气温");

        // Check ts_col is primary key (highest priority)
        let tc1 = config.table_config_map.get("150017").unwrap();
        let ts_col = tc1
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::ORIGINAL_TS)
            .unwrap();
        assert!(ts_col.is_primary_key);
        assert_eq!(ts_col.alias.as_deref(), Some("ts"));

        // request_ts and received_ts should NOT be primary key
        let req_ts = tc1
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::REQUEST_TS)
            .unwrap();
        assert!(!req_ts.is_primary_key);

        let recv_ts = tc1
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::RECEIVED_TS)
            .unwrap();
        assert!(!recv_ts.is_primary_key);

        // Tag configs should exist
        let tag_configs = tc1.tag_configs.as_ref().unwrap();
        assert_eq!(tag_configs.len(), 3);
        assert_eq!(tag_configs[0].name, "name");
        assert_eq!(tag_configs[1].name, "LongName");
        assert_eq!(tag_configs[2].name, "Description");
    }

    #[test]
    fn test_parse_csv_primary_key_request_ts() {
        // No ts_col, but request_ts_col present → request_ts is primary
        let csv = "\
point_id,stable,tbname,request_ts_col
150017,pspace_{type},t_{point_id},qts
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        let tc = config.table_config_map.get("150017").unwrap();
        let req_ts = tc
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::REQUEST_TS)
            .unwrap();
        assert!(req_ts.is_primary_key);
        assert_eq!(req_ts.alias.as_deref(), Some("qts"));

        // No original_ts column should exist
        assert!(
            tc.column_configs
                .iter()
                .all(|c| c.name != ColumnConfig::ORIGINAL_TS)
        );
    }

    #[test]
    fn test_parse_csv_primary_key_received_ts() {
        // No ts_col, no request_ts_col, but received_ts_col → received_ts is primary
        let csv = "\
point_id,stable,tbname,received_ts_col
150017,pspace_{type},t_{point_id},rts
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        let tc = config.table_config_map.get("150017").unwrap();
        let recv_ts = tc
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::RECEIVED_TS)
            .unwrap();
        assert!(recv_ts.is_primary_key);
    }

    #[test]
    fn test_parse_csv_with_value_type() {
        let csv = "\
point_id,stable,tbname,type
150017,pspace_{type},t_{point_id},FLOAT
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        let pc = config.point_config_map.get("150017").unwrap();
        // stable should have {type} replaced with float type
        assert!(pc.stable.is_some());
        let tc = config.table_config_map.get("150017").unwrap();
        let val_col = tc
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::VALUE)
            .unwrap();
        assert!(val_col.r#type.is_some());
    }

    #[test]
    fn test_parse_csv_with_value_transform() {
        let csv = "\
point_id,stable,tbname,value_transform
150017,pspace_{type},t_{point_id},x * 100
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        let tc = config.table_config_map.get("150017").unwrap();
        let val_col = tc
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::VALUE)
            .unwrap();
        assert_eq!(val_col.transform.as_deref(), Some("x * 100"));
    }

    #[test]
    fn test_parse_csv_with_ts_transform() {
        let csv = "\
point_id,stable,tbname,ts_col,ts_transform
150017,pspace_{type},t_{point_id},ts,x + 8h
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        let tc = config.table_config_map.get("150017").unwrap();
        let ts_col = tc
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::ORIGINAL_TS)
            .unwrap();
        assert_eq!(ts_col.transform.as_deref(), Some("x + 8h"));
        assert!(ts_col.is_primary_key);
    }

    #[test]
    fn test_parse_csv_empty_body() {
        let csv = "point_id,stable,tbname\n";
        let err = parse_point_model_config_from_csv_content(csv).unwrap_err();
        assert!(err.to_string().contains("empty pSpace CSV file"));
    }

    #[test]
    fn test_parse_csv_missing_required_column_point_id() {
        let csv = "\
stable,tbname
pspace_{type},t_{point_id}
";
        let err = parse_point_model_config_from_csv_content(csv).unwrap_err();
        assert!(err.to_string().contains("point_id"));
    }

    #[test]
    fn test_parse_csv_missing_required_column_stable() {
        let csv = "\
point_id,tbname
150017,t_{point_id}
";
        let err = parse_point_model_config_from_csv_content(csv).unwrap_err();
        assert!(err.to_string().contains("stable"));
    }

    #[test]
    fn test_parse_csv_missing_required_column_tbname() {
        let csv = "\
point_id,stable
150017,pspace_{type}
";
        let err = parse_point_model_config_from_csv_content(csv).unwrap_err();
        assert!(err.to_string().contains("tbname"));
    }

    #[test]
    fn test_parse_csv_empty_required_field() {
        let csv = "\
point_id,stable,tbname
,pspace_{type},t_{point_id}
";
        let err = parse_point_model_config_from_csv_content(csv).unwrap_err();
        assert!(err.to_string().contains("point_id"));
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_parse_csv_custom_value_col() {
        let csv = "\
point_id,stable,tbname,value_col
150017,pspace_{type},t_{point_id},my_value
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        let tc = config.table_config_map.get("150017").unwrap();
        let val_col = tc
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::VALUE)
            .unwrap();
        assert_eq!(val_col.alias.as_deref(), Some("my_value"));
    }

    #[test]
    fn test_parse_csv_custom_quality_col() {
        let csv = "\
point_id,stable,tbname,quality_col
150017,pspace_{type},t_{point_id},q
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        let tc = config.table_config_map.get("150017").unwrap();
        let qual_col = tc
            .column_configs
            .iter()
            .find(|c| c.name == ColumnConfig::QUALITY)
            .unwrap();
        assert_eq!(qual_col.alias.as_deref(), Some("q"));
    }

    #[test]
    fn test_parse_csv_multiple_rows() {
        let csv = "\
point_id,stable,tbname
100,s1,t1
200,s2,t2
300,s3,t3
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        assert_eq!(config.point_config_map.len(), 3);
        assert_eq!(config.table_config_map.len(), 3);

        // Verify row indices
        assert_eq!(config.point_config_map.get("100").unwrap().row_index, 0);
        assert_eq!(config.point_config_map.get("200").unwrap().row_index, 1);
        assert_eq!(config.point_config_map.get("300").unwrap().row_index, 2);
    }

    #[test]
    fn test_parse_csv_whitespace_trimming() {
        let csv = "\
point_id , stable , tbname
  150017 , pspace_{type} , t_{point_id}
";
        let config = parse_point_model_config_from_csv_content(csv).unwrap();
        assert!(config.point_config_map.contains_key("150017"));
    }
}
