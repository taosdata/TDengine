use crate::runners::opc::model::{ColumnConfig, OpcModelConfig, TableConfig, TagConfig};
use crate::utils::sql::sql_value_escaped_fmt;
use anyhow::Context;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float16Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use rhai::{Dynamic, Scope, AST};
use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;
use taosx_ipc::prelude::{record_batch_to_column_view, IpcDataType};
use taosx_ipc::stream::point::{RecordMessage, RecordTransform};

static OPC_PARSER_ID_PATH_DEVICE: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    std::env::var("OPC_PARSER_ID_PATH_DEVICE")
        .map(|v| matches!(v.as_str(), "true" | "1" | "yes" | "on"))
        .unwrap_or(false)
});

/// 处理 Point RecordMessage 的 transform
/// request_ts 是 3.3.6.0 版本新增的字段，表示：轮询 OPC Server 的请求的发起时间
/// request_ts_transform 需要兼容之前的行为，如果 message 中没有 request_ts 字段，则不进行 transform
pub async fn handle_transform(
    message: &RecordMessage,
    config: &OpcModelConfig,
) -> anyhow::Result<RecordMessage> {
    let transform_columns: [&str; 4] = [
        ColumnConfig::VALUE,
        ColumnConfig::ORIGINAL_TS,
        ColumnConfig::REQUEST_TS,
        ColumnConfig::RECEIVED_TS,
    ];
    let transform_config = config.transform_map(&transform_columns).await?;

    // id
    let id_col = message.clone_column_by_name("id")?;

    // name
    let name_col = message.clone_column_by_name("name")?;

    // transform ts
    let ts_config_map = config.get_column_config_map_by_name(ColumnConfig::ORIGINAL_TS);
    let mut ts_transform = to_record_transform_map(&ts_config_map);
    if let Some(generated_ts_config) = transform_config.get(ColumnConfig::ORIGINAL_TS) {
        let generated_ts_transform_map = to_record_transform_map(generated_ts_config);
        for (point_id, transform) in generated_ts_transform_map {
            ts_transform.entry(point_id).or_insert(transform);
        }
    }
    let transformed_ts_col = transform_by_name(message.record(), "ts", ts_transform)?;

    // transform received_ts
    let rts_config_map = config.get_column_config_map_by_name(ColumnConfig::RECEIVED_TS);
    let mut rts_transform = to_record_transform_map(&rts_config_map);
    if let Some(generated_rts_config) = transform_config.get(ColumnConfig::RECEIVED_TS) {
        let generated_rts_transform_map = to_record_transform_map(generated_rts_config);
        for (point_id, transform) in generated_rts_transform_map {
            rts_transform.entry(point_id).or_insert(transform);
        }
    }
    let transformed_received_col = transform_by_name(message.record(), "received", rts_transform)?;

    // transform value
    let val_config_map = config.get_column_config_map_by_name(ColumnConfig::VALUE);
    let mut value_transform = to_record_transform_map(&val_config_map);
    if let Some(generated_value_config) = transform_config.get(ColumnConfig::VALUE) {
        let generated_value_transform = to_record_transform_map(generated_value_config);
        for (point_id, transform) in generated_value_transform {
            value_transform.entry(point_id).or_insert(transform);
        }
    }
    let transformed_value_col = transform_by_name(message.record(), "value", value_transform)?;

    // status
    let status_col = message.clone_column_by_name("status")?;

    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("ts", transformed_ts_col.data_type().clone(), false),
        Field::new(
            "received",
            transformed_received_col.data_type().clone(),
            false,
        ),
        Field::new("value", transformed_value_col.data_type().clone(), true),
        Field::new("status", DataType::Int64, false),
    ];

    let mut columns = vec![
        id_col,
        name_col,
        transformed_ts_col,
        transformed_received_col,
        transformed_value_col,
        status_col,
    ];

    // request
    let schema = message.schema();
    if let Some((_idx, _f)) = schema.column_with_name("request") {
        let qts_config_map = config.get_column_config_map_by_name(ColumnConfig::REQUEST_TS);
        let mut qts_transform = to_record_transform_map(&qts_config_map);
        if let Some(generated_qts_config) = transform_config.get(ColumnConfig::REQUEST_TS) {
            let generated_qts_transform_map = to_record_transform_map(generated_qts_config);
            for (point_id, transform) in generated_qts_transform_map {
                qts_transform.entry(point_id).or_insert(transform);
            }
        }
        let transformed_request_col =
            transform_by_name(message.record(), "request", qts_transform)?;
        fields.push(Field::new(
            "request",
            transformed_request_col.data_type().clone(),
            false,
        ));
        columns.push(transformed_request_col);
    }

    let schema = Schema::new(fields);

    let transformed_record = RecordBatch::try_new(Arc::new(schema), columns)?;

    Ok(RecordMessage::from_record(transformed_record))
}

/// convert ColumnConfig map to RecordTransform map
/// return (point_id, RecordTransform) pairs
fn to_record_transform_map(
    config_map: &HashMap<String, ColumnConfig>,
) -> HashMap<String, RecordTransform> {
    config_map
        .iter()
        .filter(|(_, ts_config)| ts_config.transform.is_some())
        .map(|(point_id, ts_config)| {
            let transform = RecordTransform {
                column_name: ts_config.alias.clone(),
                transform_expression: ts_config.transform.clone(),
            };
            (point_id.clone(), transform)
        })
        .collect()
}

/// get a transformed column by name and data type
/// # Arguments
/// * `col_name` - column name
/// * `col_type` - column data type
/// * `transform_map` - (point_id, transform_expression) pairs
fn transform_by_name(
    record: &RecordBatch,
    col_name: &str,
    transform_map: HashMap<String, RecordTransform>,
) -> anyhow::Result<ArrayRef> {
    let rows = record.num_rows();
    if transform_map.is_empty() || rows == 0 {
        let raw_column = record
            .column_by_name(col_name)
            .ok_or(anyhow::anyhow!(
                "column: {} not exist in record batch",
                col_name
            ))?
            .clone();
        return Ok(raw_column);
    }

    let schema = record.schema();
    let columns = record.columns();
    let id_col_index = schema.index_of("id").unwrap();
    let col_index = schema.index_of(col_name).unwrap();
    let col_type = schema.field(col_index).data_type();

    let mut values: Vec<Dynamic> = Vec::with_capacity(rows);
    for row_index in 0..rows {
        let point_id = columns[id_col_index]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row_index);

        let expression = get_transform_expression_by_id(point_id, &transform_map);
        match expression {
            Some((name, expr)) => {
                let mut scope = Scope::new();
                match col_type {
                    DataType::Boolean => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    DataType::Int8 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Int16 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Int32 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Int64 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::UInt8 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::UInt16 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::UInt32 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::UInt64 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Float16 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Float16Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value.to_f64());
                    }
                    DataType::Float32 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Float64 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    DataType::Binary => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, String::from_utf8_lossy(value).to_string());
                    }
                    DataType::Utf8 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value.to_string());
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, None) => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, None) => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    dt => {
                        tracing::warn!(
                            "unsupported data type: {}, expression scope not set",
                            dt.clone()
                        )
                    }
                }
                let new_value: Dynamic = crate::utils::rhai_syntax_validator::ENGINE
                    .eval_ast_with_scope(&mut scope, &expr)
                    .unwrap_or(Dynamic::UNIT);
                values.push(new_value);
            }
            None => {
                // no transform expression for this point_id, use raw value
                let value: Dynamic = to_dynamic_value(record, col_type, col_index, row_index)?;
                values.push(value);
            }
        }
    }

    let mut is_none = true;
    for v in &values {
        if !v.is_unit() {
            is_none = false;
        }
    }

    if is_none || values.is_empty() {
        let raw_column = record
            .column_by_name(col_name)
            .ok_or(anyhow::anyhow!(
                "column: {} not exist in record batch",
                col_name
            ))?
            .clone();
        return Ok(raw_column);
    }

    crate::plugins::expr::array_from_rhai_dynamics(values).ok_or(anyhow::anyhow!(
        "failed to transform Vec<Dynamic> to ArrayRef"
    ))
}

fn to_dynamic_value(
    record_batch: &RecordBatch,
    col_type: &DataType,
    col_index: usize,
    row_index: usize,
) -> anyhow::Result<Dynamic> {
    let columns = record_batch.columns();
    let value: Dynamic = match col_type {
        DataType::Boolean => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value)
        }
        DataType::Int8 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Int16 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Int32 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Int64 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::UInt8 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::UInt16 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::UInt32 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::UInt64 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Float16 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Float16Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value.to_f64())
        }
        DataType::Float32 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Float64 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value)
        }
        DataType::Binary => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(String::from_utf8_lossy(value).to_string())
        }
        DataType::Utf8 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value.to_string())
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(row_index);
            rhai::Dynamic::from(value)
        }
        dt => {
            unimplemented!("unsupported data type: {}", dt.clone())
        }
    };

    Ok(value)
}

fn get_transform_expression_by_id(
    id: &str,
    map: &HashMap<String, RecordTransform>,
) -> Option<(String, AST)> {
    map.get(id).and_then(|transform| {
        match (&transform.column_name, &transform.transform_expression) {
            (Some(name), Some(expr)) => crate::utils::rhai_syntax_validator::ENGINE
                .compile_expression(expr)
                .inspect_err(|e| {
                    tracing::warn!(
                        "failed to compile expression: {} and skip, err: {:?}",
                        expr,
                        e
                    )
                })
                .ok()
                .map(|ast| Some((name.clone(), ast)))?,
            _ => None,
        }
    })
}

/// Point 类型的 RecordMessage 转为 SQL 插入语句
pub async fn point_records_to_sql(
    message: RecordMessage,
    config: &OpcModelConfig,
    target_precision: taos::Precision,
) -> anyhow::Result<(
    HashMap<String, Vec<SqlInsertion>>,
    HashMap<String, HashMap<String, String>>,
)> {
    let mut point_config_map = config.point_config_map.clone();
    let mut table_config_map = config.table_config_map.clone();

    let cv_vec = record_batch_to_column_view(message.record(), target_precision);

    let schema = message.schema();
    // id: point_id
    let id_column_view = cv_vec.get(schema.index_of("id")?).unwrap();
    // name：point_name
    let name_column_view = cv_vec.get(schema.index_of("name")?).unwrap();
    // ts：original_ts
    let ts_column_view = cv_vec.get(schema.index_of("ts")?).unwrap();
    // value：value
    let value_column_view = cv_vec.get(schema.index_of("value")?).unwrap();
    let value_raw_type = IpcDataType::from(schema.field_with_name("value")?.data_type());
    // received: rts
    let received_column_view = cv_vec.get(schema.index_of("received")?).unwrap();
    // status: quality
    let status_column_view = cv_vec.get(schema.index_of("status")?).unwrap();
    // request: qts
    let request_column_view = schema
        .index_of("request")
        .ok()
        .and_then(|idx| cv_vec.get(idx));

    // (k: stable, v: Vec<SqlInsertion>)
    let mut stable_insert_map: HashMap<String, Vec<SqlInsertion>> = HashMap::new();
    // (k: stable, v: (k: child_table_name, v: sql))
    let mut child_table_create_sql_map: HashMap<String, HashMap<String, String>> = HashMap::new();

    for i in 0..id_column_view.len() {
        let point_id = id_column_view
            .get(i)
            .ok_or(anyhow::anyhow!("id not found"))?
            .to_string()
            .context("invalid id value")?;
        let point_config = point_config_map.get(&point_id);
        tracing::info!(?point_config);
        let point_id_short = crate::runners::opc::generate_tag_value_from_pattern(
            config.opc_type.as_static_str(),
            "{id}",
            &point_id,
        );
        let path = crate::runners::opc::generate_tag_value_from_pattern(
            config.opc_type.as_static_str(),
            "{id..}",
            &point_id,
        );
        let point_path = sql_value_escaped_fmt(&path);

        let device = crate::runners::opc::generate_tag_value_from_pattern(
            config.opc_type.as_static_str(),
            "{..id.}",
            &point_id,
        );
        let point_device = sql_value_escaped_fmt(&device);
        let point_id_short_sql = sql_value_escaped_fmt(&point_id_short);
        let point_id_sql = id_column_view
            .get(i)
            .ok_or(anyhow::anyhow!("id not found"))?
            .to_sql_value();

        let mapping = config.get_point_mapping(&point_id)?;
        if mapping.is_none() {
            // 如果在一开始的 modelConfig 中找不到点位对应的 PoingConfig 和 TableConfig，则尝试使用规则生成
            tracing::warn!(
                "point mapping not found and try to auto generate, point_id: {}",
                point_id
            );
            let mapping = config
                .generate_point_mapping(&point_id, &value_raw_type)
                .await;
            match mapping {
                Err(err) => {
                    tracing::warn!(
                        "failed to generate point mapping with point_id: {}, cause: {:?}",
                        point_id,
                        err
                    );
                    continue;
                }
                Ok((p, t)) => {
                    tracing::debug!(
                        "generate point mapping, point config: {:?}, table config: {:?}",
                        p,
                        t
                    );
                    point_config_map.insert(point_id.clone(), p);
                    table_config_map.insert(point_id.clone(), t);
                }
            }
        }
        let point_config = point_config_map.get(&point_id).unwrap();
        let table_config = table_config_map.get(&point_id).unwrap();

        // stable_name
        let stable_name = stable_name(
            &point_config.stable,
            &table_config.stable_prefix,
            &value_raw_type,
        )
        .ok_or(anyhow::anyhow!(
            "failed to get stable name, point_id: {}, point_config: {:?}, table_config: {:?}",
            point_id,
            point_config,
            table_config
        ))?;

        // point_insertion
        let point_insertion = PointInsertion::from_table_config(table_config, &value_raw_type);

        let mut value_column_name = "value";
        let mut value_column_length: usize = 0;
        // Columns
        let mut values = String::new();
        let mut columns_in_insert = String::new();
        for (temp_name, temp_alias) in &point_insertion.columns {
            match temp_name.as_str() {
                ColumnConfig::ORIGINAL_TS | "original_time" => {
                    let ts = format!(
                        "{},",
                        ts_column_view
                            .slice(i..i + 1)
                            .unwrap()
                            .get(0)
                            .unwrap()
                            .into_value()
                            .to_sql_value()
                    );
                    values.push_str(ts.as_str());
                    columns_in_insert.push_str(format!("`{temp_alias}`,").as_str());
                }
                ColumnConfig::REQUEST_TS => match request_column_view {
                    Some(request_column_view) => {
                        let qts = format!(
                            "{},",
                            request_column_view
                                .slice(i..i + 1)
                                .unwrap()
                                .get(0)
                                .unwrap()
                                .into_value()
                                .to_sql_value()
                        );
                        values.push_str(qts.as_str());
                        columns_in_insert.push_str(format!("`{temp_alias}`,").as_str());
                    }
                    None => {
                        tracing::warn!(
                            "request_ts not found in record message, point_id: {}",
                            point_id
                        );
                    }
                },
                ColumnConfig::RECEIVED_TS | "received_time" => {
                    let rts = format!(
                        "{},",
                        received_column_view
                            .slice(i..i + 1)
                            .unwrap()
                            .get(0)
                            .unwrap()
                            .into_value()
                            .to_sql_value()
                    );
                    values.push_str(rts.as_str());
                    columns_in_insert.push_str(format!("`{temp_alias}`,").as_str());
                }
                ColumnConfig::VALUE => {
                    let value_column = value_column_view
                        .slice(i..i + 1)
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .into_value()
                        .to_sql_value()
                        .replace("NaN", "NULL");
                    values.push_str(format!("{value_column},").as_str());
                    value_column_name = temp_alias;
                    value_column_length = cmp::max(value_column.len(), value_column_length);
                    columns_in_insert.push_str(format!("`{temp_alias}`,").as_str());
                }
                ColumnConfig::QUALITY => {
                    let quality = format!(
                        "{},",
                        status_column_view
                            .slice(i..i + 1)
                            .unwrap()
                            .get(0)
                            .unwrap()
                            .into_value()
                            .to_sql_value()
                    );
                    values.push_str(quality.as_str());
                    columns_in_insert.push_str(format!("`{temp_alias}`,").as_str());
                }
                _ => {
                    unreachable!("unexpected column name: {}", temp_name);
                }
            }
        }
        values.pop(); // remove last `,` in sql
        columns_in_insert.pop(); // remove last `,` in sql

        // Tags
        let mut tag_names = String::new();
        let mut tag_values = String::new();
        if let Some(tag_configs) = &table_config.tag_configs {
            for ele in tag_configs {
                let tag_name = ele.name.clone();
                tag_names.push_str(format!("`{}`,", tag_name).as_str());
                let value = point_config
                    .tag_values
                    .as_ref()
                    .unwrap()
                    .get(&tag_name)
                    .unwrap();
                let value = match ele.r#type {
                    IpcDataType::VarChar(_) | IpcDataType::NChar(_) | IpcDataType::Json => {
                        format!("\"{value}\"")
                    }
                    _ => value.to_string(),
                };
                tag_values.push_str(format!("{},", value.replace("NaN", "NULL")).as_str());
            }
            tag_names.pop(); // remove last `,` in sql
            tag_values.pop();
        }

        let child_table_name = point_config.code.to_string(); // tbname
        let point_name = name_column_view
            .slice(i..i + 1)
            .unwrap()
            .get(0)
            .unwrap()
            .to_sql_value();
        if tag_names.is_empty() {
            if child_table_create_sql_map.contains_key(&stable_name) {
                let map = child_table_create_sql_map.get_mut(&stable_name).unwrap();
                let sql = if *OPC_PARSER_ID_PATH_DEVICE {
                    format!(
                        "(`point_id`, `id`, `path`, `device`, `point_name`) TAGS ({}, {}, {}, {}, {})",
                        &point_id_sql, &point_id_short_sql, point_path, point_device, &point_name
                    )
                } else {
                    format!(
                        "(`point_id`, `point_name`) TAGS ({}, {})",
                        &point_id_sql, &point_name
                    )
                };
                map.insert(child_table_name.clone(), sql);
            } else {
                let mut map = HashMap::new();
                let sql = if *OPC_PARSER_ID_PATH_DEVICE {
                    format!(
                        "(`point_id`, `id`, `path`, `device`, `point_name`) TAGS ({}, {}, {}, {}, {})",
                        &point_id_sql, &point_id_short_sql, point_path, point_device, &point_name
                    )
                } else {
                    format!(
                        "(`point_id`, `point_name`) TAGS ({}, {})",
                        &point_id_sql, &point_name
                    )
                };
                map.insert(child_table_name.clone(), sql);
                child_table_create_sql_map.insert(stable_name.clone(), map);
            }
        } else if child_table_create_sql_map.contains_key(&stable_name) {
            let map = child_table_create_sql_map.get_mut(&stable_name).unwrap();
            map.insert(
                child_table_name.clone(),
                format!("({}) TAGS ({})", tag_names, tag_values),
            );
        } else {
            let mut map = HashMap::new();
            map.insert(
                child_table_name.clone(),
                format!("({}) TAGS ({})", tag_names, tag_values),
            );
            child_table_create_sql_map.insert(stable_name.clone(), map);
        }

        let sql_vec = stable_insert_map.get_mut(&stable_name);
        let mut insert_done = false;

        if sql_vec.is_none() {
            let sql = format!(
                "insert into `{}` ({}) VALUES ({})",
                child_table_name,
                columns_in_insert.as_str(),
                values
            );

            let value_column_type = match &point_config.value_type {
                // 如果在 CSV 中指定了点位的 type，则使用指定的 type
                Some(value_type) => value_type.sql_repr(),
                // 如果在 CSV 中没有指定点位的 type，则使用原始的 type
                None => value_raw_type.sql_repr(),
            };

            let sql_vec = vec![SqlInsertion {
                point_insertion: point_insertion.clone(),
                sql,
                overflow: false,
                value_column_type,
                modify: ModifyStructForPointMessage {
                    id: point_id,
                    point_name,
                    value_column_name: value_column_name.to_string(),
                    value_column_length,
                },
            }];
            stable_insert_map.insert(stable_name.clone(), sql_vec);
        } else {
            // 这部分是拼多个点位的sql，注意：需要合并 columnConfig, 合并modify
            let sql_vec = sql_vec.unwrap();

            for index in 0..sql_vec.len() {
                let sql_insertion = sql_vec.get_mut(index).unwrap();
                if sql_insertion.overflow {
                    continue;
                } else {
                    let sql_suffix = format!(
                        " `{child_table_name}` ({}) VALUES ({}) ",
                        columns_in_insert.as_str(),
                        values
                    );
                    if sql_insertion.sql.len() + sql_suffix.len() > 1000 * 1000 {
                        sql_insertion.overflow = true;
                        continue;
                    } else {
                        // 不同点位入同一张表的情况，需要合并column_configs
                        let exist_column_configs =
                            &mut sql_insertion.point_insertion.column_configs;
                        let column_configs = &table_config.column_configs;
                        for column_config in column_configs {
                            if !exist_column_configs.contains(column_config) {
                                exist_column_configs.push(column_config.clone());
                            }
                        }
                        // 需要更新 modify.value_column_length
                        let exist_value_column_length = sql_insertion.modify.value_column_length;
                        sql_insertion.modify.value_column_length =
                            cmp::max(exist_value_column_length, value_column_length);

                        sql_insertion.sql.push_str(sql_suffix.as_str());
                        insert_done = true;
                    }
                }
            }

            if !insert_done {
                let value_column_type = match &point_config.value_type {
                    // 如果在 CSV 中指定了点位的 type，则使用指定的 type
                    Some(value_type) => value_type.sql_repr(),
                    // 如果在 CSV 中没有指定点位的 type，则使用原始的 type
                    None => value_raw_type.sql_repr(),
                };

                let sql = format!(
                    "insert into `{}` ({}) VALUES ({})",
                    child_table_name,
                    columns_in_insert.as_str(),
                    values
                );

                sql_vec.push(SqlInsertion {
                    point_insertion: point_insertion.clone(),
                    sql,
                    overflow: false,
                    value_column_type,
                    modify: ModifyStructForPointMessage {
                        id: point_id,
                        point_name,
                        value_column_name: value_column_name.to_string(),
                        value_column_length,
                    },
                });
            }
        }
    }

    Ok((stable_insert_map, child_table_create_sql_map))
}

/// 按照 stable_name > {prefix}_{raw_type} > None 的顺序生成 stable_name
fn stable_name(
    stable_name: &Option<String>,
    prefix: &Option<String>,
    raw_type: &IpcDataType,
) -> Option<String> {
    if let Some(stable_name) = stable_name {
        if stable_name.contains("{type}") {
            let stable = match raw_type {
                IpcDataType::VarChar(_len) => stable_name.replace("{type}", "varchar"),
                IpcDataType::NChar(_len) => stable_name.replace("{type}", "nchar"),
                _ => stable_name.replace("{type}", &raw_type.sql_repr().replace(" ", "_")),
            };
            return Some(stable);
        } else {
            return Some(stable_name.clone());
        }
    }

    if let Some(prefix) = prefix {
        let stable_name = match raw_type {
            IpcDataType::VarChar(_len) => format!("{}_varchar", prefix),
            IpcDataType::NChar(_len) => format!("{}_nchar", prefix),
            _ => format!("{}_{}", prefix, raw_type.sql_repr().replace(" ", "_")),
        };
        return Some(stable_name);
    }

    None
}

#[derive(Clone, Debug)]
pub struct PointInsertion {
    pub column_configs: Vec<ColumnConfig>,
    pub tag_configs: Option<Vec<TagConfig>>,
    pub columns: Vec<(String, String)>, // column_name(original_ts/received_ts/value/quality), column_alias
    pub value_column_config: ColumnConfig,
    pub other_columns: String,
    pub tags: String,
}

impl PointInsertion {
    /// 从 TableConfig 生成 PointInsertion
    fn from_table_config(table_config: &TableConfig, raw_type: &IpcDataType) -> Self {
        let mut columns: Vec<(String, String)> = Vec::new();
        let mut value_column_config = ColumnConfig {
            name: ColumnConfig::VALUE.to_string(),
            r#type: None,
            alias: Some("val".to_string()),
            transform: None,
            is_primary_key: false,
        };
        let mut other_columns = String::new();

        for column_config in &table_config.column_configs {
            if column_config.is_primary_key {
                let primary_key_column_name = column_config.name.clone();
                let primary_key_column_alias = column_config
                    .alias
                    .clone()
                    .unwrap_or(primary_key_column_name.clone());
                columns.insert(
                    0,
                    (primary_key_column_name, primary_key_column_alias.clone()),
                );
                other_columns.insert_str(
                    0,
                    format!("`{primary_key_column_alias}` TIMESTAMP,").as_str(),
                );
            } else {
                let column_name = column_config.name.clone();
                let column_alias = column_config.alias.clone().unwrap_or(column_name.clone());

                columns.push((column_name, column_alias.clone()));

                let column_type = match &column_config.r#type {
                    Some(t) => t.to_string(),
                    None => raw_type.sql_repr().clone(),
                };

                if column_config.name == ColumnConfig::VALUE {
                    value_column_config.alias = Some(column_alias.clone());
                    value_column_config.r#type = column_config.r#type;
                } else {
                    other_columns.push_str(format!("`{column_alias}` {column_type},").as_str());
                }
            }
        }
        // remove last char
        other_columns.pop();

        // tags
        let tags = if table_config.tag_configs.is_none() {
            if *OPC_PARSER_ID_PATH_DEVICE {
                "`point_id` VARCHAR(256), `id` VARCHAR(256), `path` VARCHAR(256), `device` VARCHAR(256), `point_name` VARCHAR(256)".to_string()
            } else {
                "`point_id` VARCHAR(256), `point_name` VARCHAR(256)".to_string()
            }
        } else {
            let tag_configs = table_config.tag_configs.clone().unwrap();
            tag_configs
                .iter()
                .map(|tag| format!("`{}` {}", tag.name, tag.r#type.sql_repr()))
                .collect::<Vec<String>>()
                .join(",")
        };

        Self {
            column_configs: table_config.column_configs.clone(),
            tag_configs: table_config.tag_configs.clone(),
            columns,
            value_column_config,
            other_columns,
            tags,
        }
    }
}

#[derive(Debug)]
pub struct SqlInsertion {
    pub point_insertion: PointInsertion,
    pub sql: String,
    pub overflow: bool,
    pub value_column_type: String,
    pub modify: ModifyStructForPointMessage,
}

#[derive(Debug)]
pub struct ModifyStructForPointMessage {
    pub id: String,
    pub point_name: String,
    pub value_column_name: String,
    pub value_column_length: usize,
}

#[cfg(test)]
mod tests {
    use crate::plugins::runners::opc::csv::CsvParser;
    use arrow::array::{
        Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Instant;
    use taos::Dsn;
    use taosx_ipc::stream::point::RecordMessage;

    use super::*;

    #[tokio::test]
    async fn test_point_records_to_sql() {
        std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());

        // CSV 不包含 request_ts ，RecordBatch 不包含 request
        let message = mock_point_message();
        let dsn = Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-utf8.csv").unwrap();
        let parser = CsvParser::from_dsn(&dsn).unwrap();
        let config = parser.parse().await.unwrap();
        let (s, _t) = point_records_to_sql(message, &config, taos::Precision::Millisecond)
            .await
            .unwrap();
        assert_eq!(s.len(), 1);
        let opc_int = s.get("opc_int").unwrap();
        assert_eq!(opc_int.len(), 1);
        let sql = opc_int[0].sql.clone();
        assert_eq!(
            sql,
            "insert into `t_3_1005` (`ts`,`val`,`quality`,`rts`) VALUES (1700000000000,1,0,1700000000000) `t_3_1006` (`ts`,`val`,`quality`,`rts`) VALUES (1700000000000,2,1,1700000000000)  `t_3_1007` (`ts`,`val`,`quality`,`rts`) VALUES (1700000000000,3,0,1700000000000) "
        );

        // CSV 包含 request_ts，RecordBatch 不包含 request
        let message = mock_point_message();
        let dsn = Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-3.3.6.0.csv").unwrap();
        let parser = CsvParser::from_dsn(&dsn).unwrap();
        let config = parser.parse().await.unwrap();
        let (s, _t) = point_records_to_sql(message, &config, taos::Precision::Millisecond)
            .await
            .unwrap();
        assert_eq!(s.len(), 1);
        let opc_int = s.get("opc_int").unwrap();
        assert_eq!(opc_int.len(), 1);
        let sql = opc_int[0].sql.clone();
        assert_eq!(
            sql,
            "insert into `t_3_1005` (`ts`,`val`,`quality`,`rts`) VALUES (1700000000000,1,0,1700000000000) `t_3_1006` (`ts`,`val`,`quality`,`rts`) VALUES (1700000000000,2,1,1700000000000)  `t_3_1007` (`ts`,`val`,`quality`,`rts`) VALUES (1700000000000,3,0,1700000000000) "
        );

        // CSV 包含 request_ts，RecordBatch 包含 request
        let message = mock_point_message_2();
        let dsn = Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-3.3.6.0.csv").unwrap();
        let parser = CsvParser::from_dsn(&dsn).unwrap();
        let config = parser.parse().await.unwrap();
        let (s, _t) = point_records_to_sql(message, &config, taos::Precision::Millisecond)
            .await
            .unwrap();
        assert_eq!(s.len(), 1);
        let opc_int = s.get("opc_int").unwrap();
        assert_eq!(opc_int.len(), 1);
        let sql = opc_int[0].sql.clone();
        assert_eq!(
            sql,
            "insert into `t_3_1005` (`ts`,`val`,`quality`,`qts`,`rts`) VALUES (1700000000000,1,0,1700000000000,1700000000000) `t_3_1006` (`ts`,`val`,`quality`,`qts`,`rts`) VALUES (1700000000000,2,1,1700000000000,1700000000000)  `t_3_1007` (`ts`,`val`,`quality`,`qts`,`rts`) VALUES (1700000000000,3,0,1700000000000,1700000000000) "
        );

        // ns=3;s="数据块_1"."Tag1"
        let message = mock_point_message_3();
        let (s, _t) = point_records_to_sql(message, &config, taos::Precision::Millisecond)
            .await
            .unwrap();
        assert_eq!(s.len(), 1);
        let opc_int = s.get("opc_int").unwrap();
        assert_eq!(opc_int.len(), 1);
        let sql = opc_int[0].sql.clone();
        assert_eq!(sql, "insert into `t_3_\"数据块_1\"_\"Tag101\"` (`ts`,`val`,`quality`,`qts`,`rts`) VALUES (1600000000000,11,1,1600000000000,1600000000000) `t_3_\"数据块_1\"_\"Tag101\"` (`ts`,`val`,`quality`,`qts`,`rts`) VALUES (1700000000000,22,1,1700000000000,1700000000000)  `t_3_\"数据块_1\"_\"Tag101\"` (`ts`,`val`,`quality`,`qts`,`rts`) VALUES (1800000000000,33,1,1800000000000,1800000000000) ");
    }

    #[tokio::test]
    async fn test_handle_transform() {
        std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        let message = mock_point_message();

        let start = Instant::now();

        let dsn = Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-large.csv").unwrap();
        let parser = CsvParser::from_dsn(&dsn).unwrap();
        let model_config = parser.parse().await.unwrap();

        let transformed_msg = handle_transform(&message, &model_config).await.unwrap();

        assert!(start.elapsed().as_secs() < 5);

        let value = transformed_msg
            .record()
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(value, vec![12.0, 22.0, 32.0]);

        let ts = transformed_msg
            .record()
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ts, vec![1700028800000, 1700028800000, 1700028800000]);

        let rts = transformed_msg
            .record()
            .column_by_name("received")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(rts, vec![1700028800000, 1700028800000, 1700028800000]);
    }

    fn mock_point_message() -> RecordMessage {
        let fields = mock_fields(false);

        let r = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(StringArray::from(vec![
                    "ns=3;i=1005",
                    "ns=3;i=1006",
                    "ns=3;i=1007",
                ])),
                Arc::new(StringArray::from(vec!["标签5", "标签6", "标签7"])),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        1700000000000,
                        1700000000000,
                        1700000000000,
                    ])
                    .with_timezone_opt::<&str>(None),
                ),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        1700000000000,
                        1700000000000,
                        1700000000000,
                    ])
                    .with_timezone_opt::<&str>(None),
                ),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![0, 1, 0])),
            ],
        )
        .unwrap();
        RecordMessage::from_record(r)
    }

    fn mock_point_message_2() -> RecordMessage {
        let fields = mock_fields(true);

        let r = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(StringArray::from(vec![
                    "ns=3;i=1005",
                    "ns=3;i=1006",
                    "ns=3;i=1007",
                ])),
                Arc::new(StringArray::from(vec!["标签5", "标签6", "标签7"])),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        1700000000000,
                        1700000000000,
                        1700000000000,
                    ])
                    .with_timezone_opt::<&str>(None),
                ),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        1700000000000,
                        1700000000000,
                        1700000000000,
                    ])
                    .with_timezone_opt::<&str>(None),
                ),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![0, 1, 0])),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        1700000000000,
                        1700000000000,
                        1700000000000,
                    ])
                    .with_timezone_opt::<&str>(None),
                ),
            ],
        )
        .unwrap();
        RecordMessage::from_record(r)
    }

    fn mock_fields(with_qts: bool) -> Vec<Field> {
        let mut fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "received",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int32, true),
            Field::new("status", DataType::Int64, false),
        ];
        if with_qts {
            fields.push(Field::new(
                "request",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ));
        }
        fields
    }

    fn mock_point_message_3() -> RecordMessage {
        let fields = mock_fields(true);

        let r = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(StringArray::from(vec![
                    "ns=3;s=\"数据块_1\".\"Tag101\"",
                    "ns=3;s=\"数据块_1\".\"Tag101\"",
                    "ns=3;s=\"数据块_1\".\"Tag101\"",
                ])),
                Arc::new(StringArray::from(vec![
                    "\"数据块_1\".\"Tag101\"",
                    "\"数据块_1\".\"Tag101\"",
                    "\"数据块_1\".\"Tag101\"",
                ])),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        1600000000000,
                        1700000000000,
                        1800000000000,
                    ])
                    .with_timezone_opt::<&str>(None),
                ),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        1600000000000,
                        1700000000000,
                        1800000000000,
                    ])
                    .with_timezone_opt::<&str>(None),
                ),
                Arc::new(Int32Array::from(vec![11, 22, 33])),
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        1600000000000,
                        1700000000000,
                        1800000000000,
                    ])
                    .with_timezone_opt::<&str>(None),
                ),
            ],
        )
        .unwrap();
        RecordMessage::from_record(r)
    }
}
