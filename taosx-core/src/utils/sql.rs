use arrow::{array::Array, record_batch::RecordBatch};
use arrow_schema::ArrowError;
use itertools::Itertools;

/// Escape a string value for SQL.
pub fn sql_value_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('\'');

    for c in value.chars() {
        match c {
            '\'' => {
                escaped.push('\'');
                escaped.push('\'');
            }

            '\t' => {
                escaped.push('\\');
                escaped.push('t');
            }
            '\r' => {
                escaped.push('\\');
                escaped.push('r');
            }
            '\n' => {
                escaped.push('\\');
                escaped.push('n');
            }
            '\\' | '"' => {
                escaped.push('\\');
                escaped.push(c);
            }
            _ => escaped.push(c),
        }
    }
    escaped.push('\'');
    escaped
}

pub fn sql_max_var_length(batch: &RecordBatch) -> Vec<usize> {
    let mut lengths = vec![0; batch.num_columns()];

    for i in 0..batch.num_columns() {
        let array = batch.column(i);

        match array.data_type() {
            arrow_schema::DataType::Binary => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .unwrap();
                if let Some(len) = array.iter().flatten().map(|v| v.len()).max() {
                    lengths[i] = len;
                }
            }
            arrow_schema::DataType::FixedSizeBinary(len) => {
                lengths[i] = *len as _;
            }
            arrow_schema::DataType::LargeBinary => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::LargeBinaryArray>()
                    .unwrap();
                if let Some(len) = array.iter().flatten().map(|v| v.len()).max() {
                    lengths[i] = len;
                }
            }
            arrow_schema::DataType::Utf8 => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                if let Some(len) = array.iter().flatten().map(|v| v.len()).max() {
                    lengths[i] = len;
                }
            }
            arrow_schema::DataType::LargeUtf8 => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::LargeStringArray>()
                    .unwrap();
                if let Some(len) = array.iter().flatten().map(|v| v.len()).max() {
                    lengths[i] = len;
                }
            }
            _ => (),
        }
    }
    lengths
}

pub fn sql_values_from_record_batch(
    batch: &RecordBatch,
    precision: taos::Precision,
    with_field_names: bool,
) -> Result<Vec<(String, usize)>, arrow::error::ArrowError> {
    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }
    let schema = batch.schema();
    let names = schema
        .fields()
        .iter()
        .map(|f| format!("`{}`", f.name()))
        .join(",");
    let mut vec = Vec::with_capacity(1);
    let mut rows = 0;
    let mut values = String::with_capacity(256);
    if with_field_names {
        values.push('(');
        values.push_str(&names);
        values.push_str(") values");
    } else {
        values.push_str("values");
    }
    let columns = batch.columns();

    for row in 0..batch.num_rows() {
        if columns[0].is_null(row) {
            continue;
        }
        values.push('(');
        for col in 0..batch.num_columns() {
            let array = &columns[col];
            if col > 0 {
                values.push(',');
            }
            if array.is_null(row) {
                values.push_str("NULL");
                continue;
            }
            match columns[col].data_type() {
                arrow_schema::DataType::Null => {
                    values.push_str("NULL");
                }
                arrow_schema::DataType::Boolean => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .unwrap();
                    values.push_str(if array.value(row) { "true" } else { "false" });
                }
                arrow_schema::DataType::Int8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int8Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Int16 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int16Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Int32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Int64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt8Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt16 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt16Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt32Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Float16 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float16Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Float32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Float64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .unwrap();
                    values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Timestamp(unit, _) => match unit {
                    arrow_schema::TimeUnit::Second => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampSecondArray>()
                            .unwrap();
                        match precision {
                            taos::Precision::Millisecond => {
                                values.push_str(&(array.value(row) * 1000).to_string());
                            }
                            taos::Precision::Microsecond => {
                                values.push_str(&(array.value(row) * 1000_000).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                values.push_str(&(array.value(row) * 1000_000_000).to_string());
                            }
                        }
                    }
                    arrow_schema::TimeUnit::Millisecond => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                            .unwrap();

                        match precision {
                            taos::Precision::Millisecond => {
                                values.push_str(&(array.value(row)).to_string());
                            }
                            taos::Precision::Microsecond => {
                                values.push_str(&(array.value(row) * 1000).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                values.push_str(&(array.value(row) * 1000_000).to_string());
                            }
                        }
                    }
                    arrow_schema::TimeUnit::Microsecond => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                            .unwrap();

                        match precision {
                            taos::Precision::Millisecond => {
                                values.push_str(&(array.value(row) / 1000).to_string());
                            }
                            taos::Precision::Microsecond => {
                                values.push_str(&(array.value(row)).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                values.push_str(&(array.value(row) * 1000).to_string());
                            }
                        }
                    }
                    arrow_schema::TimeUnit::Nanosecond => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                            .unwrap();

                        match precision {
                            taos::Precision::Millisecond => {
                                values.push_str(&(array.value(row) / 1000_000).to_string());
                            }
                            taos::Precision::Microsecond => {
                                values.push_str(&(array.value(row) / 1000).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                values.push_str(&(array.value(row)).to_string());
                            }
                        }
                    }
                },
                arrow_schema::DataType::Binary => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::BinaryArray>()
                        .unwrap();
                    values.push_str(&sql_value_escape(&String::from_utf8_lossy(
                        array.value(row),
                    )));
                }
                arrow_schema::DataType::FixedSizeBinary(_) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                        .unwrap();
                    values.push_str(&sql_value_escape(&String::from_utf8_lossy(
                        array.value(row),
                    )));
                }
                arrow_schema::DataType::LargeBinary => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::LargeBinaryArray>()
                        .unwrap();
                    values.push_str(&sql_value_escape(&String::from_utf8_lossy(
                        array.value(row),
                    )));
                }
                arrow_schema::DataType::Utf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .unwrap();
                    values.push_str(&sql_value_escape(&array.value(row)));
                }
                arrow_schema::DataType::LargeUtf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                        .unwrap();
                    values.push_str(&sql_value_escape(&array.value(row)));
                }
                dt => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Convert `{dt:?}` to sql value"
                    )));
                }
            }
        }
        values.push(')');
        rows += 1;

        if values.len() > 900_000 {
            vec.push((values, rows));
            rows = 0;
            values = String::with_capacity(256);
            if with_field_names {
                values.push('(');
                values.push_str(&names);
                values.push_str(") values");
            } else {
                values.push_str("values");
            }
        }
    }

    if rows > 0 {
        vec.push((values, rows));
    }

    Ok(vec)
}

/// 对于属于同一个子表的一批 Records，生成一条 SQL 插入语句的 insert into 后面的部分，不使用自动建表语法。
/// 如果某个字段的值为 NULL，则跳过该字段。
/// 如：tablename (col_names) values (col_values) tablename (col_names) values (col_values) ...
pub fn sql_values_from_record_batch_skip_null(
    table_name: &str,
    batch: &RecordBatch,
    target_precision: taos::Precision,
) -> Result<Vec<String>, arrow::error::ArrowError> {
    let mut vec = Vec::with_capacity(1);
    let schema = batch.schema();
    let col_names = schema
        .fields()
        .iter()
        .map(|f| format!("`{}`", f.name()))
        .collect::<Vec<_>>();
    let columns = batch.columns();
    let mut sql = String::with_capacity(256);

    for row in 0..batch.num_rows() {
        if columns[0].is_null(row) {
            continue;
        }
        let mut insert_col_names = String::new();
        let mut insert_col_values = String::new();
        for col in 0..batch.num_columns() {
            let array = &columns[col];
            if array.is_null(row) {
                continue;
            }
            let column_data_type = columns[col].data_type();
            match column_data_type {
                arrow_schema::DataType::Null => {
                    continue;
                }
                _ => (),
            }
            insert_col_names.push_str(&col_names[col]);
            match column_data_type {
                arrow_schema::DataType::Boolean => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .unwrap();
                    insert_col_values.push_str(if array.value(row) { "true" } else { "false" });
                }
                arrow_schema::DataType::Int8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int8Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Int16 => {
                    let array: &arrow::array::PrimitiveArray<arrow::datatypes::Int16Type> = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int16Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Int32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Int64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt8Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt16 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt16Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt32Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Float16 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float16Array>()
                        .unwrap();
                    let v = array.value(row);
                    if v.is_nan() {
                        insert_col_values.push_str("NULL");
                    } else {
                        insert_col_values.push_str(&v.to_string());
                    }
                }
                arrow_schema::DataType::Float32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()
                        .unwrap();
                    let v = array.value(row);
                    if v.is_nan() {
                        insert_col_values.push_str("NULL");
                    } else {
                        insert_col_values.push_str(&v.to_string());
                    }
                }
                arrow_schema::DataType::Float64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .unwrap();
                    let v = array.value(row);
                    if v.is_nan() {
                        insert_col_values.push_str("NULL");
                    } else {
                        insert_col_values.push_str(&v.to_string());
                    }
                }
                arrow_schema::DataType::Timestamp(unit, _) => match unit {
                    arrow_schema::TimeUnit::Second => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampSecondArray>()
                            .unwrap();
                        match target_precision {
                            taos::Precision::Millisecond => {
                                insert_col_values.push_str(&(array.value(row) * 1000).to_string());
                            }
                            taos::Precision::Microsecond => {
                                insert_col_values
                                    .push_str(&(array.value(row) * 1000_000).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                insert_col_values
                                    .push_str(&(array.value(row) * 1000_000_000).to_string());
                            }
                        }
                    }
                    arrow_schema::TimeUnit::Millisecond => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                            .unwrap();

                        match target_precision {
                            taos::Precision::Millisecond => {
                                insert_col_values.push_str(&(array.value(row)).to_string());
                            }
                            taos::Precision::Microsecond => {
                                insert_col_values.push_str(&(array.value(row) * 1000).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                insert_col_values
                                    .push_str(&(array.value(row) * 1000_000).to_string());
                            }
                        }
                    }
                    arrow_schema::TimeUnit::Microsecond => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                            .unwrap();

                        match target_precision {
                            taos::Precision::Millisecond => {
                                insert_col_values.push_str(&(array.value(row) / 1000).to_string());
                            }
                            taos::Precision::Microsecond => {
                                insert_col_values.push_str(&(array.value(row)).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                insert_col_values.push_str(&(array.value(row) * 1000).to_string());
                            }
                        }
                    }
                    arrow_schema::TimeUnit::Nanosecond => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                            .unwrap();

                        match target_precision {
                            taos::Precision::Millisecond => {
                                insert_col_values
                                    .push_str(&(array.value(row) / 1000_000).to_string());
                            }
                            taos::Precision::Microsecond => {
                                insert_col_values.push_str(&(array.value(row) / 1000).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                insert_col_values.push_str(&(array.value(row)).to_string());
                            }
                        }
                    }
                },
                arrow_schema::DataType::Binary => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::BinaryArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(&String::from_utf8_lossy(
                        array.value(row),
                    )));
                }
                arrow_schema::DataType::FixedSizeBinary(_) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(&String::from_utf8_lossy(
                        array.value(row),
                    )));
                }
                arrow_schema::DataType::LargeBinary => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::LargeBinaryArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(&String::from_utf8_lossy(
                        array.value(row),
                    )));
                }
                arrow_schema::DataType::Utf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(&array.value(row)));
                }
                arrow_schema::DataType::LargeUtf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(&array.value(row)));
                }
                dt => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Convert `{dt:?}` to sql value"
                    )));
                }
            }
            insert_col_values.push(',');
            insert_col_names.push(',');
        }
        insert_col_values.pop();
        insert_col_names.pop();

        sql.push_str(&format!(
            " `{}` ({}) values ({})",
            table_name, insert_col_names, insert_col_values
        ));
        if sql.len() > 900_000 {
            vec.push(sql);
            sql = String::with_capacity(256);
        }
    }
    if sql.len() > 0 {
        vec.push(sql);
    }
    Ok(vec)
}

const MAX_SQL_LENGTH: usize = 1_000_000;

/// 二分法递归拼装 SQL 语句
/// 输入是一个元组数组，元组的第一个元素是一个 SQL 片段，只包含 insert into 后面的部分，第二个元素是记录数， 比如 ("table_name using super_table tags(a int) (c1, c2, c3) values (1,2,3) (4, 5, 6),(7, 8, 9)", 3)
/// 返回值是一个元组，第一个元素是完整的 SQL 语句，第二个元素是表的数量，第三个元素 SQL 的记录数
pub fn values_to_sqls(slice: &[(String, usize)]) -> Vec<(String, usize, usize)> {
    if slice.len() == 0 {
        return vec![];
    }
    if let Some(sql) = valid_sql_or_none(slice) {
        return vec![sql];
    }
    let p = (slice.len() + 1) / 2;
    let (left, right) = slice.split_at(p);
    let mut sqls = values_to_sqls(left);
    sqls.extend(values_to_sqls(right));
    sqls
}

/// 拼装 SQL 语句
/// 尝试将多个 table 的 values 部分拼装到一起，如果长度超过 MAX_SQL_LENGTH，则返回 None
fn valid_sql_or_none(
    slice: &[(
        String, // One table values SQL
        usize,  // One table records
    )],
) -> Option<(
    String, // SQL to insert into.
    usize,  // number of tables
    usize,  // number of records
)> {
    if slice.len() == 1 {
        return Some((format!("INSERT INTO {}", slice[0].0), 1, slice[0].1));
    }
    let len = slice.iter().map(|(sql, _)| sql.len()).sum::<usize>();
    if len < MAX_SQL_LENGTH - 12 {
        let mut sql = String::with_capacity(len + 12);
        sql.push_str("INSERT INTO ");
        let (sql, records) = slice.iter().fold((sql, 0), |(mut sql, records), (s, n)| {
            sql.push_str(s);
            (sql, records + n)
        });
        Some((sql, slice.len(), records))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::*;
    use std::sync::Arc;
    use taos::{AsyncQueryable, AsyncTBuilder};
    use taosx_ipc::prelude::IpcDataType;

    use super::*;

    const ROWS: usize = 10;
    fn valid_values_record() -> RecordBatch {
        let now = chrono::Utc::now().timestamp_millis();
        RecordBatch::try_from_iter(vec![
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    (0..ROWS).map(|i| now + i as i64 * 100),
                )) as ArrayRef,
            ),
            ("null", Arc::new(NullArray::new(ROWS))),
            (
                "bool",
                Arc::new(BooleanArray::from_iter(
                    (0..ROWS).map(|i| i % 2 == 0).map(Some),
                )) as ArrayRef,
            ),
            (
                "i8",
                Arc::new(Int8Array::from_iter_values((0..ROWS).map(|i| i as i8))) as ArrayRef,
            ),
            (
                "i16",
                Arc::new(Int16Array::from_iter_values((0..ROWS).map(|i| i as i16))) as ArrayRef,
            ),
            (
                "i32",
                Arc::new(Int32Array::from_iter_values((0..ROWS).map(|i| i as i32))) as ArrayRef,
            ),
            (
                "i64",
                Arc::new(Int64Array::from_iter_values((0..ROWS).map(|i| i as i64))) as ArrayRef,
            ),
            (
                "u8",
                Arc::new(UInt8Array::from_iter_values((0..ROWS).map(|i| i as u8))) as ArrayRef,
            ),
            (
                "u16",
                Arc::new(UInt16Array::from_iter_values((0..ROWS).map(|i| i as u16))) as ArrayRef,
            ),
            (
                "u32",
                Arc::new(UInt32Array::from_iter_values((0..ROWS).map(|i| i as u32))) as ArrayRef,
            ),
            (
                "u64",
                Arc::new(UInt64Array::from_iter_values((0..ROWS).map(|i| i as u64))) as ArrayRef,
            ),
            (
                "f32",
                Arc::new(Float32Array::from_iter_values((0..ROWS).map(|i| i as f32))) as ArrayRef,
            ),
            (
                "f64",
                Arc::new(Float64Array::from_iter_values((0..ROWS).map(|i| i as f64))) as ArrayRef,
            ),
            (
                "str",
                Arc::new(StringArray::from_iter_values(
                    (0..ROWS).map(|i| format!("str{}", i)),
                )) as ArrayRef,
            ),
            (
                "binary",
                Arc::new(BinaryArray::from_iter_values(
                    (0..ROWS).map(|i| format!("binary{}", i).into_bytes()),
                )) as ArrayRef,
            ),
            (
                "string",
                Arc::new(BinaryArray::from_iter_values(
                    (0..ROWS).map(|i| format!("string'\"\t\n!@#$%^&*()_-+={}`/?.,:;", i)),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn record_to_sql() {
        let batch = valid_values_record();
        let schema = batch.schema();
        let builder = taos::TaosBuilder::from_dsn("taos:///").unwrap();
        let taos = builder.build().await.unwrap();
        for precision in [
            taos::Precision::Millisecond,
            taos::Precision::Microsecond,
            taos::Precision::Nanosecond,
        ] {
            let db = format!("precision_{precision}");
            taos.exec_many([
                format!("drop database if exists {db}"),
                format!("create database {db} precision '{precision}'"),
                format!("use {db}"),
            ])
            .await
            .unwrap();

            let stable = "stb";

            let mut stable_create = format!("create stable {stable} (ts timestamp, `null` int");
            for i in 2..batch.num_columns() {
                let field = schema.field(i);
                let name = field.name();
                let ty: IpcDataType = field.data_type().into();
                stable_create.push_str(&format!(", `{name}` {ty}", name = name, ty = ty));
            }
            stable_create.push_str(") tags(t1 int)");
            taos.exec(&stable_create).await.unwrap();

            let table_prefix = "tb";
            let tables = 100;

            let (values, _size) =
                &sql_values_from_record_batch(&batch, precision, true).unwrap()[0];
            let mut sql = String::new();
            sql.push_str("insert into ");
            for i in 0..tables {
                sql.push_str(&format!(
                    "{table_prefix}_{i} using {stable} tags({i})",
                    table_prefix = table_prefix,
                    stable = stable,
                    i = i
                ));
                sql.push_str(&values);
            }

            let n = taos.exec(&sql).await.unwrap();

            assert_eq!(n, tables * ROWS);

            // taos.query("select * from {}")
        }
    }
}
