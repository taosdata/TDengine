use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use arrow::{
    array::{Array, ArrayRef},
    datatypes::{Schema, TimeUnit},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use std::io::prelude::*;
use taos::{
    AsyncFetchable, AsyncQueryable, ColumnView, Dsn, Field, Itertools, Precision, TBuilder, Taos,
    TaosBuilder, Ty,
};

use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};

fn fields_to_schema(fields: &[Field]) -> String {
    format!(
        "message schema {{ {} }}",
        fields
            .into_iter()
            .map(|f| match f.ty() {
                Ty::Bool => format!("optional bool {};", f.name()),
                Ty::TinyInt | Ty::SmallInt | Ty::Int | Ty::UTinyInt | Ty::USmallInt =>
                    format!("optional int32 {};", f.name()),
                Ty::BigInt | Ty::UInt | Ty::UBigInt => format!("optional int64 {};", f.name()),
                Ty::Float | Ty::Double => format!("optional double {};", f.name()),
                Ty::Timestamp => format!("optional int64 {};", f.name()),
                Ty::VarChar | Ty::NChar | Ty::Json => format!("optional int64 {};", f.name()),
                _ => todo!(),
            })
            .join("")
    )
}

fn precision_to_arrow(precision: Precision) -> TimeUnit {
    match precision {
        Precision::Millisecond => TimeUnit::Millisecond,
        Precision::Microsecond => TimeUnit::Microsecond,
        Precision::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn fields_to_arrow(fields: &[Field], precision: Precision) -> Schema {
    use arrow::datatypes::DataType;
    Schema::new(
        fields
            .into_iter()
            .map(|f| match f.ty() {
                Ty::Null => unreachable!("field should always have a known type"),
                Ty::Bool => arrow::datatypes::Field::new(f.name(), DataType::Boolean, true),
                Ty::TinyInt => arrow::datatypes::Field::new(f.name(), DataType::Int8, true),
                Ty::SmallInt => arrow::datatypes::Field::new(f.name(), DataType::Int16, true),
                Ty::Int => arrow::datatypes::Field::new(f.name(), DataType::Int32, true),
                Ty::BigInt => arrow::datatypes::Field::new(f.name(), DataType::Int64, true),
                Ty::Float => arrow::datatypes::Field::new(f.name(), DataType::Float32, true),
                Ty::Double => arrow::datatypes::Field::new(f.name(), DataType::Float64, true),
                Ty::VarChar => arrow::datatypes::Field::new(
                    f.name(),
                    DataType::FixedSizeBinary(f.bytes() as _),
                    true,
                ),
                Ty::Timestamp => arrow::datatypes::Field::new(
                    f.name(),
                    DataType::Timestamp(precision_to_arrow(precision), None),
                    true,
                ),
                Ty::NChar => arrow::datatypes::Field::new(
                    f.name(),
                    DataType::FixedSizeBinary(f.bytes() as i32 * 4),
                    true,
                ),
                Ty::UTinyInt => arrow::datatypes::Field::new(f.name(), DataType::UInt8, true),
                Ty::USmallInt => arrow::datatypes::Field::new(f.name(), DataType::UInt16, true),
                Ty::UInt => arrow::datatypes::Field::new(f.name(), DataType::UInt32, true),
                Ty::UBigInt => arrow::datatypes::Field::new(f.name(), DataType::UInt64, true),
                Ty::Json => arrow::datatypes::Field::new(f.name(), DataType::Utf8, true),
                Ty::VarBinary => todo!(),
                Ty::Decimal => todo!(),
                Ty::Blob => todo!(),
                Ty::MediumBlob => todo!(),
                _ => todo!(),
            })
            .collect_vec(),
    )
}

fn column_to_arrow(column: &ColumnView) -> ArrayRef {
    match column {
        ColumnView::Bool(v) => {
            ArrayRef::from(arrow::array::BooleanArray::from(v.to_vec()).into_data())
        }
        ColumnView::TinyInt(v) => {
            ArrayRef::from(arrow::array::Int8Array::from(v.to_vec()).into_data())
        }
        ColumnView::SmallInt(v) => {
            ArrayRef::from(arrow::array::Int16Array::from(v.to_vec()).into_data())
        }
        ColumnView::Int(v) => {
            ArrayRef::from(arrow::array::Int32Array::from(v.to_vec()).into_data())
        }
        ColumnView::BigInt(v) => {
            ArrayRef::from(arrow::array::Int64Array::from(v.to_vec()).into_data())
        }
        ColumnView::Float(v) => {
            ArrayRef::from(arrow::array::Float32Array::from(v.to_vec()).into_data())
        }
        ColumnView::Double(v) => {
            ArrayRef::from(arrow::array::Float64Array::from(v.to_vec()).into_data())
        }
        ColumnView::VarChar(v) => {
            ArrayRef::from(arrow::array::StringArray::from_iter(v.to_vec().iter()).into_data())
        }
        ColumnView::Timestamp(v) => ArrayRef::from(
            arrow::array::Int64Array::from_iter(
                v.to_vec().iter().map(|ts| ts.map(|ts| ts.as_raw_i64())),
            )
            .into_data(),
        ),
        ColumnView::NChar(v) => {
            ArrayRef::from(arrow::array::StringArray::from_iter(v.to_vec().iter()).into_data())
        }
        ColumnView::UTinyInt(v) => {
            ArrayRef::from(arrow::array::UInt8Array::from(v.to_vec()).into_data())
        }
        ColumnView::USmallInt(v) => {
            ArrayRef::from(arrow::array::UInt16Array::from(v.to_vec()).into_data())
        }
        ColumnView::UInt(v) => {
            ArrayRef::from(arrow::array::UInt32Array::from(v.to_vec()).into_data())
        }
        ColumnView::UBigInt(v) => {
            ArrayRef::from(arrow::array::UInt64Array::from(v.to_vec()).into_data())
        }
        ColumnView::Json(v) => {
            ArrayRef::from(arrow::array::StringArray::from_iter(v.to_vec().iter()).into_data())
        }
    }
}

pub async fn query_to_parquet(mut from: Dsn, to: Dsn) -> Result<()> {
    let sql = from.params.remove("query").unwrap();
    let taos = TaosBuilder::from_dsn(from)?.build()?;
    let mut rs = taos.query(&sql).await?;

    let names = rs
        .filed_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect_vec();
    log::info!("sql: {sql}, fields: {}", rs.num_of_fields());

    let file = to.fragment.expect("csv file not found");
    let schema = Arc::new(fields_to_arrow(rs.fields(), rs.precision()));
    let props = WriterProperties::builder().build();
    let file = std::fs::File::create(&file).unwrap();
    use parquet::arrow::arrow_writer::ArrowWriter;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();

    let mut rows = rs.blocks();

    while let Some(row) = rows.try_next().await? {
        let columns = row.columns();
        let batch = RecordBatch::try_from_iter(names.iter().zip(columns.map(column_to_arrow)))?;
        writer.write(&batch)?;
    }
    writer.close().unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test() -> Result<()> {
    let from = Dsn::from_str("taos:///test?query=select * from test.d0")?;
    let to = Dsn::from_str("local:./test.parquet")?;

    query_to_parquet(from, to).await?;
    Ok(())
}
