use std::sync::Arc;

use anyhow::Result;

use arrow::{
    array::{Array, ArrayRef},
    datatypes::{Schema, TimeUnit},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use taos::{
    AsyncFetchable, AsyncQueryable, ColumnView, Dsn, Field, Itertools, Precision, TBuilder,
    TaosBuilder, Ty,
};

use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
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
                Ty::VarChar => arrow::datatypes::Field::new(f.name(), DataType::Binary, true),
                Ty::Timestamp => arrow::datatypes::Field::new(
                    f.name(),
                    DataType::Timestamp(precision_to_arrow(precision), None),
                    // DataType::Int64,
                    true,
                ),
                Ty::NChar => arrow::datatypes::Field::new(f.name(), DataType::Utf8, true),
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
            ArrayRef::from(arrow::array::BooleanArray::from_iter(v.iter()).into_data())
        }
        ColumnView::TinyInt(v) => {
            ArrayRef::from(arrow::array::Int8Array::from_iter(v.iter()).into_data())
        }
        ColumnView::SmallInt(v) => {
            ArrayRef::from(arrow::array::Int16Array::from_iter(v.iter()).into_data())
        }
        ColumnView::Int(v) => {
            ArrayRef::from(arrow::array::Int32Array::from_iter(v.iter()).into_data())
        }
        ColumnView::BigInt(v) => {
            ArrayRef::from(arrow::array::Int64Array::from_iter(v.iter()).into_data())
        }
        ColumnView::Float(v) => {
            ArrayRef::from(arrow::array::Float32Array::from_iter(v.iter()).into_data())
        }
        ColumnView::Double(v) => {
            ArrayRef::from(arrow::array::Float64Array::from_iter(v.iter()).into_data())
        }
        ColumnView::VarChar(v) => {
            ArrayRef::from(arrow::array::BinaryArray::from_iter(v.iter_as_bytes()).into_data())
        }
        ColumnView::Timestamp(v) => {
            let iter = v
                .to_vec()
                .into_iter()
                .map(|ts| ts.map(|ts| ts.as_raw_i64()));
            match v.precision() {
                Precision::Millisecond => ArrayRef::from(
                    arrow::array::TimestampMillisecondArray::from_iter(iter).into_data(),
                ),
                Precision::Microsecond => ArrayRef::from(
                    arrow::array::TimestampMicrosecondArray::from_iter(iter).into_data(),
                ),
                Precision::Nanosecond => ArrayRef::from(
                    arrow::array::TimestampNanosecondArray::from_iter(iter).into_data(),
                ),
            }
        }
        ColumnView::NChar(v) => {
            ArrayRef::from(arrow::array::StringArray::from_iter(v.to_vec().iter()).into_data())
        }
        ColumnView::UTinyInt(v) => {
            ArrayRef::from(arrow::array::UInt8Array::from_iter(v.iter()).into_data())
        }
        ColumnView::USmallInt(v) => {
            ArrayRef::from(arrow::array::UInt16Array::from_iter(v.iter()).into_data())
        }
        ColumnView::UInt(v) => {
            ArrayRef::from(arrow::array::UInt32Array::from_iter(v.iter()).into_data())
        }
        ColumnView::UBigInt(v) => {
            ArrayRef::from(arrow::array::UInt64Array::from_iter(v.iter()).into_data())
        }
        ColumnView::Json(v) => {
            ArrayRef::from(arrow::array::StringArray::from_iter(v.to_vec().iter()).into_data())
        }
    }
}

pub async fn query_to_parquet(mut from: Dsn, to: Dsn, force: bool) -> Result<()> {
    let sql = from.params.remove("query").unwrap();
    let taos = TaosBuilder::from_dsn(from)?.build()?;
    let mut rs = taos.query(&sql).await?;

    log::info!("sql: {sql}, fields: {}", rs.num_of_fields());

    let file = to.fragment.expect("parquet file must be input");
    if std::path::Path::new(&file).exists() && !force {
        anyhow::bail!("Parquet file {} exists, please check or use `-y`", file);
    }

    let schema = Arc::new(fields_to_arrow(rs.fields(), rs.precision()));
    log::debug!("schema: {}", &schema);
    let schema_ref = schema.clone();
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD)
        .build();
    let file = std::fs::File::create(&file).unwrap();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();

    let mut rows = rs.blocks();

    while let Some(row) = rows.try_next().await? {
        let columns = row.columns();
        let batch = RecordBatch::try_new(
            schema_ref.clone(),
            columns.map(column_to_arrow).collect_vec(),
        )?;
        writer.write(&batch)?;
    }
    writer.close().unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test() -> Result<()> {
    use std::str::FromStr;
    let from = Dsn::from_str("taos:///?query=select * from test.stb1")?;
    let to = Dsn::from_str("local:./test.parquet")?;

    let client = TaosBuilder::from_dsn(&from)?.build()?;

    let db = "test";
    assert_eq!(
        client.exec(format!("drop database if exists {db}")).await?,
        0
    );
    assert_eq!(
        client
            .exec(format!("create database {db} keep 36500"))
            .await?,
        0
    );
    assert_eq!(
            client.exec(
                format!("create table {db}.stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16)) tags (jt json)")
            ).await?,
            0
        );
    assert_eq!(
        client
            .exec(format!(
                r#"insert into {db}.tb1 using {db}.stb1 tags('{{"key":"数据"}}')
                   values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
            ))
            .await?,
        2
    );
    assert_eq!(
        client
            .exec(format!(
                r#"insert into {db}.tb2 using {db}.stb1 tags(NULL)
                   values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
            ))
            .await?,
        2
    );

    query_to_parquet(from, to.clone(), true).await?;

    std::fs::remove_file(&to.fragment.unwrap())?;
    Ok(())
}
