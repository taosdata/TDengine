use std::sync::Arc;

use anyhow::Result;

use arrow::{
    array::ArrayRef,
    datatypes::{Schema, TimeUnit},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use taos::{
    AsyncFetchable, AsyncQueryable, AsyncTBuilder, ColumnView, Dsn, Field, Itertools, Precision,
    TaosBuilder, Ty,
};

use parquet::{
    arrow::arrow_writer::ArrowWriter, basic::ZstdLevel, file::properties::WriterProperties,
};

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
            .iter()
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

fn column_to_arrow(column: &ColumnView) -> Result<ArrayRef> {
    let array: ArrayRef = match column {
        ColumnView::Bool(v) => Arc::new(arrow::array::BooleanArray::from_iter(v.iter())),
        ColumnView::TinyInt(v) => Arc::new(arrow::array::Int8Array::from_iter(v.iter())),
        ColumnView::SmallInt(v) => Arc::new(arrow::array::Int16Array::from_iter(v.iter())),
        ColumnView::Int(v) => Arc::new(arrow::array::Int32Array::from_iter(v.iter())),
        ColumnView::BigInt(v) => Arc::new(arrow::array::Int64Array::from_iter(v.iter())),
        ColumnView::Float(v) => Arc::new(arrow::array::Float32Array::from_iter(v.iter())),
        ColumnView::Double(v) => Arc::new(arrow::array::Float64Array::from_iter(v.iter())),
        ColumnView::VarChar(v) => Arc::new(arrow::array::BinaryArray::from_iter(v.iter_as_bytes())),
        ColumnView::Timestamp(v) => {
            let iter = v
                .to_vec()
                .into_iter()
                .map(|ts| ts.map(|ts| ts.as_raw_i64()));
            match v.precision() {
                Precision::Millisecond => {
                    Arc::new(arrow::array::TimestampMillisecondArray::from_iter(iter))
                }
                Precision::Microsecond => {
                    Arc::new(arrow::array::TimestampMicrosecondArray::from_iter(iter))
                }
                Precision::Nanosecond => {
                    Arc::new(arrow::array::TimestampNanosecondArray::from_iter(iter))
                }
            }
        }
        ColumnView::NChar(v) => Arc::new(arrow::array::StringArray::from_iter(v.to_vec().iter())),
        ColumnView::UTinyInt(v) => Arc::new(arrow::array::UInt8Array::from_iter(v.iter())),
        ColumnView::USmallInt(v) => Arc::new(arrow::array::UInt16Array::from_iter(v.iter())),
        ColumnView::UInt(v) => Arc::new(arrow::array::UInt32Array::from_iter(v.iter())),
        ColumnView::UBigInt(v) => Arc::new(arrow::array::UInt64Array::from_iter(v.iter())),
        ColumnView::Json(v) => Arc::new(arrow::array::StringArray::from_iter(v.to_vec().iter())),
        ColumnView::VarBinary(v) => Arc::new(arrow::array::BinaryArray::from_iter(v.iter())),
        ColumnView::Geometry(v) => Arc::new(arrow::array::BinaryArray::from_iter(v.iter())),
        ColumnView::Blob(v) => Arc::new(arrow::array::BinaryArray::from_iter(v.iter())),
        ColumnView::Decimal(v) => {
            let (precision, scale) = v.precision_and_scale();
            Arc::new(
                arrow::array::Decimal128Array::from_iter(v.iter().map(|v| v.map(|v| v.data())))
                    .with_precision_and_scale(precision as _, scale as _)?,
            )
        }
        ColumnView::Decimal64(v) => {
            let (precision, scale) = v.precision_and_scale();
            Arc::new(
                arrow::array::Decimal128Array::from_iter(
                    v.iter().map(|v| v.map(|v| v.data() as i128)),
                )
                .with_precision_and_scale(precision as _, scale as _)?,
            )
        }
    };
    Ok(array)
}

pub async fn query_to_parquet(mut from: Dsn, to: Dsn) -> Result<()> {
    let force = true; // FIXME
    let sql = from.params.remove("query").unwrap();
    let builder = TaosBuilder::from_dsn(from)?;
    let taos = builder.build().await?;

    let mut rs = taos.query(&sql).await?;

    tracing::info!("sql: {sql}, fields: {}", rs.num_of_fields());

    let filename = to.path.expect("parquet file must be input");
    if std::path::Path::new(&filename).exists() && !force {
        anyhow::bail!("Parquet file {} exists, please check or use `-y`", filename);
    }

    let schema = Arc::new(fields_to_arrow(rs.fields(), rs.precision()));
    tracing::debug!("schema: {}", &schema);
    let schema_ref = schema.clone();
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
        .build();
    let file = std::fs::File::create(&filename).unwrap();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();

    let mut blocks = rs.blocks();

    // let mut rows = 0;
    while let Some(row) = blocks.try_next().await? {
        let columns = row.columns();
        let batch = RecordBatch::try_new(
            schema_ref.clone(),
            columns
                .map(column_to_arrow)
                .collect::<Result<Vec<_>, _>>()?,
        )?;
        writer.write(&batch)?;
        // rows += row.nrows();
    }
    writer.close().unwrap();

    let (blocks, rows) = rs.summary();

    tracing::info!(
        "write {rows} rows(in {blocks} blocks) to parquet: {}",
        filename
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test() -> Result<()> {
    use std::str::FromStr;
    let db = "parquet_test";
    let from = Dsn::from_str(&format!("taos:///?query=select * from {db}.stb1"))?;
    let to = Dsn::from_str("local:./test.parquet")?;

    let client = TaosBuilder::from_dsn(&from)?.build().await?;

    assert_eq!(
        client
            .exec_many([
                format!("drop database if exists {db}"),
                format!("create database {db} keep 36500"),
                format!("use {db}"),
            ])
            .await?,
        0
    );
    assert_eq!(
            client.exec(
                "create table stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16)) tags (jt json)".to_string()
            ).await?,
            0
        );
    assert_eq!(
        client
            .exec(
                r#"insert into tb1 using stb1 tags('{"key":"数据"}')
                   values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                    .to_string()
            )
            .await?,
        2
    );
    assert_eq!(
        client
            .exec(
                r#"insert into tb2 using stb1 tags(NULL)
                   values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                    .to_string()
            )
            .await?,
        2
    );

    query_to_parquet(from, to.clone()).await?;

    std::fs::remove_file(to.path.unwrap())?;

    client.exec(format!("drop database {db}")).await?;
    Ok(())
}
