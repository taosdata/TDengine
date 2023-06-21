use anyhow::Result;

use arrow::{
    array::{BinaryBuilder, Float64Builder, Int32Builder, TimestampMillisecondBuilder},
    datatypes::DataType,
    ipc::{writer::StreamWriter, Null, NullBuilder},
};

use taosx_ipc::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(not(target_os = "windows"))]
    let stream = std::os::unix::net::UnixStream::connect("./taosx.sock")?;
    #[cfg(target_os = "windows")]
    let stream = std::net::TcpStream::connect("127.0.0.1:6051")?;
    let timestamp_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);

    let lush_builder = LushMessageBuilder::new()
        .with_stable(
            "meters---",
            vec![
                IpcField::new("ts", false, timestamp_type, IpcDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond)),
                IpcField::new("c1", false, ArrowDataType::Int32, IpcDataType::Int32),
                IpcField::new("c2", false, ArrowDataType::Float64, IpcDataType::Float64),
                IpcField::new(
                    "c3",
                    false,
                    ArrowDataType::Binary,
                    IpcDataType::VarChar(100),
                ),
                IpcField::new(
                    "__table_name__",
                    false,
                    ArrowDataType::Binary,
                    IpcDataType::VarChar(100),
                ),
            ],
            vec![
                IpcField::new("t1", false, ArrowDataType::Boolean, IpcDataType::Bool),
                IpcField::new("t2", false, ArrowDataType::Int8, IpcDataType::Int8),
                IpcField::new("t3", false, ArrowDataType::Int16, IpcDataType::Int16),
                IpcField::new("t4", false, ArrowDataType::Int32, IpcDataType::Int32),
                IpcField::new("t5", false, ArrowDataType::Int64, IpcDataType::Int64),
                IpcField::new("t6", false, ArrowDataType::UInt8, IpcDataType::UInt8),
                IpcField::new("t7", false, ArrowDataType::UInt16, IpcDataType::UInt16),
                IpcField::new("t8", false, ArrowDataType::UInt32, IpcDataType::UInt32),
                IpcField::new("t9", false, ArrowDataType::UInt64, IpcDataType::UInt64),
                IpcField::new("t10", false, ArrowDataType::Float32, IpcDataType::Float32),
                IpcField::new("t11", false, ArrowDataType::Float64, IpcDataType::Float64),
                IpcField::new("t12", true, ArrowDataType::Binary, IpcDataType::VarChar(10)),
            ],
        )
        .build();

    let schema = lush_builder.schema_ref();

    let mut writer = StreamWriter::try_new(&stream, &schema)?;
    // let schema = Arc::new(schema);
    // let insert_schema = Arc::new(schema.project(&indices)?);
    // dbg!(&insert_schema);
    let mut records = 0;
    let now = chrono::Utc::now();
    let mut ms = now.timestamp_millis() - 10000;

    let mut tables = lush_builder.child_tables_builder();

    let tables = tables
        .next_table("fake01")
        .append(&true)
        .append(&1i8)
        .append(&2i16)
        .append(&3i32)
        .append(&4i64)
        .append(&5u8)
        .append(&6u16)
        .append(&7u32)
        .append(&8u64)
        .append(&9f32)
        .append(&10f64)
        .append(&"I'm fake01")
        .next_table("d1001")
        .fill_nulls_to_end()
        .next_table("d1002")
        .fill_nulls_to_end()
        .next_table("d1003")
        .fill_nulls_to_end()
        .finish()?;
    // dbg!(&tables);
    writer.write(&tables)?;

    loop {
        let mut insert = lush_builder.insert_builder();

        // insert
        //     .table("tb1")
        //     .with_tag(&true)
        //     .with_tag(&1i8)
        //     .with_tag(&2i16)
        //     .with_tag(&3i32)
        //     .with_tag(&4i64)
        //     .with_tag(&5u8)
        //     .with_tag(&6u16)
        //     .with_tag(&7u32)
        //     .with_tag(&8u64)
        //     .with_tag(&9f32)
        //     .with_tag(&10f64)
        //     .with_tag(&"abc");

        let builder = insert.columns_builder();

        builder.append(true);

        builder
            .field_builder::<TimestampMillisecondBuilder>(0)
            .unwrap()
            .append_value(ms);

        builder
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_null();
        // .append_value(Null);
        builder
            .field_builder::<Float64Builder>(2)
            .unwrap()
            .append_value(100.1);
        builder
            .field_builder::<BinaryBuilder>(3)
            .unwrap()
            .append_value("中文0".as_bytes());
        builder
            .field_builder::<BinaryBuilder>(4)
            .unwrap()
            .append_value("d1001".as_bytes());

        let builder = insert.columns_builder();

        builder.append(true);

        builder
            .field_builder::<TimestampMillisecondBuilder>(0)
            .unwrap()
            .append_value(ms + 1000);

        builder
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(101i32);
        builder
            .field_builder::<Float64Builder>(2)
            .unwrap()
            .append_null();
        // .append_value(101.);
        builder
            .field_builder::<BinaryBuilder>(3)
            .unwrap()
            .append_value("中文1".as_bytes());
        builder
            .field_builder::<BinaryBuilder>(4)
            .unwrap()
            .append_value("d1002".as_bytes());

        let builder = insert.columns_builder();

        builder.append(true);

        builder
            .field_builder::<TimestampMillisecondBuilder>(0)
            .unwrap()
            .append_value(ms + 2000);

        builder
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(102i32);
        builder
            .field_builder::<Float64Builder>(2)
            .unwrap()
            .append_value(102.3);
        builder
            .field_builder::<BinaryBuilder>(3)
            .unwrap()
            .append_null();
        // .append_value("中文2".as_bytes());
        builder
            .field_builder::<BinaryBuilder>(4)
            .unwrap()
            .append_value("d1001".as_bytes());

        let builder = insert.columns_builder();

        builder.append(true);

        builder
            .field_builder::<TimestampMillisecondBuilder>(0)
            .unwrap()
            .append_value(ms + 3000);

        builder
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(103i32);
        builder
            .field_builder::<Float64Builder>(2)
            .unwrap()
            .append_value(103.4);
        builder
            .field_builder::<BinaryBuilder>(3)
            .unwrap()
            .append_value("中文3".as_bytes());
        builder
            .field_builder::<BinaryBuilder>(4)
            .unwrap()
            .append_value("d1003".as_bytes());

        let batch = insert.build()?;
        dbg!(&batch);
        writer.write(&batch)?;

        records += batch.num_rows();
        println!("written {} record batch", records);
        if records >= 1 {
            break;
        }
        ms += 1;
    }
    Ok(())
}
