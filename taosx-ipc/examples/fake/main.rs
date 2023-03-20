use std::any::Any;

use anyhow::Result;

use arrow::{
    array::{BinaryBuilder, Float64Builder, Int32Builder, TimestampMillisecondBuilder},
    datatypes::DataType,
    ipc::writer::StreamWriter,
};

use taosx_ipc::writer::*;

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(not(target_os = "windows"))]
    let stream = std::os::unix::net::UnixStream::connect("./taosx.sock")?;
    #[cfg(target_os = "windows")]
    let stream = std::net::TcpStream::connect("127.0.0.1:6051")?;
    let timestamp_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);

    let lush_builder = LushMessageBuilder::new()
        .with_stable(
            "meters",
            vec![
                IpcField::new("ts", false, timestamp_type, IpcDataType::Timestamp),
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
                IpcField::new(
                    "t12",
                    false,
                    ArrowDataType::Binary,
                    IpcDataType::VarChar(10),
                ),
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

    loop {
        let mut insert = lush_builder.insert_builder();
        insert
            .table("tb1")
            .using("meters")
            .with_tag(Box::new(true) as Box<dyn Any>)
            .with_tag(Box::new(1i8) as Box<dyn Any>)
            .with_tag(Box::new(1i16) as Box<dyn Any>)
            .with_tag(Box::new(1i32) as Box<dyn Any>)
            .with_tag(Box::new(1i64) as Box<dyn Any>)
            .with_tag(Box::new(1u8) as Box<dyn Any>)
            .with_tag(Box::new(1u16) as Box<dyn Any>)
            .with_tag(Box::new(1u32) as Box<dyn Any>)
            .with_tag(Box::new(1u64) as Box<dyn Any>)
            .with_tag(Box::new(1.0f32) as Box<dyn Any>)
            .with_tag(Box::new(1.0f64) as Box<dyn Any>)
            .with_tag(Box::new(&"abc") as Box<dyn Any>);

        let builder = insert.columns_builder();

        builder.append(true);

        builder
            .field_builder::<TimestampMillisecondBuilder>(0)
            .unwrap()
            .append_value(ms);

        builder
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(100i32);
        builder
            .field_builder::<Float64Builder>(2)
            .unwrap()
            .append_value(100.);
        builder
            .field_builder::<BinaryBuilder>(3)
            .unwrap()
            .append_value("中文".as_bytes());
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
            .append_value(101.);
        builder
            .field_builder::<BinaryBuilder>(3)
            .unwrap()
            .append_value("中文0".as_bytes());
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
            .append_value(102.);
        builder
            .field_builder::<BinaryBuilder>(3)
            .unwrap()
            .append_value("中文2".as_bytes());
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
            .append_value(103.);
        builder
            .field_builder::<BinaryBuilder>(3)
            .unwrap()
            .append_value("中文3".as_bytes());
        builder
            .field_builder::<BinaryBuilder>(4)
            .unwrap()
            .append_value("d1003".as_bytes());

        let batch = insert.build()?;
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
