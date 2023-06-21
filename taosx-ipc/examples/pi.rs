use std::time::Duration;

use anyhow::Result;

use arrow::{
    array::{BinaryBuilder, Float64Builder, Int32Builder, TimestampMillisecondBuilder},
    datatypes::DataType,
    ipc::writer::StreamWriter,
};

use taosx_ipc::prelude::*;

async fn process_tcp_stream(id: usize) -> Result<()> {
    let stream = std::net::TcpStream::connect("127.0.0.1:6051")?;
    let timestamp_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);

    let lush_builder = LushMessageBuilder::new()
        .with_stable(
            "meters",
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
        .next_table(&format!("d{id}001"))
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
        .fill_nulls_to_end()
        .next_table(&format!("d{id}002"))
        .append(&true)
        .append(&1i8)
        .append(&2i16)
        .append(&3i32)
        .append(&4i64)
        .fill_nulls_to_end()
        .next_table(&format!("d{id}003"))
        .fill_nulls_to_end()
        .finish()?;
    // dbg!(&tables);
    writer.write(&tables)?;

    loop {
        let mut insert = lush_builder.insert_builder();

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
            .append_value(format!("d{id}001").as_bytes());

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
            .append_value(format!("d{id}002").as_bytes());

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
            .append_value(format!("d{id}003").as_bytes());

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
            .append_value(format!("d{id}003").as_bytes());

        let batch = insert.build()?;
        dbg!(&batch);
        writer.write(&batch)?;

        records += batch.num_rows();
        println!("written {} record batch", records);
        if records >= 1 {
            // break;
        }
        std::thread::sleep(Duration::from_secs(1));
        ms += 1;
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
async fn main() -> Result<()> {
    let mut handles = vec![];
    for i in 0..4 {
        handles.push(tokio::spawn(process_tcp_stream(i)));
    }
    for h in handles {
        h.await?.unwrap();
    }
    panic!("expect run forever");
}
