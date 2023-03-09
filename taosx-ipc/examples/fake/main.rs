use std::{any::Any};

use anyhow::Result;

use arrow::{
    array::{ Int32Builder, TimestampMillisecondBuilder, },
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
                IpcField::new("value", false, ArrowDataType::Int32, IpcDataType::Int32),
            ],
            vec![IpcField::new(
                "id",
                false,
                ArrowDataType::Int32,
                IpcDataType::Int32,
            )],
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
            .with_tag(Box::new(1i32) as Box<dyn Any>);

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

        let batch = insert.build()?;
        // dbg!(&batch);
        // writer.write(batch)
        writer.write(&batch)?;

        records += batch.num_rows();
        println!("written {} rows", records);
        if records >= 1 {
            break;
        }
        ms += 1;
    }
    Ok(())
}
