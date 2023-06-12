use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

use arrow::{
    array::{
        make_builder, Float32Builder, StringBuilder, StructBuilder, TimestampMillisecondBuilder,
        UInt8Array, Int32Builder,
    },
    datatypes::{DataType, Field, Fields, Schema},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};

use taosx_ipc::{prelude::*, stream::components::ListOfStructBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(not(target_os = "windows"))]
    let stream = std::os::unix::net::UnixStream::connect("./taosx.sock")?;
    #[cfg(target_os = "windows")]
    let stream = std::net::TcpStream::connect("127.0.0.1:6051")?;
    // let timestamp_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);

    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("point"));
    metadata.insert(String::from("ack"), String::from("none"));
    let opc_columns = vec![
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("id", ArrowDataType::Utf8, false),
        Field::new("value", ArrowDataType::Utf8, false),
        Field::new("received", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("status", ArrowDataType::Int32, false),
    ];
    // let opc_columns = Fields::from_iter(&opc_columns);
    let fields = Fields::from((&opc_columns).clone());
    let record = DataType::Struct(fields);
    let schema = Schema::new(opc_columns).with_metadata(metadata);

    let mut writer = StreamWriter::try_new(&stream, &schema)?;
    // let schema = Arc::new(schema);
    // let insert_schema = Arc::new(schema.project(&indices)?);
    let mut records = 0;
    let now = chrono::Utc::now();
    let mut ms = now.timestamp_millis() - 10000;

    loop {
        let mut timestamp_builder = TimestampMillisecondBuilder::new();
        timestamp_builder.append_value(ms + 1000);
        let timestamp = timestamp_builder.finish();
        let mut id_builder = StringBuilder::new();
        id_builder.append_value("1");
        let id = id_builder.finish();
        let mut value_builder = StringBuilder::new();
        value_builder.append_value("12312311111111111111");
        let value = value_builder.finish();
        let mut received_builder = TimestampMillisecondBuilder::new();
        received_builder.append_value(ms + 1000);
        let received = received_builder.finish();
        let mut name_builder = StringBuilder::new();
        name_builder.append_value("ddddd0---name-------");
        let name = name_builder.finish();

        let mut status_builder = Int32Builder::new();
        status_builder.append_value(123);
        let status = status_builder.finish();
        // dbg!(&timestamp);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(timestamp),
                Arc::new(id),
                Arc::new(value),
                Arc::new(received),
                Arc::new(name),
                Arc::new(status),
            ],
        )?;
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
