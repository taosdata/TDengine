use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;

use arrow::{
    array::{
        make_builder, BinaryBuilder, PrimitiveBuilder, StringBuilder, StructBuilder,
        TimestampMillisecondBuilder, UInt8Array, UInt8Builder,
    },
    datatypes::{DataType, Field, Schema},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};

use taosx_ipc::{prelude::*, stream::components::ListOfStructBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    // #[cfg(not(target_os = "windows"))]
    // let stream = std::os::unix::net::UnixStream::connect("./taosx.sock")?;
    // #[cfg(target_os = "windows")]
    let stream = std::net::TcpStream::connect("127.0.0.1:6051")?;
    // let timestamp_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);

    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("flat"));
    metadata.insert(String::from("ack"), String::from("none"));
    let flat_columns = vec![
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("topic", ArrowDataType::Utf8, false),
        Field::new("qos", ArrowDataType::UInt8, false),
        Field::new("payload", ArrowDataType::Binary, false),
    ];
    let record_list = DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);

    let schema = Schema::new(flat_columns).with_metadata(metadata);
    // let schema = Schema::new(vec![
    // Field::new("__type__", DataType::UInt8, false),
    // Field::new_dict("__records__", record_list, true, 4, false),
    // ])
    // .with_metadata(metadata);

    let mut writer = StreamWriter::try_new(&stream, &schema)?;
    let mut records = 0;
    let now = chrono::Utc::now();
    let mut ms = now.timestamp_millis() - 10000;

    loop {
        // let mut list_struct_builder = ListOfStructBuilder::new(flat_columns.clone(), 4);
        // let field_builders = flat_columns
        //     .iter()
        //     .map(|f| make_builder(f.data_type(), 4))
        //     .collect();
        // let mut builder = StructBuilder::new(flat_columns.clone(), field_builders);
        // let builder = list_struct_builder.values();
        // let mut builder = TimestampMillisecondBuilder::new();

        let mut timestamp_builder = TimestampMillisecondBuilder::new();
        timestamp_builder.append_value(ms + 1000);
        let timestamp = timestamp_builder.finish();
        let mut topic_builder = StringBuilder::new();
        topic_builder.append_value("topic1");
        let topic = topic_builder.finish();
        let mut qos_builder = UInt8Builder::new();
        qos_builder.append_value(1);
        let qos = qos_builder.finish();
        let mut binary_builder = BinaryBuilder::new();
        binary_builder.append_value(r#"{"ts": 1681699204689, "pre": 123.4}"#);
        let payload = binary_builder.finish();
        // dbg!(&timestamp);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(timestamp),
                Arc::new(topic),
                Arc::new(qos),
                Arc::new(payload),
            ],
        )?;
        // dbg!(&batch);

        writer.write(&batch)?;

        records += batch.num_rows();
        println!("written {} record batch", records);
        tokio::time::sleep(Duration::from_secs(1)).await;
        // if records >= 1 {
        //     break;
        // }
        ms += 1;
    }
    Ok(())
}
