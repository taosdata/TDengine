use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

use arrow::{
    array::{
        make_builder, BinaryBuilder, Float32Builder, Int32Builder, StringBuilder, StructArray,
        StructBuilder, TimestampMillisecondBuilder, UInt8Array,
    },
    datatypes::{DataType, Field, Schema},
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
        Field::new("value", ArrowDataType::Float32, false),
    ];
    let record = DataType::Struct(opc_columns.clone());
    let record_list = DataType::List(Box::new(Field::new("item", record.clone(), true)));
    let schema = Schema::new(vec![
        Field::new("__type__", DataType::UInt8, false),
        // Field::new_dict(
        //     "__tables__",
        //     DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
        //     true,
        //     LushMessageType::Children as u8 as _,
        //     false,
        // ),
        // Field::new_dict(
        //     "__attrs__",
        //     DataType::Utf8,
        //     true,
        //     0,
        //     false,
        // ),
        Field::new_dict("__records__", record_list, true, 3, false),
    ])
    .with_metadata(metadata);

    let mut writer = StreamWriter::try_new(&stream, &schema)?;
    // let schema = Arc::new(schema);
    // let insert_schema = Arc::new(schema.project(&indices)?);
    let mut records = 0;
    let now = chrono::Utc::now();
    let mut ms = now.timestamp_millis() - 10000;

    loop {
        let mut list_struct_builder = ListOfStructBuilder::new(opc_columns.clone(), 3);
        let builder = list_struct_builder.values();
        builder.append(true);
        builder
            .field_builder::<TimestampMillisecondBuilder>(0)
            .unwrap()
            .append_value(ms + 1000);

        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("1");
        builder
            .field_builder::<Float32Builder>(2)
            .unwrap()
            .append_value(101.);
        builder.append(true);
        builder
            .field_builder::<TimestampMillisecondBuilder>(0)
            .unwrap()
            .append_value(ms + 1000);

        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("3");
        builder
            .field_builder::<Float32Builder>(2)
            .unwrap()
            .append_value(101.);

        let list = list_struct_builder.finish();
        let attrs = StructBuilder::new(
            opc_columns.clone(),
            opc_columns
                .iter()
                .map(|f| make_builder(f.data_type(), 3))
                .collect(),
        )
        .finish();

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(UInt8Array::from(vec![3 as u8])), // __type
                // Arc::new(StructArray::from(vec![])),                   // __tables__
                // Arc::new(StructArray::from(vec![])),                   // __tables__
                Arc::new(list),
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
