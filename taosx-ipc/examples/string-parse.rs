use anyhow::Result;

use arrow::{
    array::{StringBuilder, TimestampMillisecondBuilder},
    datatypes::DataType,
    ipc::writer::StreamWriter,
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
            "meters",
            vec![
                IpcField::new("ts", false, timestamp_type, IpcDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond)),
                IpcField::new("c1", false, ArrowDataType::Utf8, IpcDataType::Int32),
                IpcField::new("c2", false, ArrowDataType::Utf8, IpcDataType::Float64),
                IpcField::new("c3", false, ArrowDataType::Utf8, IpcDataType::VarChar(100)),
                IpcField::new(
                    "__table_name__",
                    false,
                    ArrowDataType::Utf8,
                    IpcDataType::VarChar(100),
                ),
            ],
            vec![
                IpcField::new("t1", false, ArrowDataType::Utf8, IpcDataType::Bool),
                IpcField::new("t2", false, ArrowDataType::Utf8, IpcDataType::Int8),
                IpcField::new("t3", false, ArrowDataType::Utf8, IpcDataType::Int16),
                IpcField::new("t4", false, ArrowDataType::Utf8, IpcDataType::Int32),
                IpcField::new("t5", false, ArrowDataType::Utf8, IpcDataType::Int64),
                IpcField::new("t6", false, ArrowDataType::Utf8, IpcDataType::UInt8),
                IpcField::new("t7", false, ArrowDataType::Utf8, IpcDataType::UInt16),
                IpcField::new("t8", false, ArrowDataType::Utf8, IpcDataType::UInt32),
                IpcField::new("t9", false, ArrowDataType::Utf8, IpcDataType::UInt64),
                IpcField::new("t10", false, ArrowDataType::Utf8, IpcDataType::Float32),
                IpcField::new("t11", false, ArrowDataType::Utf8, IpcDataType::Float64),
                IpcField::new("t12", true, ArrowDataType::Utf8, IpcDataType::VarChar(10)),
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
        .append(&"true")
        .append(&"1")
        .append(&"2")
        .append(&"3")
        .append(&"4")
        .append(&"5")
        .append(&"6")
        .append(&"7")
        .append(&"8")
        .append(&"9")
        .append(&"10")
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

        let builder = insert.columns_builder();

        builder.append(true);

        builder
            .field_builder::<TimestampMillisecondBuilder>(0)
            .unwrap()
            .append_value(ms);

        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("100");
        builder
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_value("100");
        builder
            .field_builder::<StringBuilder>(3)
            .unwrap()
            .append_value("中文");
        builder
            .field_builder::<StringBuilder>(4)
            .unwrap()
            .append_value("d1001");

        let builder = insert.columns_builder();

        builder.append(true);

        builder
            .field_builder::<TimestampMillisecondBuilder>(0)
            .unwrap()
            .append_value(ms + 1000);

        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("100");
        builder
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_value("101");
        builder
            .field_builder::<StringBuilder>(3)
            .unwrap()
            .append_value("中文0");
        builder
            .field_builder::<StringBuilder>(4)
            .unwrap()
            .append_value("fake01");

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
