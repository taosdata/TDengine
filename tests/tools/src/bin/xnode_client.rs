use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow_flight::{encode::FlightDataEncoderBuilder, FlightClient};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use clap::Parser;
use futures::TryStreamExt;
use tonic::transport::Channel;

#[derive(Debug, Clone, clap::Parser)]
struct Args {
    #[arg(short = 'a', long, default_value = "http://localhost:6055")]
    addr: String,
    #[arg(short = 'f', long)]
    payload_file: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let payloads = tokio::fs::read_to_string(&args.payload_file)
        .await
        .context("read payload file error")?;
    let payloads = serde_json::from_str::<Vec<HashMap<String, serde_json::Value>>>(&payloads)
        .context("payload invalid json")?;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("action", DataType::Utf8, false),
        Field::new("context", DataType::Utf8, false),
        Field::new("req_id", DataType::UInt64, false),
    ]));

    let uri = args.addr.parse().context("invalid address")?;
    let channel = Channel::builder(uri)
        .connect()
        .await
        .context("connect grpc server error")?;
    let mut client = FlightClient::new(channel);

    for mut payload in payloads {
        let enabled = payload
            .remove("enabled")
            .is_some_and(|v| v.as_bool().is_some_and(|v| v));
        if !enabled {
            continue;
        }
        for (action, context) in payload {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                        chrono::Utc::now().timestamp_millis(),
                    ])),
                    Arc::new(arrow::array::StringArray::from(vec![action])),
                    Arc::new(arrow::array::StringArray::from(vec![context.to_string()])),
                    Arc::new(arrow::array::UInt64Array::from(vec![0u64])),
                ],
            )?;
            let data = FlightDataEncoderBuilder::new()
                .with_schema(schema.clone())
                .build(futures::stream::iter(vec![Ok(batch)]));
            let res = client
                .do_exchange(data)
                .await
                .context("do exchange error")?
                .try_collect::<Vec<_>>()
                .await
                .context("collect doexchange error")?;
            for batch in res {
                arrow::util::pretty::print_batches(&[batch])
                    .context("print response recordbatch error")?;
            }
        }
    }

    Ok(())
}
