use std::{
    str::FromStr,
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_schema::{DataType, Field, TimeUnit};
use taos::Dsn;
use taosx_core::ha;
use tonic::{Code, Status};

fn build_batch(action: &str, context: &str, req_id: u64) -> Result<RecordBatch, FlightError> {
    static SCHEMA: LazyLock<Arc<arrow_schema::Schema>> = LazyLock::new(|| {
        Arc::new(arrow_schema::Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("action", DataType::Utf8, false),
            Field::new("context", DataType::Utf8, false),
            Field::new("req_id", DataType::UInt64, false),
        ]))
    });
    let schema = SCHEMA.clone();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                chrono::Utc::now().timestamp_millis(),
            ])),
            Arc::new(arrow::array::StringArray::from(vec![action])),
            Arc::new(arrow::array::StringArray::from(vec![context])),
            Arc::new(arrow::array::UInt64Array::from(vec![req_id])),
        ],
    )
    .map_err(FlightError::Arrow)
}

pub async fn xnode_plan_task(context: &str, req_id: u64) -> Result<RecordBatch, FlightError> {
    let task = serde_json::from_str::<ha::HaTask>(context).map_err(|e| {
        FlightError::DecodeError(format!("deserialize xnode_plan_task body error: {e:#}"))
    })?;
    let from = Dsn::from_str(&task.from)
        .map_err(|e| FlightError::DecodeError(format!("parse from dsn error: {e:#}")))?;
    let to = Dsn::from_str(&task.to)
        .map_err(|e| FlightError::DecodeError(format!("parse from dsn error: {e:#}")))?;
    let from_driver = from.driver.as_str();
    let to_driver = to.driver.as_str();
    let split_task = ha::SplitJobTask::new(from.clone(), to.clone(), task.parser);
    let status_err =
        |e: anyhow::Error| FlightError::Tonic(Status::new(Code::Internal, format!("{e:#}")).into());
    match (from_driver, to_driver) {
        ("tmq" | "sync", "taos")
        | ("tmq" | "sync", "local")
        | ("local", "taos" | "tmq")
        | ("taos", "taos")
        | ("taos", "csv")
        | ("taos", "parquet")
        | ("pi" | "pibackfill", "taos")
        | ("opc" | "opcda" | "opcua", "taos")
        | ("tmq", "mqtt")
        | ("sparkplugb", "taos")
        | ("influxdb", "taos")
        | ("opentsdb", "taos")
        | ("csv", "taos")
        | ("tmq", "kafka")
        | ("avevaHistorian", "taos")
        | ("orc", "taos")
        | ("mongodb", _)
        | ("mysql", _)
        | ("postgres", _)
        | ("oracle", _)
        | ("mssql", _) => {
            return build_batch("xnode_plan_task_resp", context, req_id);
        }
        _ => {}
    }
    let task_value = match (from_driver, to_driver) {
        ("kafka", _) => source_kafka::split_job::split_job(split_task)
            .await
            .context("kafka split job error")
            .map_err(status_err)?,
        ("mqtt", _) => source_mqtt::split_job::split_job(split_task)
            .await
            .context("mqtt split job error")
            .map_err(status_err)?,
        _ => {
            return Err(FlightError::DecodeError(format!(
                "unsupported split job from `{from_driver}` to `{to_driver}`"
            )));
        }
    };
    let context = serde_json::to_string(&task_value)
        .context("serialize xnode plan task response error")
        .map_err(status_err)?;

    build_batch("xnode_plan_task_resp", &context, req_id)
}
