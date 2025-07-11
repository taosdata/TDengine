use crate::serve::controller::TaskControllerRef;
use crate::serve::task::Failed;
use actix_web::web::{Data, Path};
use actix_web::{HttpResponse, Responder, post};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::Serialize;
use source_kafka::TopicOffsetInfo;
use std::fs::File;
use std::io::Write;
use taos::IntoDsn;
use taosx_core::get_data_dir;

#[utoipa::path(
    tag = "kafka",
    responses(
        (status = 200, description = "Task started successfully"),
        (status = 404, description = "Task not found by id", body = Failed),
        (status = 500, description = "Server error", body = Failed),
    ),
    params(
        ("id", description = "Unique storage id of Task")
    ),
)]
#[post("/kafka/{id}/seek_to_end")]
pub async fn seek_to_end(
    task_id: Path<i64>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let id = task_id.into_inner();
    match seek_to_end_impl(id, task_store).await {
        Ok(Some(_)) => Ok(HttpResponse::Ok().json("{}")),
        Ok(None) => Ok(HttpResponse::NotFound().finish()),
        Err(err) => {
            tracing::error!("failed to seek kafka consumers to end: {:#}", err);
            Err(Failed::from_error(err))
        }
    }
}

async fn seek_to_end_impl(
    task_id: i64,
    task_store: Data<TaskControllerRef>,
) -> anyhow::Result<Option<()>> {
    // 从数据库中获取 kafka 任务的 DSN
    let task = task_store.get(task_id).await?;
    if task.is_none() {
        return Ok(None);
    }
    let mut task = task.unwrap();

    let mut from = task.from.as_str().into_dsn().context("invalid from")?;
    // 从 Kafka 获取当前任务相关的 Offset
    let offsets = source_kafka::get_topics_offset(Some(task_id), &from)
        .await
        .context("failed to get topics offset")?;

    // 将 Offset 持久化到 data_dir
    let now = Utc::now();
    let offset_range = OffsetRange {
        task_id,
        created: now,
        offsets,
    };
    let data_dir = get_data_dir();
    let offset_file = data_dir
        .join("tasks")
        .join(format!("{}", task_id))
        .join(format!("kafka-offset-{}.toml", now.to_rfc3339()));
    let mut config_file = File::create(&offset_file)?;
    let toml = toml::to_string(&offset_range).context("failed to serialize offset range")?;
    config_file.write_all(toml.as_bytes())?;

    // 为 kafka 任务的 DSN 追加参数 seek_to_end=true
    from.params
        .insert("seek_to_end".to_string(), "true".to_string());
    task.from = from.to_string();

    // 启动 kafka 任务
    task_store.start_task(&task).await?;

    Ok(Some(()))
}

#[derive(Debug, Serialize)]
struct OffsetRange {
    task_id: i64,
    created: DateTime<Utc>,
    offsets: Vec<TopicOffsetInfo>,
}
