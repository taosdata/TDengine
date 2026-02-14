use std::{collections::HashMap, sync::Arc};

use actix_files::NamedFile;
use actix_web::{
    HttpRequest, Responder, ResponseError,
    web::{self, Data, Json, Path, Query},
};
use anyhow::{Context, bail};
use chrono::{Local, Utc};
use ha_core::{
    activity::TaskStatus,
    consts::{TASK_ACTIVITIES_STABLE, TASK_METRICS_STABLE},
    types::{HaTask, MetricsType},
};
use http::StatusCode;
use taos::Dsn;
use taosx_core::core_metrics::{CoreMetrics, get_task_metrics_string};
use taosx_utils::{
    labels::{LabelFilter, build_json_labels_from_string},
    sql::sql_value_escaped_fmt,
};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::instrument;

use super::{get_dsn, types::*};
use crate::{
    Args,
    sql::{exec, query, query_one},
    x_api::{JsonResult, JsonStatusResult, Result},
};

pub async fn get_tasks(
    args: web::Data<Args>,
    req: HttpRequest,
    param: Query<GetTaskParam>,
) -> JsonResult<Vec<GetTaskResult>> {
    let param = param.into_inner();

    let query_labels = param
        .labels
        .as_ref()
        .and_then(|v| {
            build_json_labels_from_string(v)
                .as_object()
                .and_then(|v| v.get("type"))
                .cloned()
        })
        .map(|v| LabelFilter::new().with("type", v));

    let dsn = get_dsn(&args, &req).await?;
    let tasks = query::<TaskRecord>(&dsn, "SHOW XNODE TASKS").await?;
    let mut res = Vec::with_capacity(tasks.len());
    for task in tasks {
        let Some(query_labels) = query_labels.as_ref() else {
            res.push(task.try_into()?);
            continue;
        };

        // When a label filter is provided, only keep tasks whose labels match.
        let Some(task_labels_str) = task.labels.as_ref() else {
            continue;
        };
        let task_labels =
            serde_json::from_str(task_labels_str).context("task labels invalid json")?;
        if !query_labels.matches(&task_labels) {
            continue;
        }
        res.push(task.try_into()?);
    }
    Ok(Json(res))
}

pub async fn get_task(
    args: web::Data<Args>,
    task_id: Path<i64>,
    req: HttpRequest,
) -> JsonResult<GetTaskResult> {
    let task_id = task_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    let sql = format!("SHOW XNODE TASKS WHERE ID = {task_id}");
    let data = query_one::<TaskRecord>(&dsn, &sql).await?;
    Ok(Json(
        data.map(|v| v.try_into())
            .transpose()?
            .context("task {} not found")?,
    ))
}

#[instrument(skip_all)]
pub async fn create_task(
    args: web::Data<Args>,
    Json(task): Json<Task>,
    req: HttpRequest,
) -> JsonStatusResult<GetTaskResult> {
    Ok((
        Json(create_task_inner(&args, &req, task, true).await?),
        StatusCode::CREATED,
    ))
}

#[instrument(skip_all)]
async fn create_task_inner(
    args: &Args,
    req: &HttpRequest,
    task: Task,
    start: bool,
) -> Result<GetTaskResult> {
    let (from, to) = task.extract_from_to()?;
    let backup_topic = match (from.driver.as_str(), to.driver.as_str()) {
        ("tmq", "local") => Some(tmq_to_local::conf::BackupConfig::group_id(
            Utc::now().timestamp_millis(),
            &from,
            &to,
        )),
        _ => None,
    };
    // create
    let task_name = task.name.clone();
    let config: HaTask = task.try_into()?;
    let status = TaskStatus::Created;
    let mut sql = format!(
        "CREATE XNODE TASK '{}' FROM '{}' TO '{}' WITH STATUS '{status}'",
        task_name, config.from, config.to
    );

    if let Some(parser) = config
        .parser
        .as_ref()
        .map(serde_json::to_string::<serde_json::Value>)
        .transpose()
        .context("invalid `parser` param")?
    {
        sql.push_str(&format!(" PARSER {} ", sql_value_escaped_fmt(&parser)));
    }

    if let Some(via) = config.via {
        sql.push_str(&format!(" VIA {via}"));
    }

    let labels = match (config.labels, backup_topic) {
        (Some(labels), Some(oneshot_topic)) => {
            let mut labels = labels.clone();
            if let Some(map) = labels.as_object_mut() {
                map.insert("oneshot_topic".to_string(), oneshot_topic.into());
            }
            Some(labels)
        }
        (Some(labels), None) => Some(labels),
        (None, Some(oneshot_topic)) => Some(serde_json::json!({"oneshot_topic": oneshot_topic})),
        (None, None) => None,
    };
    if let Some(labels) = labels
        .as_ref()
        .map(serde_json::to_string::<serde_json::Value>)
        .transpose()
        .context("invalid `labels` param")?
    {
        sql.push_str(&format!(" LABELS {}", sql_value_escaped_fmt(&labels)));
    }

    tracing::debug!(sql, "create task sql");
    let dsn = get_dsn(args, req).await?;
    exec(&dsn, &sql).await?;

    // start
    if start {
        let sql = format!("START XNODE TASK '{task_name}'");
        exec(&dsn, &sql).await?;
    }

    let sql = format!("SHOW XNODE TASKS WHERE NAME = '{task_name}'");
    let task = query_one::<TaskRecord>(&dsn, &sql)
        .await?
        .with_context(|| format!("task {task_name} not found"))?;

    Ok(task.try_into()?)
}

pub async fn update_task(
    args: web::Data<Args>,
    task_id: Path<i64>,
    Json(task): Json<Task>,
    req: HttpRequest,
) -> JsonResult<()> {
    // update
    let task_id = task_id.into_inner();
    let config: HaTask = task.try_into()?;
    let mut sql = format!(
        "ALTER XNODE TASK {} FROM '{}' TO '{}'",
        task_id, config.from, config.to
    );
    if config.parser.is_some() || config.via.is_some() {
        sql.push_str(" WITH");
    }
    if let Some(parser) = config
        .parser
        .as_ref()
        .map(serde_json::to_string::<serde_json::Value>)
        .transpose()
        .context("invalid `parser` param")?
    {
        sql.push_str(&format!(" PARSER '{}'", parser));
    }

    if let Some(via) = config.via {
        sql.push_str(&format!(" VIA '{}'", via));
    }

    let dsn = get_dsn(&args, &req).await?;
    exec(&dsn, &sql).await?;

    // start
    let sql = format!("START XNODE TASK {}", task_id);
    exec(&dsn, &sql).await?;
    Ok(Json(()))
}

pub async fn delete_task(
    args: web::Data<Args>,
    task_id: Path<i64>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_id = task_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    let sql = format!("DROP XNODE TASK {}", task_id);
    exec(&dsn, &sql).await?;
    Ok(Json(()))
}

pub async fn start_task(
    args: web::Data<Args>,
    task_id: Path<i64>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_id = task_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    let sql = format!("START XNODE TASK {}", task_id);
    exec(&dsn, &sql).await?;
    Ok(Json(()))
}

pub async fn stop_task(
    args: web::Data<Args>,
    task_id: Path<i64>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_id = task_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    let sql = format!("STOP XNODE TASK {}", task_id);
    exec(&dsn, &sql).await?;
    Ok(Json(()))
}

pub async fn batch_start_tasks(
    args: web::Data<Args>,
    task_ids: web::Json<Vec<i64>>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_ids = task_ids.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    for task_id in task_ids {
        let sql = format!("START XNODE TASK {}", task_id);
        exec(&dsn, &sql).await?;
    }
    Ok(Json(()))
}

pub async fn batch_stop_tasks(
    args: web::Data<Args>,
    task_ids: web::Json<Vec<i64>>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_ids = task_ids.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    for task_id in task_ids {
        let sql = format!("STOP XNODE TASK {}", task_id);
        exec(&dsn, &sql).await?;
    }
    Ok(Json(()))
}

pub async fn batch_delete_tasks(
    args: web::Data<Args>,
    task_ids: web::Json<Vec<i64>>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_ids = task_ids.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    for task_id in task_ids {
        let sql = format!("DROP XNODE TASK {}", task_id);
        exec(&dsn, &sql).await?;
    }
    Ok(Json(()))
}

pub async fn export_task(
    args: web::Data<Args>,
    ids: Query<ExportTaskParam>,
    req: HttpRequest,
) -> impl Responder {
    match export_task_inner(&args, &req, &ids).await {
        Ok(file) => file.into_response(&req),
        Err(e) => e.error_response(),
    }
}

async fn export_task_inner(
    args: &Args,
    req: &HttpRequest,
    ids: &ExportTaskParam,
) -> Result<NamedFile> {
    let dsn = get_dsn(args, req).await?;
    let mut tasks = query::<TaskRecord>(&dsn, "SHOW XNODE TASKS")
        .await?
        .into_iter()
        .map(|v| (v.id, v))
        .collect::<HashMap<_, _>>();

    let ids = ids.ids()?;
    let mut exported = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(task) = tasks.remove(&id) {
            exported.push(task.try_into()?);
        }
    }
    let now = Local::now();
    let res = ExportTaskResult {
        tasks_num: exported.len(),
        export_time: now.to_rfc3339(),
        tasks: exported,
    };
    let content = serde_json::to_string_pretty(&res).context("serialize export content error")?;
    let file_name = format!(
        "/tmp/taos-explorer-tasks-{}.json",
        now.format("%Y%m%d%H%M%S")
    );
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&file_name)
        .await
        .context("create export file error")?;
    file.write_all(content.as_bytes())
        .await
        .context("write export file error")?;
    file.flush().await.context("flush export file error")?;

    Ok(NamedFile::open(file_name).context("open export named file error")?)
}

pub async fn import_task(
    args: web::Data<Args>,
    params: Json<ExportTaskResult>,
    req: HttpRequest,
) -> JsonResult<()> {
    for task in params.into_inner().tasks {
        create_task_inner(&args, &req, task.into(), false).await?;
    }
    Ok(Json(()))
}

pub async fn get_task_metrics(
    args: Data<Args>,
    task_id: Path<i64>,
    req: HttpRequest,
) -> Result<String> {
    let task_id = task_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    Ok(get_all_task_job_metrics(dsn, task_id).await?)
}

pub async fn get_task_activities(
    args: Data<Args>,
    task_id: Path<i64>,
    req: HttpRequest,
) -> JsonResult<Vec<ActivityLog>> {
    let task_id = task_id.into_inner();

    let dsn = get_dsn(&args, &req).await?;
    let sql = format!(
        "select \
        `task_id` as `id`, `ts` as `at`, `level`, `status`, `activity` \
        from log.{TASK_ACTIVITIES_STABLE} \
        where task_id = {task_id} and status != '-' \
        order by ts desc limit 10;"
    );
    let activities = query::<ActivityLog>(&dsn, &sql).await?;
    Ok(Json(activities))
}

pub async fn get_all_task_job_metrics(dsn: Dsn, task_id: i64) -> anyhow::Result<String> {
    let sql = format!(
        "select last_row(`type`) as `type`, last_row(`value`) as `value` \
        from log.{TASK_METRICS_STABLE} where task_id = {task_id} partition by tbname"
    );

    #[derive(Debug, serde::Deserialize)]
    struct DBMetrics {
        r#type: String,
        value: String,
    }

    let metrics = query::<DBMetrics>(&dsn, &sql).await?;
    let mut result = None;
    for item in metrics {
        let item = match MetricsType::from_str_opt(&item.r#type) {
            Some(ty) => match ty {
                MetricsType::Ipc => CoreMetrics::IPC(
                    serde_json::from_str(&item.value).context("invalid ipc metrics")?,
                ),
                MetricsType::Tmq => CoreMetrics::TMQ(
                    serde_json::from_str(&item.value).context("invalid tmq metrics")?,
                ),
                MetricsType::Legacy => CoreMetrics::Legacy(
                    serde_json::from_str(&item.value).context("invalid legacy metrics")?,
                ),
            },
            None => bail!("invalid metrics type {}", item.r#type),
        };
        let Some(res) = result.as_mut() else {
            result = Some(item);
            continue;
        };

        *res += item;
    }

    let sql = format!("SHOW XNODE TASKS WHERE ID = {task_id}");
    let task = query_one::<TaskRecord>(&dsn, &sql)
        .await?
        .context("task not found")?;

    let Some(result) = result else {
        return Ok("{}".into());
    };
    let res = get_task_metrics_string(task.status.unwrap_or(TaskStatus::Created), Arc::new(result))
        .context("serialize metrics to string error")?;
    Ok(res)
}
