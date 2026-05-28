use std::{
    collections::{HashMap, HashSet},
    io::{Seek, Write},
    path::{Path as StdPath, PathBuf},
    str::FromStr,
    sync::Arc,
};

use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError,
    body::SizedStream,
    http::header::{self, ContentDisposition, DispositionParam, DispositionType},
    web::{self, Data, Json, Path, Query},
};
use anyhow::{Context, anyhow, bail};
use chrono::{Local, Utc};
use ha_core::{
    activity::TaskStatus,
    consts::{TASK_ACTIVITIES_STABLE, TASK_METRICS_STABLE},
    types::{HaTask, MetricsType},
};
use http::StatusCode;
use taos::Dsn;
use taosx_core::{
    core_metrics::{CoreMetrics, get_task_metrics_string},
    get_file_upload_home_dir,
    tmq::tmq_metric::TmqMetrics,
};
use taosx_utils::{
    labels::{LabelFilter, build_json_labels_from_string},
    sql::sql_value_escaped_fmt,
};
use tokio::task::spawn_blocking;
use tokio_util::io::ReaderStream;
use tracing::instrument;

use super::{get_dsn, types::*};
use crate::{
    Args,
    oauth::{self, session::SessionManager},
    sql::{exec, query, query_one},
    x_api::{Error, JsonResult, JsonStatusResult, Result},
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
) -> JsonStatusResult<Option<GetTaskResult>> {
    let task_id = task_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    let sql = format!("SHOW XNODE TASKS WHERE ID = {task_id}");
    let data = query_one::<TaskRecord>(&dsn, &sql).await?;
    match data {
        Some(data) => Ok((Json(Some(data.try_into()?)), StatusCode::OK)),
        None => Ok((Json(None), StatusCode::NOT_FOUND)),
    }
}

#[instrument(skip_all)]
pub async fn create_task(
    args: web::Data<Args>,
    session_manager: web::Data<SessionManager>,
    Json(task): Json<Task>,
    req: HttpRequest,
) -> JsonStatusResult<GetTaskResult> {
    Ok((
        Json(create_task_inner(&args, &session_manager, &req, task, true).await?),
        StatusCode::CREATED,
    ))
}

#[instrument(skip_all)]
async fn create_task_inner(
    args: &Args,
    session_manager: &SessionManager,
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
    let mut config: HaTask = task.try_into()?;

    // Inject bearer_token into `to` DSN if user has a taosx token and none is already set.
    // This allows tasks to authenticate via token even when TOTP is enabled.
    if let Some(username) = extract_username_from_request(req).await
        && let Ok(Some(token_value)) = session_manager.get_taosx_token(&username).await
        && let Ok(mut to_dsn) = Dsn::from_str(&config.to)
        && to_dsn.get("bearer_token").is_none()
    {
        to_dsn.set("bearer_token", &token_value);
        config.to = to_dsn.to_string();
        tracing::debug!(
            "Injected bearer_token into task `to` DSN for user: {}",
            username
        );
    }

    // Fallback: convert `__token__`-prefixed password to `bearer_token` param.
    // This handles the case where the user logged in via token and the frontend
    // sent `__token__<real_token>` as a literal password in the `to` DSN.
    if let Some(fixed) = fix_token_password_in_dsn(&config.to) {
        config.to = fixed;
    }

    let status = TaskStatus::Created;
    let mut sql = format!(
        "CREATE XNODE TASK '{}' FROM '{}' TO '{}' WITH STATUS '{status}'",
        task_name, config.from, config.to
    );

    if let Some(parser) = config
        .parser
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize parser")?
    {
        sql.push_str(&format!(" PARSER {} ", sql_value_escaped_fmt(&parser)));
    }

    if let Some(via) = config.via {
        sql.push_str(&format!(" VIA {via}"));
    }

    let labels = match (config.labels, backup_topic) {
        (Some(mut labels), Some(oneshot_topic)) => {
            if let Some(map) = labels.as_object_mut() {
                map.insert("oneshot_topic".to_string(), oneshot_topic.into());
            }
            labels
        }
        (Some(labels), None) => labels,
        (None, Some(oneshot_topic)) => serde_json::json!({"oneshot_topic": oneshot_topic}),
        (None, None) => serde_json::json!({"type": "datain"}),
    };
    let labels = serde_json::to_string(&labels).context("invalid `labels` param")?;
    sql.push_str(&format!(" LABELS {}", sql_value_escaped_fmt(&labels)));
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
    session_manager: web::Data<SessionManager>,
    task_id: Path<i64>,
    Json(task): Json<Task>,
    req: HttpRequest,
) -> JsonResult<()> {
    // update
    let task_id = task_id.into_inner();
    let mut config: HaTask = task.try_into()?;

    // Inject bearer_token into `to` DSN if user has a taosx token and none is already set.
    if let Some(username) = extract_username_from_request(&req).await
        && let Ok(Some(token_value)) = session_manager.get_taosx_token(&username).await
        && let Ok(mut to_dsn) = Dsn::from_str(&config.to)
        && to_dsn.get("bearer_token").is_none()
    {
        to_dsn.set("bearer_token", &token_value);
        config.to = to_dsn.to_string();
        tracing::debug!(
            "Injected bearer_token into updated task `to` DSN for user: {}",
            username
        );
    }

    // Fallback: convert `__token__`-prefixed password to `bearer_token` param.
    if let Some(fixed) = fix_token_password_in_dsn(&config.to) {
        config.to = fixed;
    }

    let dsn = get_dsn(&args, &req).await?;

    let sql = format!("SHOW XNODE TASKS WHERE ID = {task_id}");
    let current_task = query_one::<TaskRecord>(&dsn, &sql)
        .await?
        .ok_or_else(|| Error::not_found(format!("task {task_id} not found")))?;

    let sql = build_update_task_sql(task_id, &current_task.name, &config)?;
    exec(&dsn, &sql).await?;

    // start
    let sql = format!("START XNODE TASK {}", task_id);
    exec(&dsn, &sql).await?;
    Ok(Json(()))
}

fn build_update_task_sql(
    task_id: i64,
    current_name: &str,
    config: &HaTask,
) -> anyhow::Result<String> {
    let mut sql = format!(
        "ALTER XNODE TASK {} FROM {} TO {}",
        task_id,
        sql_value_escaped_fmt(&config.from),
        sql_value_escaped_fmt(&config.to)
    );

    let name_changed = config.name != current_name;
    if name_changed || config.labels.is_some() || config.parser.is_some() || config.via.is_some() {
        sql.push_str(" WITH");
    }

    if name_changed {
        sql.push_str(&format!(" NAME {}", sql_value_escaped_fmt(&config.name)));
    }

    if let Some(labels) = config
        .labels
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize labels")?
    {
        sql.push_str(&format!(" LABELS {}", sql_value_escaped_fmt(&labels)));
    }

    if let Some(parser) = config
        .parser
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize parser")?
    {
        sql.push_str(&format!(" PARSER {}", sql_value_escaped_fmt(&parser)));
    }

    if let Some(via) = config.via {
        sql.push_str(&format!(" VIA {}", via));
    }

    Ok(sql)
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
    session_manager: web::Data<SessionManager>,
    task_id: Path<i64>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_id = task_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    inject_bearer_token_if_needed(&dsn, &session_manager, &req, task_id).await?;
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
    session_manager: web::Data<SessionManager>,
    task_ids: web::Json<BatchOpsParam>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_ids = task_ids.into_inner().ids;
    let dsn = get_dsn(&args, &req).await?;
    for task_id in task_ids {
        inject_bearer_token_if_needed(&dsn, &session_manager, &req, task_id).await?;
        let sql = format!("START XNODE TASK {}", task_id);
        exec(&dsn, &sql).await?;
    }
    Ok(Json(()))
}

pub async fn batch_stop_tasks(
    args: web::Data<Args>,
    task_ids: web::Json<BatchOpsParam>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_ids = task_ids.into_inner().ids;
    let dsn = get_dsn(&args, &req).await?;
    for task_id in task_ids {
        let sql = format!("STOP XNODE TASK {}", task_id);
        exec(&dsn, &sql).await?;
    }
    Ok(Json(()))
}

pub async fn batch_delete_tasks(
    args: web::Data<Args>,
    task_ids: web::Json<BatchOpsParam>,
    req: HttpRequest,
) -> JsonResult<()> {
    let task_ids = task_ids.into_inner().ids;
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
        Ok(response) => response,
        Err(e) => e.error_response(),
    }
}

async fn export_task_inner(
    args: &Args,
    req: &HttpRequest,
    ids: &ExportTaskParam,
) -> Result<HttpResponse> {
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
    let mut res = ExportTaskResult {
        tasks_num: exported.len(),
        export_time: now.to_rfc3339(),
        tasks: exported,
    };
    let timestamp = now.format("%Y%m%d%H%M%S");
    let upload_dirs = resolve_upload_dirs(args.data_dir.as_deref().map(StdPath::new));
    let file_refs = collect_file_refs_for_export(
        res.tasks.iter().map(|task| task.from.clone()).collect(),
        upload_dirs,
    )
    .await
    .context("collect export file refs error")?;

    if file_refs.is_empty() {
        // No referenced files: return JSON as before (backward compatible).
        let content = serde_json::to_vec_pretty(&res).context("serialize export content error")?;
        build_download_response(
            write_temp_file(content)
                .await
                .context("create export json file error")?,
            format!("taos-explorer-tasks-{timestamp}.json"),
            "application/json",
        )
    } else {
        // Has referenced files: rewrite paths to relative and bundle into a ZIP.
        rewrite_paths_to_relative_with_refs(&mut res, &file_refs);
        let content = serde_json::to_vec_pretty(&res).context("serialize export content error")?;
        let file = build_export_zip(content, file_refs)
            .await
            .context("build export zip error")?;
        build_download_response(
            file,
            format!("taos-explorer-tasks-{timestamp}.zip"),
            "application/zip",
        )
    }
}

pub async fn import_task(
    args: web::Data<Args>,
    session_manager: web::Data<SessionManager>,
    params: Json<ExportTaskResult>,
    req: HttpRequest,
) -> JsonResult<()> {
    let exported_task = params.into_inner();
    for task in exported_task.tasks {
        create_task_inner(&args, &session_manager, &req, task.try_into()?, false).await?;
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

/// Aggregated vgroup progress entry for one (topic, vgroup) pair.
#[derive(Debug, serde::Serialize)]
struct VgroupProgressEntry {
    topic: String,
    vgroup: i32,
    offset: i64,
    latest: i64,
}

/// Aggregation result from parsed TmqMetrics rows.
struct AggregatedProgress {
    /// Per-(topic, vgroup) max(offset, latest) pairs, ordered by key.
    entries: std::collections::BTreeMap<(String, i32), (i64, i64)>,
    /// Maximum row timestamp in milliseconds.
    update_time: Option<i64>,
}

/// Aggregate progress from parsed `(TmqMetrics, row_ts_ms)` pairs.
///
/// - Entries with an empty topic are skipped.
/// - For duplicate `(topic, vgroup)` keys the maximum offset and maximum latest are kept.
/// - `update_time` is the maximum row timestamp in milliseconds.
fn aggregate_tmq_progress(rows: impl IntoIterator<Item = (TmqMetrics, i64)>) -> AggregatedProgress {
    let mut entries: std::collections::BTreeMap<(String, i32), (i64, i64)> =
        std::collections::BTreeMap::new();
    let mut update_time: Option<i64> = None;

    for (metrics, ts_ms) in rows {
        update_time = Some(match update_time {
            Some(prev) => prev.max(ts_ms),
            None => ts_ms,
        });

        let snapshot = metrics
            .progress_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for item in snapshot.iter() {
            if item.topic.is_empty() {
                continue;
            }
            let key = (item.topic.clone(), item.vgroup);
            let (cur_offset, cur_latest) = entries.entry(key).or_insert((item.offset, item.latest));
            *cur_offset = (*cur_offset).max(item.offset);
            *cur_latest = (*cur_latest).max(item.latest);
        }
    }

    AggregatedProgress {
        entries,
        update_time,
    }
}

pub async fn get_task_vgroup_progress(
    args: Data<Args>,
    task_id: Path<i64>,
    req: HttpRequest,
) -> Result<String> {
    let task_id = task_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;

    // Verify the task exists.
    let sql = format!("SHOW XNODE TASKS WHERE ID = {task_id}");
    let _task = query_one::<TaskRecord>(&dsn, &sql)
        .await?
        .ok_or_else(|| Error::not_found(format!("task {task_id} not found")))?;

    // Query the latest tmq metrics rows, one per sub-table.
    #[derive(Debug, serde::Deserialize)]
    struct TmqMetricsRow {
        value: String,
        ts: chrono::DateTime<chrono::Utc>,
    }

    let metrics_sql = format!(
        "select last_row(`value`) as `value`, last_row(`ts`) as `ts` \
        from log.{TASK_METRICS_STABLE} where task_id = {task_id} and type = 'tmq' \
        partition by tbname"
    );
    let rows = query::<TmqMetricsRow>(&dsn, &metrics_sql).await?;

    if rows.is_empty() {
        let result = serde_json::json!({
            "update_time": serde_json::Value::Null,
            "data": serde_json::Value::Array(vec![]),
        });
        return Ok(
            serde_json::to_string(&result).context("failed to serialize empty vgroup progress")?
        );
    }

    let total_rows = rows.len();
    let mut parsed: Vec<(TmqMetrics, i64)> = Vec::with_capacity(total_rows);
    let mut malformed: usize = 0;

    for row in rows {
        let ts_ms = row.ts.timestamp_millis();
        match serde_json::from_str::<TmqMetrics>(&row.value) {
            Ok(m) => parsed.push((m, ts_ms)),
            Err(e) => {
                malformed += 1;
                tracing::warn!(
                    task_id,
                    error = %e,
                    "skipping malformed tmq metrics row"
                );
            }
        }
    }

    if malformed == total_rows {
        return Err(anyhow::anyhow!(
            "all {malformed} tmq metrics rows for task {task_id} were malformed"
        )
        .into());
    }

    let agg = aggregate_tmq_progress(parsed);
    let data: Vec<VgroupProgressEntry> = agg
        .entries
        .into_iter()
        .map(|((topic, vgroup), (offset, latest))| VgroupProgressEntry {
            topic,
            vgroup,
            offset,
            latest,
        })
        .collect();

    let result = serde_json::json!({
        "update_time": agg.update_time,
        "data": data,
    });
    Ok(serde_json::to_string(&result).context("failed to serialize vgroup progress")?)
}

#[derive(Debug, serde::Deserialize)]
pub struct TableProgressQuery {
    pub table: String,
    pub start: Option<String>,
    pub end: Option<String>,
}

pub async fn get_task_table_progress(
    args: Data<Args>,
    task_id: Path<i64>,
    query: Query<TableProgressQuery>,
    req: HttpRequest,
) -> Result<String> {
    let task_id = task_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;

    // Verify the task exists and retrieve its DSNs.
    let sql = format!("SHOW XNODE TASKS WHERE ID = {task_id}");
    let task = query_one::<TaskRecord>(&dsn, &sql)
        .await?
        .ok_or_else(|| Error::not_found(format!("task {task_id} not found")))?;

    let q = query.into_inner();
    let progress = tmq_to_td::get_table_progress(
        &task.from,
        &task.to,
        &q.table,
        q.start.as_ref(),
        q.end.as_ref(),
    )
    .await
    .map_err(|e| anyhow::anyhow!(e))?;

    Ok(serde_json::to_string(&progress).context("failed to serialize table progress")?)
}

async fn extract_username_from_request(req: &HttpRequest) -> Option<String> {
    match oauth::middleware::extract_auth_from_request(req).await {
        Ok(Some(auth)) => Some(auth.username),
        _ => None,
    }
}

/// Convert `__token__`-prefixed passwords in a DSN to proper `bearer_token` params.
///
/// When a user logs in via token, the frontend stores `__token__<real_token>` as the
/// password in localStorage. The `to` DSN built by the frontend thus contains this
/// prefixed value as a literal password, which TDengine cannot authenticate.
/// This function detects the prefix, strips it, and sets `bearer_token` instead.
fn fix_token_password_in_dsn(to: &str) -> Option<String> {
    let mut dsn = Dsn::from_str(to).ok()?;
    let token = dsn
        .password
        .as_deref()
        .and_then(|p| p.strip_prefix("__token__"))
        .map(|t| t.to_string())?;
    dsn.set("bearer_token", &token);
    dsn.password = None;
    Some(dsn.to_string())
}

// ─── Export file-bundling helpers ─────────────────────────────────────────────

/// A single uploaded-file reference discovered inside exported task JSON.
struct FileRef {
    /// Original `@...` reference as it appeared in exported JSON.
    source_ref: String,
    /// Absolute path of the file on disk.
    abs_path: PathBuf,
    /// Relative path to use inside the ZIP archive (e.g. `files/1234/config.csv`).
    rel_path: String,
}

fn normalize_zip_relative_path(path: &std::path::Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

fn missing_referenced_uploaded_file_error(
    path: &std::path::Path,
    error: &std::io::Error,
) -> anyhow::Error {
    let sanitized_path = sanitized_uploaded_file_reference(path);
    tracing::warn!(
        path = %path.display(),
        sanitized_path,
        error = %error,
        "missing referenced uploaded file"
    );
    anyhow!(
        "missing referenced uploaded file {}: {error:#}",
        sanitized_path
    )
}

fn sanitized_uploaded_file_reference(path: &std::path::Path) -> String {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty());
    let req_id = path
        .parent()
        .and_then(|parent| parent.file_name())
        .map(|part| part.to_string_lossy().into_owned())
        .filter(|part| !part.is_empty());

    match (req_id, file_name) {
        (Some(req_id), Some(file_name)) => format!("files/{req_id}/{file_name}"),
        (_, Some(file_name)) => file_name,
        _ => "uploaded file".to_string(),
    }
}

fn resolve_upload_dir(data_dir: Option<&StdPath>) -> PathBuf {
    data_dir
        .map(|dir| dir.join("files"))
        .unwrap_or_else(get_file_upload_home_dir)
}

fn resolve_upload_dirs(data_dir: Option<&StdPath>) -> Vec<PathBuf> {
    let primary = resolve_upload_dir(data_dir);
    let taosx_upload_dir = get_file_upload_home_dir();
    let mut upload_dirs = vec![primary.clone()];
    if taosx_upload_dir != primary {
        upload_dirs.push(taosx_upload_dir);
    }
    upload_dirs
}

struct UploadDirRoot {
    path: PathBuf,
    canonical: Option<PathBuf>,
}

impl UploadDirRoot {
    fn new(path: PathBuf) -> Self {
        let canonical = std::fs::canonicalize(&path).ok();
        Self { path, canonical }
    }
}

/// Test helper that mirrors export-time file collection for assertions.
#[cfg(test)]
fn collect_file_refs_from_result(
    result: &ExportTaskResult,
    upload_dir: &std::path::Path,
) -> anyhow::Result<Vec<FileRef>> {
    collect_file_refs_from_values(
        result.tasks.iter().map(|task| &task.from),
        &[upload_dir.to_path_buf()],
    )
}

#[cfg(test)]
fn collect_file_refs_from_result_with_upload_dirs(
    result: &ExportTaskResult,
    upload_dirs: &[PathBuf],
) -> anyhow::Result<Vec<FileRef>> {
    collect_file_refs_from_values(result.tasks.iter().map(|task| &task.from), upload_dirs)
}

fn collect_file_refs_from_values<'a>(
    task_values: impl IntoIterator<Item = &'a serde_json::Value>,
    upload_dirs: &[PathBuf],
) -> anyhow::Result<Vec<FileRef>> {
    let upload_roots = upload_dirs
        .iter()
        .cloned()
        .map(UploadDirRoot::new)
        .collect::<Vec<_>>();
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut refs: Vec<FileRef> = Vec::new();
    for task_value in task_values {
        collect_file_refs_from_value(task_value, &upload_roots, &mut seen, &mut refs)?;
    }
    Ok(refs)
}

/// Collect uploaded-file references on a blocking thread so export does not
/// perform filesystem lookups on an async worker.
async fn collect_file_refs_for_export(
    task_values: Vec<serde_json::Value>,
    upload_dirs: Vec<PathBuf>,
) -> anyhow::Result<Vec<FileRef>> {
    spawn_blocking(move || collect_file_refs_from_values(task_values.iter(), &upload_dirs))
        .await
        .context("join export file collection task")?
}

/// Recursively scan a JSON value for uploaded-file references.
fn collect_file_refs_from_value(
    value: &serde_json::Value,
    upload_roots: &[UploadDirRoot],
    seen: &mut HashSet<PathBuf>,
    out: &mut Vec<FileRef>,
) -> anyhow::Result<()> {
    match value {
        serde_json::Value::String(s) => {
            // Values may be comma-separated (e.g. `csv_config_file` supports multiple files).
            for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                if let Some(path_str) = part.strip_prefix('@') {
                    let candidate = resolve_uploaded_candidate_path(path_str, upload_roots);
                    match std::fs::canonicalize(&candidate) {
                        Ok(canonical_abs) => {
                            if let Some(canonical_upload_dir) = upload_roots
                                .iter()
                                .filter_map(|root| root.canonical.as_deref())
                                .find(|canonical_upload_dir| {
                                    canonical_abs.starts_with(canonical_upload_dir)
                                })
                            {
                                if !canonical_abs.is_file() {
                                    bail!(
                                        "referenced uploaded path is not a file: {}",
                                        candidate.display()
                                    );
                                }
                                if seen.insert(canonical_abs.clone())
                                    && let Ok(rel) =
                                        canonical_abs.strip_prefix(canonical_upload_dir)
                                {
                                    let rel_path = normalize_zip_relative_path(rel);
                                    out.push(FileRef {
                                        source_ref: part.trim().to_string(),
                                        abs_path: canonical_abs,
                                        rel_path: format!("files/{rel_path}"),
                                    });
                                }
                            }
                        }
                        Err(error) => {
                            if upload_roots
                                .iter()
                                .any(|root| candidate.starts_with(&root.path))
                            {
                                return Err(missing_referenced_uploaded_file_error(
                                    &candidate, &error,
                                ));
                            }
                        }
                    }
                }
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values() {
                collect_file_refs_from_value(v, upload_roots, seen, out)?;
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_file_refs_from_value(v, upload_roots, seen, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_uploaded_candidate_path(path_str: &str, upload_roots: &[UploadDirRoot]) -> PathBuf {
    let candidate = std::path::Path::new(path_str);
    if candidate.is_absolute() {
        return candidate.to_path_buf();
    }
    let is_files_relative = candidate
        .components()
        .next()
        .is_some_and(|component| component.as_os_str() == "files");
    if is_files_relative {
        for upload_root in upload_roots {
            if let Some(data_dir) = upload_root.path.parent() {
                let resolved = data_dir.join(candidate);
                if resolved.exists() {
                    return resolved;
                }
            }
        }
        if let Some(data_dir) = upload_roots.first().and_then(|root| root.path.parent()) {
            return data_dir.join(candidate);
        }
    }
    candidate.to_path_buf()
}

#[cfg(test)]
fn rewrite_paths_to_relative(
    result: &mut ExportTaskResult,
    upload_dir: &std::path::Path,
) -> anyhow::Result<()> {
    let file_refs = collect_file_refs_from_result(result, upload_dir)?;
    rewrite_paths_to_relative_with_refs(result, &file_refs);
    Ok(())
}

fn rewrite_paths_to_relative_with_refs(result: &mut ExportTaskResult, file_refs: &[FileRef]) {
    let bundled_files = file_refs
        .iter()
        .map(|file_ref| {
            (
                file_ref.source_ref.clone(),
                format!("@{}", file_ref.rel_path),
            )
        })
        .collect::<HashMap<_, _>>();
    for task in &mut result.tasks {
        rewrite_value_paths_to_relative(&mut task.from, &bundled_files);
    }
}

fn rewrite_value_paths_to_relative(
    value: &mut serde_json::Value,
    bundled_files: &HashMap<String, String>,
) {
    match value {
        serde_json::Value::String(s) => {
            let rewritten: String = s
                .split(',')
                .map(|part| {
                    let trimmed = part.trim();
                    if let Some(rel_path) = bundled_files.get(trimmed) {
                        return rel_path.clone();
                    }
                    part.to_string()
                })
                .collect::<Vec<_>>()
                .join(",");
            *s = rewritten;
        }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                rewrite_value_paths_to_relative(v, bundled_files);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                rewrite_value_paths_to_relative(v, bundled_files);
            }
        }
        _ => {}
    }
}

async fn write_temp_file(content: Vec<u8>) -> anyhow::Result<std::fs::File> {
    spawn_blocking(move || -> anyhow::Result<std::fs::File> {
        let mut file = tempfile::tempfile().context("create anonymous export temp file")?;
        file.write_all(&content)
            .context("write export temp file content")?;
        file.flush().context("flush export temp file")?;
        file.rewind().context("rewind export temp file")?;
        Ok(file)
    })
    .await
    .context("join export temp file task")?
}

fn build_download_response(
    file: std::fs::File,
    download_name: String,
    content_type: &'static str,
) -> Result<HttpResponse> {
    let len = file.metadata().context("read export file metadata")?.len();
    let stream = ReaderStream::new(tokio::fs::File::from_std(file));
    Ok(HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, content_type))
        .insert_header(ContentDisposition {
            disposition: DispositionType::Attachment,
            parameters: vec![DispositionParam::Filename(download_name)],
        })
        .body(SizedStream::new(len, stream)))
}

/// Write a ZIP archive containing `tasks.json` plus all referenced files.
/// Export fails if any referenced uploaded file can no longer be opened.
async fn build_export_zip(
    tasks_json: Vec<u8>,
    file_refs: Vec<FileRef>,
) -> anyhow::Result<std::fs::File> {
    spawn_blocking(move || -> anyhow::Result<std::fs::File> {
        use std::io::copy;
        use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

        let file = tempfile::tempfile().context("create anonymous export zip file")?;
        let mut zip = ZipWriter::new(file);

        let json_opts =
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        zip.start_file("tasks.json", json_opts)
            .context("zip: add tasks.json")?;
        zip.write_all(&tasks_json)
            .context("zip: write tasks.json")?;

        let file_opts =
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        for file_ref in &file_refs {
            let mut source = std::fs::File::open(&file_ref.abs_path).map_err(|error| {
                missing_referenced_uploaded_file_error(&file_ref.abs_path, &error)
            })?;
            zip.start_file(&file_ref.rel_path, file_opts)
                .with_context(|| format!("zip: add {}", file_ref.rel_path))?;
            copy(&mut source, &mut zip)
                .with_context(|| format!("zip: write {}", file_ref.rel_path))?;
        }

        let mut file = zip.finish().context("zip: finish")?;
        file.rewind().context("rewind export zip file")?;
        Ok(file)
    })
    .await
    .context("join export zip task")?
}

/// For existing tasks: query current `to` DSN, inject `bearer_token` if user has a taosx token,
/// then ALTER the task to update the DSN before starting.
async fn inject_bearer_token_if_needed(
    dsn: &Dsn,
    session_manager: &SessionManager,
    req: &HttpRequest,
    task_id: i64,
) -> Result<()> {
    let sql = format!("SHOW XNODE TASKS WHERE ID = {task_id}");
    let Some(task) = query_one::<TaskRecord>(dsn, &sql).await? else {
        return Ok(());
    };

    let mut to_dsn_str = task.to.clone();
    let mut changed = false;

    // Try injecting from taosx_tokens table first.
    if let Some(username) = extract_username_from_request(req).await
        && let Ok(Some(token_value)) = session_manager.get_taosx_token(&username).await
        && let Ok(mut to_dsn) = Dsn::from_str(&to_dsn_str)
        && to_dsn.get("bearer_token").is_none_or(|v| v != &token_value)
    {
        to_dsn.set("bearer_token", &token_value);
        to_dsn_str = to_dsn.to_string();
        changed = true;
        tracing::debug!(
            "Injected bearer_token into existing task {} `to` DSN for user: {}",
            task_id,
            username
        );
    }

    // Fallback: convert `__token__`-prefixed password to `bearer_token` param.
    if let Some(fixed) = fix_token_password_in_dsn(&to_dsn_str) {
        to_dsn_str = fixed;
        changed = true;
    }

    if changed {
        let alter_sql = format!(
            "ALTER XNODE TASK {} FROM '{}' TO '{}'",
            task_id, task.from, to_dsn_str
        );
        exec(dsn, &alter_sql).await?;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn make_config(name: &str, from: &str, to: &str) -> HaTask {
        HaTask {
            name: name.to_string(),
            from: from.to_string(),
            to: to.to_string(),
            parser: None,
            via: None,
            labels: None,
        }
    }

    #[test]
    fn build_alter_sql_no_changes_omits_with_clause() {
        let config = make_config("my_task", "taos://localhost/db1", "taos://localhost/db2");
        let sql = build_update_task_sql(42, "my_task", &config).unwrap();
        assert_eq!(
            sql,
            "ALTER XNODE TASK 42 FROM 'taos://localhost/db1' TO 'taos://localhost/db2'"
        );
    }

    #[test]
    fn build_alter_sql_name_changed_includes_with_name() {
        let config = make_config("new_name", "taos://localhost/db1", "taos://localhost/db2");
        let sql = build_update_task_sql(42, "old_name", &config).unwrap();

        assert!(sql.contains("WITH"));
        assert!(sql.contains("NAME"));
        assert!(sql.contains("new_name"));
    }

    #[test]
    fn build_alter_sql_labels_present_includes_labels() {
        let mut config = make_config("my_task", "taos://localhost/db1", "taos://localhost/db2");
        config.labels = Some(json!({"type": "datain", "env": "test"}));
        let sql = build_update_task_sql(42, "my_task", &config).unwrap();

        assert!(sql.contains("WITH"));
        assert!(sql.contains("LABELS"));
        assert!(sql.contains("datain"));
    }

    #[test]
    fn build_alter_sql_name_and_labels_changed() {
        let mut config = make_config("renamed", "taos://localhost/db1", "taos://localhost/db2");
        config.labels = Some(json!({"owner": "qa"}));
        let sql = build_update_task_sql(100, "original", &config).unwrap();

        assert!(sql.contains("WITH"));
        assert!(sql.contains("NAME"));
        assert!(sql.contains("renamed"));
        assert!(sql.contains("LABELS"));
        assert!(sql.contains("qa"));
    }

    #[test]
    fn build_alter_sql_with_parser() {
        let mut config = make_config("my_task", "taos://localhost/db1", "taos://localhost/db2");
        config.parser = Some(json!({"type": "regex", "pattern": ".*"}));
        let sql = build_update_task_sql(1, "my_task", &config).unwrap();

        assert!(sql.contains("WITH"));
        assert!(sql.contains("PARSER"));
        assert!(sql.contains("regex"));
    }

    #[test]
    fn build_alter_sql_with_via() {
        let mut config = make_config("my_task", "taos://localhost/db1", "taos://localhost/db2");
        config.via = Some(7);
        let sql = build_update_task_sql(1, "my_task", &config).unwrap();

        assert!(sql.contains("WITH"));
        assert!(sql.contains("VIA 7"));
    }

    #[test]
    fn build_alter_sql_all_fields_changed() {
        let mut config = make_config("new_name", "taos://localhost/src", "taos://localhost/dst");
        config.parser = Some(json!({"type": "json"}));
        config.via = Some(3);
        config.labels = Some(json!({"suite": "full"}));
        let sql = build_update_task_sql(99, "old_name", &config).unwrap();

        assert!(sql.contains("ALTER XNODE TASK 99"));
        assert!(sql.contains("WITH"));
        assert!(sql.contains("NAME"));
        assert!(sql.contains("new_name"));
        assert!(sql.contains("PARSER"));
        assert!(sql.contains("VIA 3"));
        assert!(sql.contains("LABELS"));
    }

    #[test]
    fn build_alter_sql_only_name_unchanged_labels_not_provided() {
        let config = make_config("same_name", "taos://localhost/db1", "taos://localhost/db2");
        let sql = build_update_task_sql(10, "same_name", &config).unwrap();

        assert!(!sql.contains("WITH"));
        assert!(!sql.contains("NAME"));
        assert!(!sql.contains("LABELS"));
    }

    fn export_result_with_from(from: serde_json::Value) -> ExportTaskResult {
        serde_json::from_value(json!({
            "tasks_num": 1,
            "export_time": "2026-04-13T00:00:00Z",
            "tasks": [{
                "id": 1,
                "name": "demo-task",
                "from": from,
                "to": "taos:///target",
                "parser": null,
                "via": null,
                "created_at": "2026-04-13T00:00:00Z",
                "trigger": null,
                "labels": null
            }]
        }))
        .unwrap()
    }

    #[test]
    fn collect_file_refs_rejects_dot_dot_escape_paths() {
        let tempdir = tempdir().unwrap();
        let upload_dir = tempdir.path().join("upload");
        let outside_dir = tempdir.path().join("outside");
        std::fs::create_dir_all(&upload_dir).unwrap();
        std::fs::create_dir_all(&outside_dir).unwrap();

        let escaped_file = outside_dir.join("secret.txt");
        std::fs::write(&escaped_file, "secret").unwrap();

        let escaped_path = format!("@{}", upload_dir.join("../outside/secret.txt").display());
        let result = export_result_with_from(json!({
            "csv_config_file": escaped_path
        }));

        let refs = collect_file_refs_from_result(&result, &upload_dir).unwrap();

        assert!(refs.is_empty());
    }

    #[test]
    fn rewrite_paths_to_relative_errors_when_uploaded_file_is_missing() {
        let tempdir = tempdir().unwrap();
        let upload_dir = tempdir.path().join("upload");
        std::fs::create_dir_all(&upload_dir).unwrap();

        let existing_file = upload_dir.join("config").join("demo.csv");
        std::fs::create_dir_all(existing_file.parent().unwrap()).unwrap();
        std::fs::write(&existing_file, "demo").unwrap();

        let missing_file = upload_dir.join("config").join("missing.csv");
        let mut result = export_result_with_from(json!({
            "csv_config_file": format!("@{},@{}", existing_file.display(), missing_file.display())
        }));

        let error = rewrite_paths_to_relative(&mut result, &upload_dir).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("missing referenced uploaded file"),
            "unexpected error: {error:#}"
        );
        assert!(
            error.to_string().contains("files/config/missing.csv"),
            "expected sanitized relative reference, got: {error:#}"
        );
        assert!(
            !error
                .to_string()
                .contains(&missing_file.display().to_string()),
            "error leaked absolute path: {error:#}"
        );
    }

    #[tokio::test]
    async fn collect_file_refs_for_export_errors_when_uploaded_file_is_missing() {
        let tempdir = tempdir().unwrap();
        let upload_dir = tempdir.path().join("upload");
        std::fs::create_dir_all(&upload_dir).unwrap();
        let missing_file = upload_dir.join("config").join("missing.csv");

        let result = collect_file_refs_for_export(
            vec![json!({
                "csv_config_file": format!("@{}", missing_file.display())
            })],
            vec![upload_dir],
        )
        .await;

        let error = match result {
            Ok(file_refs) => panic!("expected missing file error, got refs: {}", file_refs.len()),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("missing referenced uploaded file"),
            "unexpected error: {error:#}"
        );
        assert!(
            error.to_string().contains("files/config/missing.csv"),
            "expected sanitized relative reference, got: {error:#}"
        );
        assert!(
            !error
                .to_string()
                .contains(&missing_file.display().to_string()),
            "error leaked absolute path: {error:#}"
        );
    }

    #[test]
    fn collect_file_refs_uses_explorer_data_dir_files_dir() {
        let tempdir = tempdir().unwrap();
        let data_dir = tempdir.path().join("data");
        let upload_dir = data_dir.join("files");
        let uploaded_file = upload_dir.join("1776045068549").join("demo.csv");
        std::fs::create_dir_all(uploaded_file.parent().unwrap()).unwrap();
        std::fs::write(&uploaded_file, "demo").unwrap();

        let result = export_result_with_from(json!({
            "data": {
                "csv_config_file": format!("@{}", uploaded_file.display())
            }
        }));

        let refs =
            collect_file_refs_from_result(&result, &resolve_upload_dir(Some(&data_dir))).unwrap();

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].rel_path, "files/1776045068549/demo.csv");
    }

    #[test]
    fn collect_file_refs_supports_taosx_upload_dir_when_explorer_data_dir_differs() {
        let tempdir = tempdir().unwrap();
        let explorer_data_dir = tempdir.path().join("explorer-data");
        let explorer_upload_dir = explorer_data_dir.join("files");
        std::fs::create_dir_all(&explorer_upload_dir).unwrap();

        let taosx_data_dir = tempdir.path().join("taosx-data");
        let taosx_upload_dir = taosx_data_dir.join("files");
        let uploaded_file = taosx_upload_dir.join("upload-456").join("demo.csv");
        std::fs::create_dir_all(uploaded_file.parent().unwrap()).unwrap();
        std::fs::write(&uploaded_file, "demo").unwrap();

        let result = export_result_with_from(json!({
            "data": {
                "csv_config_file": format!("@{}", uploaded_file.display())
            }
        }));

        let refs = collect_file_refs_from_result_with_upload_dirs(
            &result,
            &[
                resolve_upload_dir(Some(&explorer_data_dir)),
                taosx_upload_dir.clone(),
            ],
        )
        .unwrap();

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].abs_path, uploaded_file.canonicalize().unwrap());
        assert_eq!(refs[0].rel_path, "files/upload-456/demo.csv");
    }

    #[test]
    fn collect_file_refs_supports_relative_files_references() {
        let tempdir = tempdir().unwrap();
        let data_dir = tempdir.path().join("data");
        let upload_dir = data_dir.join("files");
        let uploaded_file = upload_dir.join("upload-123").join("demo.csv");
        std::fs::create_dir_all(uploaded_file.parent().unwrap()).unwrap();
        std::fs::write(&uploaded_file, "demo").unwrap();

        let result = export_result_with_from(json!({
            "data": {
                "csv_config_file": "@files/upload-123/demo.csv"
            }
        }));

        let refs =
            collect_file_refs_from_result(&result, &resolve_upload_dir(Some(&data_dir))).unwrap();

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].rel_path, "files/upload-123/demo.csv");
        assert_eq!(refs[0].source_ref, "@files/upload-123/demo.csv");
    }

    #[tokio::test]
    async fn collect_file_refs_for_export_matches_sync_collection() {
        let tempdir = tempdir().unwrap();
        let upload_dir = tempdir.path().join("upload");
        let uploaded_file = upload_dir.join("1776045068549").join("demo.csv");
        std::fs::create_dir_all(uploaded_file.parent().unwrap()).unwrap();
        std::fs::write(&uploaded_file, "demo").unwrap();

        let from = json!({
            "data": {
                "csv_config_file": format!("@{}", uploaded_file.display())
            }
        });
        let result = export_result_with_from(from.clone());

        let expected = collect_file_refs_from_result(&result, &upload_dir).unwrap();
        let actual = collect_file_refs_for_export(vec![from], vec![upload_dir.clone()])
            .await
            .unwrap();

        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual[0].abs_path, expected[0].abs_path);
        assert_eq!(actual[0].rel_path, expected[0].rel_path);
    }

    #[test]
    fn rewrite_paths_to_relative_uses_collected_refs_without_recrawling_filesystem() {
        let tempdir = tempdir().unwrap();
        let upload_dir = tempdir.path().join("upload");
        let uploaded_file = upload_dir.join("1776045068549").join("demo.csv");
        std::fs::create_dir_all(uploaded_file.parent().unwrap()).unwrap();
        std::fs::write(&uploaded_file, "demo").unwrap();

        let mut result = export_result_with_from(json!({
            "data": {
                "csv_config_file": format!("@{}", uploaded_file.display())
            }
        }));
        let refs = collect_file_refs_from_result(&result, &upload_dir).unwrap();
        std::fs::remove_file(&uploaded_file).unwrap();

        rewrite_paths_to_relative_with_refs(&mut result, &refs);

        assert_eq!(
            result.tasks[0].from["data"]["csv_config_file"],
            json!("@files/1776045068549/demo.csv")
        );
    }

    #[tokio::test]
    async fn build_export_zip_errors_when_file_disappears_before_packaging() {
        let tempdir = tempdir().unwrap();
        let upload_dir = tempdir.path().join("upload");
        let uploaded_file = upload_dir.join("1776045068549").join("demo.csv");
        std::fs::create_dir_all(uploaded_file.parent().unwrap()).unwrap();
        std::fs::write(&uploaded_file, "demo").unwrap();

        let file_refs = collect_file_refs_for_export(
            vec![json!({
                "data": {
                    "csv_config_file": format!("@{}", uploaded_file.display())
                }
            })],
            vec![upload_dir],
        )
        .await
        .unwrap();
        std::fs::remove_file(&uploaded_file).unwrap();

        let result = build_export_zip(br#"{"tasks":[]}"#.to_vec(), file_refs).await;
        let error = match result {
            Ok(_) => panic!("expected missing file error"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("missing referenced uploaded file"),
            "unexpected error: {error:#}"
        );
        assert!(
            error.to_string().contains("files/1776045068549/demo.csv"),
            "expected sanitized relative reference, got: {error:#}"
        );
        assert!(
            !error
                .to_string()
                .contains(&uploaded_file.display().to_string()),
            "error leaked absolute path: {error:#}"
        );
    }

    // ── aggregate_tmq_progress unit tests ────────────────────────────────────

    fn make_tmq_metrics(snapshot: Vec<taosx_core::tmq::tmq_metric::TopicProgress>) -> TmqMetrics {
        let m = TmqMetrics::default();
        *m.progress_snapshot.lock().unwrap() = snapshot;
        m
    }

    #[test]
    fn aggregate_deduplicates_by_max_offset_and_latest() {
        use taosx_core::tmq::tmq_metric::TopicProgress;

        let m1 = make_tmq_metrics(vec![TopicProgress {
            topic: "t1".into(),
            vgroup: 1,
            offset: 10,
            latest: 20,
        }]);
        let m2 = make_tmq_metrics(vec![TopicProgress {
            topic: "t1".into(),
            vgroup: 1,
            offset: 15,
            latest: 18,
        }]);

        let agg = aggregate_tmq_progress(vec![(m1, 1000), (m2, 2000)]);

        let entries: Vec<_> = agg.entries.values().copied().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (15, 20)); // max offset = 15, max latest = 20
        assert_eq!(agg.update_time, Some(2000));
    }

    #[test]
    fn aggregate_skips_empty_topic_entries() {
        use taosx_core::tmq::tmq_metric::TopicProgress;

        let m = make_tmq_metrics(vec![
            TopicProgress {
                topic: "".into(),
                vgroup: 1,
                offset: 5,
                latest: 10,
            },
            TopicProgress {
                topic: "real".into(),
                vgroup: 2,
                offset: 3,
                latest: 7,
            },
        ]);

        let agg = aggregate_tmq_progress(vec![(m, 500)]);

        assert_eq!(agg.entries.len(), 1);
        assert!(agg.entries.contains_key(&("real".to_string(), 2)));
    }

    #[test]
    fn aggregate_update_time_is_max_ts() {
        use taosx_core::tmq::tmq_metric::TopicProgress;

        let m1 = make_tmq_metrics(vec![TopicProgress {
            topic: "t".into(),
            vgroup: 1,
            offset: 1,
            latest: 2,
        }]);
        let m2 = make_tmq_metrics(vec![TopicProgress {
            topic: "t".into(),
            vgroup: 2,
            offset: 3,
            latest: 4,
        }]);

        let agg = aggregate_tmq_progress(vec![(m1, 100), (m2, 9999)]);
        assert_eq!(agg.update_time, Some(9999));
    }

    #[test]
    fn aggregate_empty_rows_gives_no_update_time() {
        let agg = aggregate_tmq_progress(vec![]);
        assert!(agg.update_time.is_none());
        assert!(agg.entries.is_empty());
    }

    #[test]
    fn table_progress_query_deserializes_all_fields() {
        let q: TableProgressQuery = serde_json::from_value(serde_json::json!({
            "table": "mytable",
            "start": "2024-01-01",
            "end": "2024-12-31"
        }))
        .unwrap();
        assert_eq!(q.table, "mytable");
        assert_eq!(q.start.as_deref(), Some("2024-01-01"));
        assert_eq!(q.end.as_deref(), Some("2024-12-31"));
    }

    #[test]
    fn table_progress_query_optional_fields_absent() {
        let q: TableProgressQuery =
            serde_json::from_value(serde_json::json!({ "table": "t" })).unwrap();
        assert_eq!(q.table, "t");
        assert!(q.start.is_none());
        assert!(q.end.is_none());
    }
}
