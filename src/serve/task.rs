use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::fs;

use actix_files::NamedFile;
use actix_multipart::form::{tempfile::TempFile, text::Text, MultipartForm};
use actix_web::body::BoxBody;
use actix_web::web::Json;
use actix_web::{
    delete, get, patch, post,
    web::{Data, Path, Query},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use anyhow::anyhow;
use anyhow::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Code;
use utoipa::*;

use taosx_core::core_metrics::CoreMetrics;
use taosx_core::{get_csv_files_from_task, get_data_dir, get_file_upload_home_dir};

use super::controller::agent::AgentActivityFilter;
use crate::serve::metrics::{get_task_metrics_string, try_get_metrics_from_task_detail};
use crate::serve::{
    controller::{Status, TaskControllerRef},
    NewTask, TaskDecorator, TaskFilter, UpdateTask,
};

/// Task endpoint error responses
#[derive(Debug, Default, Serialize, Deserialize, Clone, ToSchema)]
pub struct Failed<T = ()>
where
    T: Debug + serde::Serialize,
{
    /// Error code
    #[schema(example = 0, value_type = i32)]
    pub code: Code,
    /// Error reason
    pub message: String,

    pub data: T,
}

impl Failed<()> {
    pub fn from_error(err: impl Display) -> Self {
        Self {
            code: Code::FAILED,
            message: format!("{}", err),
            data: (),
        }
    }
}

impl<T: Debug + Serialize> Failed<T> {
    pub fn new(code: Code, message: String, data: T) -> Self {
        Self {
            code,
            message,
            data,
        }
    }
}

impl<T: Debug + Serialize> Display for Failed<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "code={:?} message={:?} data={:?}",
            self.code, self.message, self.data
        ))
    }
}

impl<T> ResponseError for Failed<T>
where
    T: Debug + serde::Serialize,
{
    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::InternalServerError().json(self)
    }
}

/// List tasks in current.
///
/// One could call the api endpoint with following curl.
///
/// ```shell
/// curl localhost:6040/tasks
/// ```
#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "List current task items", body = [Task])
        ),
        params(
        TaskFilter,
        TaskDecorator,
    )
)]
#[get("/tasks")]
pub(super) async fn get_tasks(
    task_store: Data<TaskControllerRef>,
    filter: Query<TaskFilter>,
    decorator: Query<TaskDecorator>,
) -> impl Responder {
    match task_store.tasks(filter.into_inner()).await {
        Ok(tasks) => Ok(HttpResponse::Ok()
            .append_header(("Count", tasks.len()))
            .json(
                tasks
                    .into_iter()
                    .map(|t| t.decorate(&decorator))
                    .collect_vec(),
            )),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// List tasks in current.
///
/// One could call the api endpoint with following curl.
///
/// ```shell
/// curl localhost:6040/tasks
/// ```
#[utoipa::path(
    tag = "tasks",
    responses(
    (status = 200, description = "Tasks count (deleted tasks will not be included by default)", body = [usize])
    ),
    params(
        TaskFilter,
    )
)]
#[get("/tasks/count")]
pub(super) async fn get_tasks_count(
    task_store: Data<TaskControllerRef>,
    filter: Query<TaskFilter>,
) -> impl Responder {
    match task_store.tasks_count(filter.into_inner()).await {
        Ok(tasks) => Ok(HttpResponse::Ok().body(format!("{tasks}"))),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Create new streaming task.
///
/// Post a new `Task` in request body as json to store it. Api will return
/// created `Task` on success.
///
/// One could call the api with.
///
/// ```shell
/// curl localhost:8080/task -d '{"from": "tmq:///test", "to": "local:test"}'
/// ```
#[utoipa::path(
    tag = "tasks",
    request_body = NewTask,
    params(
        TaskDecorator,
    ),
    responses(
        (status = 201, description = "Task created successfully", body = Task),
        // (status = 409, description = "Task with id already exists", body = ErrorResponse, example = json!(ErrorResponse::Conflict(String::from("id = 1"))))
    )
)]
#[post("/tasks")]
pub(super) async fn create_task(
    task: actix_web::web::Json<NewTask>,
    task_store: Data<TaskControllerRef>,
    decorator: Query<TaskDecorator>,
) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    let task = task.into_inner();
    tracing::info!(task.name, "create task with name");

    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    // validate parser
    if let Some(parser) = task.parser.as_ref() {
        // check TIMESTAMP Precision: all columns should have same precision
        let parser_string = parser.to_string();
        if !check_parser_timestamp_precision(&parser_string) {
            return Err(Failed::from_error(
                "parser shouldn't contains different timestamp precision",
            ));
        }
    }
    let controller = task_store.into_inner();
    match controller.create(task).await {
        Ok(task) => Ok(HttpResponse::Created().json(task.decorate(&decorator))),
        Err(err) => Err(Failed::from_error(err)),
    }
}

pub fn check_parser_timestamp_precision(parser_string: &str) -> bool {
    if (parser_string.contains(r#""TIMESTAMP""#) && parser_string.contains(r#""TIMESTAMP(us)""#))
        || (parser_string.contains(r#""TIMESTAMP""#)
            && parser_string.contains(r#""TIMESTAMP(ns)""#))
        || (parser_string.contains(r#""TIMESTAMP(us)""#)
            && parser_string.contains(r#""TIMESTAMP(ns)""#))
    {
        return false;
    }
    true
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub(super) enum FromOrTo {
    From(String),
    To(String),
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub(super) struct NewReplicate {
    /// Cluster username
    #[schema(example = "root")]
    username: String,
    /// Cluster password
    #[schema(example = "taosdata")]
    password: String,
    /// Source or target database name(or database topic name as data source).
    #[schema(example = "test2")]
    database: String,
    /// Replicate database from another TDengine data source to this.
    #[schema(example = "use from or to")]
    from: Option<String>,
    /// Replicate database to another TDengine.
    to: Option<String>,
    /// Set if the target database should be cleared before running task.
    #[schema(example = "false")]
    #[serde(default)]
    clear: bool,
    /// Override if database if not matched.
    #[serde(default)]
    force: bool,
}

#[derive(Copy, Clone, Debug)]
pub enum TaskBatchOperation {
    Start,
    Stop,
    Delete,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TaskBatchReq {
    ids: Vec<i64>,
}

/// Update Task by given path variable id.
///
/// This endpoint needs `api_key` authentication in order to call. Api key can be found from README.md.
///
/// Api will delete task from shared in-memory storage by the provided id and return success 200.
/// If storage does not contain `Task` with given id 404 not found will be returned.
#[utoipa::path(
    tag = "tasks",
    request_body = UpdateTask,
    responses(
        (status = 200, description = "Task deleted successfully"),
        // (status = 401, description = "Unauthorized to delete Task", body = ErrorResponse, example = json!(ErrorResponse::Unauthorized(String::from("missing api key")))),
        (status = 404, description = "Task not found by id", body = Failed)
    ),
    params(
        ("id", description = "Unique storage id of Task")
    ),
    params(
        TaskDecorator,
    ),
)]
#[patch("/tasks/{id}")]
pub(super) async fn update_task(
    id: Path<i64>,
    task: actix_web::web::Json<UpdateTask>,
    task_store: Data<TaskControllerRef>,
    decorator: Query<TaskDecorator>,
) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    // validate parser
    if let Some(parser) = task.parser.as_ref() {
        // check TIMESTAMP Precision: all columns should have same precision
        let parser_string = parser.to_string();
        if !check_parser_timestamp_precision(&parser_string) {
            return Err(Failed::from_error(
                "parser shouldn't contains different timestamp precision",
            ));
        }
    }
    match task_store.update(id.into_inner(), task.into_inner()).await {
        Ok(Some(task)) => Ok(HttpResponse::Ok().json(task.decorate(&decorator))),
        Ok(None) => Ok(HttpResponse::NotFound().finish()),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Delete Task by given path variable id.
///
/// This endpoint needs `api_key` authentication in order to call. Api key can be found from README.md.
///
/// Api will delete task from shared in-memory storage by the provided id and return success 200.
/// If storage does not contain `Task` with given id 404 not found will be returned.
#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "Task deleted successfully", body = Task),
        // (status = 401, description = "Unauthorized to delete Task", body = ErrorResponse, example = json!(ErrorResponse::Unauthorized(String::from("missing api key")))),
        (status = 404, description = "Task not found by id", body = Failed)
    ),
    params(
        ("id", description = "Unique storage id of Task")
        ),
        params(
        TaskDecorator,
    ),
)]
#[delete("/tasks/{id}")]
pub(super) async fn delete_task(
    id: Path<i64>,
    task_store: Data<TaskControllerRef>,
    decorator: Query<TaskDecorator>,
) -> impl Responder {
    let id = id.into_inner();
    match task_store.delete(id).await {
        Ok(Some(task)) => Ok(HttpResponse::Ok().json(task.decorate(&decorator))),
        Ok(None) => Ok(HttpResponse::NotFound().json(Failed::new(
            Code::FAILED,
            format!("Task {id} not found"),
            (),
        ))),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "delete batch tasks"),
        (status = 500, description = "failed to delete tasks", body = Failed),
    )
)]
#[post("/tasks/delete")]
pub async fn delete_tasks(
    ids: Json<TaskBatchReq>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let result = batch_operation(
        TaskBatchOperation::Delete,
        ids.ids.clone(),
        task_store.clone(),
    )
    .await;

    if result.is_empty() {
        return Ok(HttpResponse::Ok().finish());
    }
    Err(Failed::new(
        Code::FAILED,
        "failed to delete tasks".to_string(),
        result,
    ))
}

/// Get Task by given task id.
///
/// Return found `Task` with status 200 or 404 not found if `Task` is not found from shared in-memory storage.
#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "Task found from storage", body = Task),
        (status = 404, description = "Task not found by id", body = Failed)
    ),
    params(
        TaskDecorator,
    ),
    params(
        ("id", description = "Unique storage id of Task")
    ),
)]
#[get("/tasks/{id}")]
pub(super) async fn get_task_by_id(
    id: Path<i64>,
    task_store: Data<TaskControllerRef>,
    decorator: Query<TaskDecorator>,
) -> impl Responder {
    let id = id.into_inner();
    match task_store.get(id).await {
        Ok(Some(task)) => Ok(HttpResponse::Ok().json(task.decorate(&decorator))),
        Ok(None) => Ok(HttpResponse::NotFound().finish()),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Start [Task] by given path variable id.
///
/// If storage does not contain `Task` with given id 404 not found will be returned.
#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "Task started successfully"),
        (status = 404, description = "Task not found by id", body = Failed),
        (status = 500, description = "Server error", body = Failed),
    ),
    params(
        ("id", description = "Unique storage id of Task")
    ),
)]
#[post("/tasks/{id}/start")]
pub(super) async fn start_task(
    id: Path<i64>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let id = id.into_inner();
    match task_store.start(id).await {
        Ok(Some(_)) => Ok(HttpResponse::Ok().body("{}")),
        Ok(None) => Ok(HttpResponse::NotFound().json(Failed::new(
            Code::FAILED,
            format!("Task {id} not found"),
            (),
        ))),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
struct TaskBatchResponse {
    id: Option<i64>,
    error: Option<String>,
}

#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "start batch tasks"),
        (status = 500, description = "failed to start tasks", body = Failed),
    )
)]
#[post("/tasks/start")]
pub async fn start_tasks(
    req: Json<TaskBatchReq>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let result = batch_operation(
        TaskBatchOperation::Start,
        req.ids.clone(),
        task_store.clone(),
    )
    .await;

    if result.is_empty() {
        return Ok(HttpResponse::Ok().body("{}"));
    }
    Err(Failed::new(
        Code::FAILED,
        "failed to start tasks".to_string(),
        result,
    ))
}

/// Stop [Task] by given path variable id.
///
/// If storage does not contain `Task` with given id 404 not found will be returned.
#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "Task stopped successfully"),
        (status = 404, description = "Task not found by id", body = Failed),
        (status = 500, description = "Server error", body = Failed),

    ),
    params(
        ("id", description = "Unique storage id of Task")
    ),
)]
#[post("/tasks/{id}/stop")]
pub(super) async fn stop_task(
    id: Path<i64>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let id = id.into_inner();
    match task_store.stop(id).await {
        Ok(Some(_)) => Ok(HttpResponse::Ok().body("{}")),
        Ok(None) => Ok(HttpResponse::NotFound().json(Failed::new(
            Code::FAILED,
            format!("Task {id} not found"),
            (),
        ))),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "stop batch tasks"),
        (status = 500, description = "failed to stop tasks", body = Failed),
    )
)]
#[post("/tasks/stop")]
pub async fn stop_tasks(
    ids: Json<TaskBatchReq>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let result = batch_operation(
        TaskBatchOperation::Stop,
        ids.ids.clone(),
        task_store.clone(),
    )
    .await;

    if result.is_empty() {
        return Ok(HttpResponse::Ok().body("{}"));
    }
    Err(Failed::new(
        Code::FAILED,
        "failed to stop tasks".to_string(),
        result,
    ))
}

async fn batch_operation(
    operation: TaskBatchOperation,
    ids: Vec<i64>,
    task_store: Data<TaskControllerRef>,
) -> Vec<TaskBatchResponse> {
    let mut set = tokio::task::JoinSet::new();
    for id in ids.iter() {
        let id_clone = *id;
        let task_store_clone = task_store.clone();
        set.spawn(async move {
            match operation {
                TaskBatchOperation::Start => match task_store_clone.start(id_clone).await {
                    Ok(Some(_)) => TaskBatchResponse {
                        id: Some(id_clone),
                        error: None,
                    },
                    Ok(None) => TaskBatchResponse {
                        id: Some(id_clone),
                        error: Some(format!("Task {id_clone} not found")),
                    },
                    Err(err) => TaskBatchResponse {
                        id: Some(id_clone),
                        error: Some(format!("{:?}", err)),
                    },
                },
                TaskBatchOperation::Stop => match task_store_clone.stop(id_clone).await {
                    Ok(Some(_)) => TaskBatchResponse {
                        id: Some(id_clone),
                        error: None,
                    },
                    Ok(None) => TaskBatchResponse {
                        id: Some(id_clone),
                        error: Some(format!("Task {id_clone} not found")),
                    },
                    Err(err) => TaskBatchResponse {
                        id: Some(id_clone),
                        error: Some(format!("{:?}", err)),
                    },
                },
                TaskBatchOperation::Delete => match task_store_clone.delete(id_clone).await {
                    Ok(Some(_)) => TaskBatchResponse {
                        id: Some(id_clone),
                        error: None,
                    },
                    Ok(None) => TaskBatchResponse {
                        id: Some(id_clone),
                        error: Some(format!("Task {id_clone} not found")),
                    },
                    Err(err) => TaskBatchResponse {
                        id: Some(id_clone),
                        error: Some(format!("{:?}", err)),
                    },
                },
            }
        });
    }

    let mut result = Vec::new();
    while let Some(res) = set.join_next().await {
        match res {
            Ok(response) => {
                if response.error.is_some() {
                    result.push(response);
                }
            }
            Err(err) => {
                result.push(TaskBatchResponse {
                    id: None,
                    error: Some(format!("{:?}", err)),
                });
            }
        }
    }

    result
}

/// Get Task Offsets by given task id.
///
/// Return found `Task Offsets` with status 200 or 404 not found if `Task Offsets` is not found from shared in-memory storage.
#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "Task offsets found from storage", body = String),
        (status = 404, description = "Task not found by id", body = Failed)
    ),
    params(
        ("id", description = "Unique storage id of Task")
    ),
)]
#[get("/tasks/{id}/offsets")]
pub(super) async fn get_task_offsets_by_id(
    id: Path<i64>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let id = id.into_inner();
    match task_store.offsets(id).await {
        Ok(Some(offsets)) => Ok(HttpResponse::Ok().json(offsets)),
        Ok(None) => Ok(HttpResponse::NotFound().json(Failed::new(
            Code::FAILED,
            format!("Task {id} not found"),
            (),
        ))),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Get Task activities by given task id.
///
#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "Task activities of the task", body = Vec < TaskActivity >),
        ),
    params(
        ("id", description = "Unique storage id of Task"),
        AgentActivityFilter
    ),
)]
#[get("/tasks/{id}/activities")]
pub(super) async fn get_task_activities_by_id(
    task_store: Data<TaskControllerRef>,
    id: Path<i64>,
    filter: Query<AgentActivityFilter>,
) -> impl Responder {
    let id = id.into_inner();
    match task_store.task_activities(id, &filter.into_inner()).await {
        Ok(acts) => Ok(HttpResponse::Ok().json(acts)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Get metrics json string of a task for displaying on the web UI
#[get("/tasks/{id}/metrics")]
pub(super) async fn get_task_metrics(
    task_store: Data<TaskControllerRef>,
    id: Path<i64>,
) -> impl Responder {
    let task_id = id.into_inner();
    let task = task_store.get(task_id).await;
    match task {
        Ok(Some(task)) => {
            let metrics = try_get_metrics_from_task_detail(&task).await;
            match metrics {
                Some(metrics) => {
                    let status: &Status = task.status();
                    get_task_metrics_string(status, metrics)
                }
                None => "".to_string(),
            }
        }
        Ok(None) => "{}".to_string(),
        Err(err) => {
            tracing::error!("get task metrics error: {}", err);
            "{}".to_string()
        }
    }
}

/// Get csv files json string of a task for displaying on the web UI
#[get("/tasks/{id}/csv_files")]
pub(super) async fn get_task_csv_files(
    task_store: Data<TaskControllerRef>,
    id: Path<i64>,
) -> impl Responder {
    let task_id = id.into_inner();
    let task = task_store.get(task_id).await;
    match task {
        Ok(Some(_)) => {
            let csv_files = get_csv_files_from_task(Some(task_id)).await;
            match csv_files {
                Ok(csv_files) => serde_json::to_string(&csv_files).unwrap(),
                Err(err) => {
                    tracing::error!("get task csv files error: {}", err);
                    "{}".to_string()
                }
            }
        }
        Ok(None) => "{}".to_string(),
        Err(err) => {
            tracing::error!("get task csv files error: {}", err);
            "{}".to_string()
        }
    }
}

/// Get tmq task progress by given task ID in respect of the vgroup consume progress.
#[get("/tasks/{id}/vgroup_progress")]
pub(super) async fn get_tmq_task_vgroup_progress(
    task_store: Data<TaskControllerRef>,
    id: Path<i64>,
) -> impl Responder {
    let task_id = id.into_inner();
    let task = task_store.get(task_id).await;
    match task {
        Ok(Some(task)) => {
            let metrics = try_get_metrics_from_task_detail(&task).await;
            match metrics {
                Some(metrics) => match metrics.as_ref() {
                    CoreMetrics::TMQ(tmq_metrics) => tmq_metrics.get_progress_string(),
                    _ => {
                        tracing::error!("Expect TmqMetrics, but got: {:?}", metrics);
                        "{}".to_string()
                    }
                },
                None => {
                    tracing::info!("Not found metrics for task: {}", task_id);
                    "{}".to_string()
                }
            }
        }
        Ok(None) => {
            tracing::info!("Not found task by id: {}", task_id);
            "{}".to_string()
        }
        Err(err) => {
            tracing::error!("Get task error: {}", err);
            "{}".to_string()
        }
    }
}

/// Get tmq task progress by given task ID in respect of latest data in specific table.
#[get("/tasks/{id}/table_progress")]
pub(super) async fn get_tmq_task_table_progress(
    task_store: Data<TaskControllerRef>,
    id: Path<i64>,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let task_id = id.into_inner();
    let table = query.get("table");
    if table.is_none() {
        return Err(Failed::from_error("table name is required"));
    }
    let table = table.unwrap().as_str();
    let start = query.get("start");
    let end = query.get("end");
    let task = task_store.get(task_id).await;
    match task {
        Ok(Some(task)) => {
            let from = &task.task.from;
            let to = &task.task.to;
            let table_progress = taosx_core::get_table_progress(from, to, table, start, end).await;
            match table_progress {
                Ok(progress) => Ok(serde_json::to_string(&progress).unwrap()),
                Err(err) => {
                    tracing::error!("Get table progress error: {}", err);
                    Err(Failed::from_error(err))
                }
            }
        }
        Ok(None) => {
            tracing::info!("Not found task by id: {}", task_id);
            Ok("{}".to_string())
        }
        Err(err) => {
            tracing::error!("Get task error: {}", err);
            Ok("{}".to_string())
        }
    }
}

#[derive(Debug, MultipartForm, ToSchema)]
pub struct UploadForm {
    #[multipart(rename = "file")]
    files: Vec<TempFile>,
    req_id: Text<String>,
}

#[utoipa::path(
    tag = "tasks",
    request_body(content = UploadForm, content_type = "multipart/form-data"),
    responses(
        (status = 201, description = "file uploaded", body = Vec < String >),
        (status = 500, description = "file upload error", body = Failed)
    ),
)]
#[post("/upload")]
pub async fn upload_files(MultipartForm(form): MultipartForm<UploadForm>) -> impl Responder {
    match save_files(MultipartForm(form)).await {
        Ok(file_saved) => Ok(HttpResponse::Created().json(file_saved)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

async fn save_files(MultipartForm(form): MultipartForm<UploadForm>) -> anyhow::Result<Vec<String>> {
    let upload_dir = get_file_upload_home_dir();
    let mut file_save_paths = Vec::new();
    if form.files.is_empty() {
        anyhow::bail!("upload file is empty");
    }
    let req_id = form.req_id.to_string();
    for f in form.files {
        // let uuid = uuid::Uuid::new_v4();
        let path = upload_dir.join(&req_id);
        fs::create_dir_all(&path).with_context(|| "create file path failed")?;
        let file_name = f.file_name.unwrap();
        let relative_path = format!("{req_id}/{file_name}");
        tracing::info!(
            "saving to {}, {relative_path}",
            upload_dir.to_str().unwrap()
        );
        let path = upload_dir.join(&req_id).join(&file_name);
        if let Err(persis_err) = f.file.persist(&path) {
            // fallback to copy
            std::fs::copy(persis_err.file.path(), path).context("cannot save uploaded file")?;
        }
        file_save_paths.push(format!("./files/{req_id}/{file_name}"));
    }
    Ok(file_save_paths)
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct FileMeta {
    filename: Option<String>,
    /// relative
    filepath: Option<String>,
    filesize: Option<u64>,
    file_header: Option<FileMetaHeader>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_values: Option<Vec<Vec<String>>>,
}

#[derive(Serialize, Deserialize, Default, Clone, IntoParams, ToSchema)]
#[serde(default)]
pub struct FileMetaRequest {
    file_path: String,
    file_type: String,
    has_header: bool,
    skip: Option<usize>,
    delimiter: Option<String>,
    quote: Option<String>,
    comment: Option<String>,
    sample: Option<usize>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct FileMetaHeader {
    columns_length: usize,
    column_names: Option<Vec<String>>,
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "filemeta access success", body = Vec < String >),
        (status = 500, description = "metadata archive occur error", body = Failed)
    ),
    params(
        FileMetaRequest
    )
)]
#[get("/filemeta")]
pub async fn filemeta(filemeta_request: Query<FileMetaRequest>) -> impl Responder {
    match get_filemeta(filemeta_request.into_inner()).await {
        Ok(filemeta) => Ok(HttpResponse::Ok().json(filemeta)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

async fn get_filemeta(filemeta_request: FileMetaRequest) -> anyhow::Result<FileMeta> {
    let (filepath_or_filedir, file_type, has_header, skip, delimiter, quote, comment, sample) = (
        filemeta_request.file_path,
        filemeta_request.file_type,
        filemeta_request.has_header,
        filemeta_request.skip.unwrap_or(0),
        filemeta_request.delimiter.unwrap_or_default(),
        filemeta_request.quote.unwrap_or_default(),
        filemeta_request.comment.unwrap_or_default(),
        filemeta_request.sample.unwrap_or(5),
    );

    let delimiter = delimiter.trim();
    let delimiter = match delimiter.len() {
        0 => None,
        1 => Some(Ok(delimiter.as_bytes()[0])),
        _ => Some(Err(anyhow!("CSV delimiter should be a single character"))),
    }
    .transpose()?
    .unwrap_or(b',');

    let quote = quote.trim();
    let quote = match quote.as_bytes() {
        [] => None,
        [quote] if *quote == delimiter => Some(Err(anyhow!(
            "CSV quote should not be the same as delimiter"
        ))),
        [quote] => Some(Ok(*quote)),
        _ => Some(Err(anyhow!("CSV quote should be a single character"))),
    }
    .transpose()?;

    let comment = comment.trim();
    let comment = match comment.as_bytes() {
        [] => None,
        [comment] if *comment == delimiter => Some(Err(anyhow!(
            "CSV comment should not be the same as delimiter"
        ))),
        [comment] => Some(Ok(*comment)),
        _ => Some(Err(anyhow!("CSV comment should be a single character"))),
    }
    .transpose()?;

    let data_dir = get_data_dir();

    match file_type.as_str() {
        "csv" => {
            let filepath_or_filedir = filepath_or_filedir
                .split(",")
                .map(|path| data_dir.join(path).display().to_string())
                .collect_vec();
            let csv_header = taosx_core::csv_header(
                filepath_or_filedir,
                has_header,
                skip,
                Some(delimiter),
                quote,
                comment,
                sample,
            )
            .await?;
            if csv_header.columns == 0 {
                anyhow::bail!("CSV file(s) are empty");
            }
            let column_names = if csv_header.headers.is_empty() {
                let mut columns_temp = vec![];
                for n in 0..(csv_header.columns) {
                    columns_temp.push(format!("c{n}"));
                }
                Some(columns_temp)
            } else {
                Some(csv_header.headers)
            };
            Ok(FileMeta {
                filename: None,
                filepath: None,
                filesize: None,
                file_header: Some(FileMetaHeader {
                    columns_length: csv_header.columns,
                    column_names,
                }),
                sample_values: if csv_header.values.is_empty() {
                    None
                } else {
                    Some(csv_header.values)
                },
            })
        }
        _ => {
            anyhow::bail!("file type not support now");
        }
    }
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct DownloadParams {
    file_path: String,
}

#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "success", body = NamedFile),
        (status = 500, description = "file download error", body = Failed)
    ),
    params(
        DownloadParams
    )
)]
#[get("/download")]
pub async fn download_files(params: Query<DownloadParams>, req: HttpRequest) -> impl Responder {
    match download(params).await {
        Ok(named_file) => Ok(named_file.into_response(&req)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

async fn download(file_path: Query<DownloadParams>) -> anyhow::Result<NamedFile> {
    let file_path = file_path.into_inner().file_path;
    let data_dir = get_data_dir();
    let file_path = data_dir.join(file_path);
    let meta = std::fs::metadata(file_path.clone()).with_context(|| "get file metadata error")?;
    if meta.is_dir() {
        anyhow::bail!("not support path");
    }
    Ok(NamedFile::open(file_path)?)
}
