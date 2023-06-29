use std::{path::PathBuf, fs};

use actix_web::{
    delete, get, patch, post,
    web::{Data, Path, Query, },
    HttpResponse, Responder, 
};

use anyhow::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use taos::Code;

use tokio_cron_scheduler::Job;

use utoipa::*;

use crate::serve::{
    controller::TaskControllerRef, NewTask, TaskController, TaskDecorator, TaskFilter, UpdateTask,
};

/// Task endpoint error responses
#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub(super) struct Failed {
    /// Error code
    #[schema(example = 0, value_type = i32)]
    pub code: Code,
    /// Error reason
    pub message: String,
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
        Ok(tasks) => HttpResponse::Ok()
            .append_header(("Count", tasks.len()))
            .json(
                tasks
                    .into_iter()
                    .map(|t| t.decorate(&decorator))
                    .collect_vec(),
            ),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
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
        Ok(tasks) => HttpResponse::Ok().body(format!("{tasks}")),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
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
    let task = task.into_inner();
    if let Some(trigger) = task.trigger.as_deref() {
        if !trigger.starts_with("schedule:") {
            return HttpResponse::InternalServerError().json(Failed {
                code: Code::Failed,
                message: format!(
                    "invalid trigger format: `{trigger}`, only `schedule:<crontab>` is supported"
                ),
            });
        }
    }
    let controller = task_store.into_inner();
    match controller.create(task).await {
        Ok(task) => {
            dbg!(&task.trigger);
            if let Some(trigger) = task.trigger.as_deref() {
                let schedule = trigger.trim_start_matches("schedule:");
                let sched = controller.scheduler.clone();
                let id = task.id;
                match Job::new_async(schedule, move |uuid, mut l| {
                    let controller = controller.clone();
                    Box::pin(async move {
                        log::info!("waiting for next tick");
                        let next_tick = l.next_tick_for_job(uuid).await;
                        match next_tick {
                            Ok(Some(ts)) => {
                                log::info!("Next tick is {:?}", ts);
                                let _ = controller.start(id).await;
                            }
                            _ => log::warn!("Could not get next tick"),
                        }
                    })
                }) {
                    Ok(job) => {
                        log::info!("add job for task: {task:?}");
                        if let Err(_err) = sched.add(job).await {
                            return HttpResponse::InternalServerError().json(Failed {
                                code: Code::Failed,
                                message: format!(
                    "invalid trigger format: `{trigger}`, only `schedule:<crontab>` is supported"
                ),
                            });
                        }
                        // sched.start().await.unwrap();
                    }
                    Err(err) => {
                        log::error!("Scheduler task error: {err:?}, task:{task:?}");
                    }
                }
            }
            HttpResponse::Created().json(task.decorate(&decorator))
        }
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
    }
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
    match task_store.update(id.into_inner(), task.into_inner()).await {
        Ok(Some(task)) => HttpResponse::Ok().json(task.decorate(&decorator)),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
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
    match task_store.delete(id.into_inner()).await {
        Ok(Some(task)) => HttpResponse::Ok().json(task.decorate(&decorator)),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
    }
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
        Ok(Some(task)) => HttpResponse::Ok().json(task.decorate(&decorator)),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
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
        Ok(Some(_)) => HttpResponse::Ok().body("{}"),
        Ok(None) => HttpResponse::NotFound().json(Failed {
            code: Code::Failed,
            message: format!("Task {id} not found"),
        }),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
    }
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
        Ok(Some(_)) => HttpResponse::Ok().body("{}"),
        Ok(None) => HttpResponse::NotFound().json(Failed {
            code: Code::Failed,
            message: format!("Task {id} not found"),
        }),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
    }
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
        Ok(Some(offsets)) => HttpResponse::Ok().body(format!("{:?}", offsets)),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
    }
}

/// Get Task activities by given task id.
///
#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "Task activities of the task", body = Vec<TaskActivity>),
    ),
    params(
        ("id", description = "Unique storage id of Task")
    ),
)]
#[get("/tasks/{id}/activities")]
pub(super) async fn get_task_activities_by_id(
    id: Path<i64>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let id = id.into_inner();
    match task_store.task_activities(id).await {
        Ok(acts) => HttpResponse::Ok().json(acts),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
    }
}

use actix_multipart::{
    form::{
        tempfile::{TempFile, },
        MultipartForm,
    },
};
#[derive(Debug, MultipartForm, ToSchema)]
pub struct UploadForm {
    #[multipart(rename = "file")]
    files: Vec<TempFile>,
}
#[utoipa::path(
    tag = "tasks",
    request_body(content = UploadForm, content_type = "multipart/form-data"),
    responses(
        (status = 201, description = "file uploaded", body = Vec<String>),
        (status = 500, description = "file upload error", body = Failed)
    ),
)]
#[post("/upload")]
pub async fn upload_files(MultipartForm(form): MultipartForm<UploadForm>,) -> impl Responder {
    match save_files(MultipartForm(form)).await {
        Ok(file_saved) => HttpResponse::Created().json(file_saved),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: format!("err: {}, cause: {}", err.to_string(), err.root_cause().to_string()),
        }),
    }
}

async fn save_files(MultipartForm(form): MultipartForm<UploadForm>,) -> anyhow::Result<Vec<String>> {
    let upload_file_save_path = get_file_save_home_dir();
    let mut file_save_paths = Vec::new();
    if form.files.is_empty() {
        anyhow::bail!("upload file is empty");
    }
    for f in form.files {
        let uuid = uuid::Uuid::new_v4();
        let path = std::path::Path::new(&format!("{}/{uuid}", upload_file_save_path.as_os_str().to_str().unwrap())).to_path_buf();
        fs::create_dir_all(&path).with_context(|| "create file path failed")?;
        let file_name = f.file_name.unwrap();
        let releative_path = format!("{}/{file_name}", uuid::Uuid::new_v4());
        log::info!("saving to {}, {releative_path}", upload_file_save_path.as_os_str().to_str().unwrap());
        file_save_paths.push(format!("./files/{releative_path}"));
        let path = std::path::Path::new(&format!("{}/{uuid}/{file_name}", upload_file_save_path.as_os_str().to_str().unwrap())).to_path_buf();
        f.file.persist(path)?;
    }
    Ok(file_save_paths)
}

// const ENV_TAOSX_UPLOAD_FILE_HOME: &'static str = "TAOSX_UPLOAD_FILE_HOME";
pub(crate) const ENV_TAOSX_UPLOAD_FILE_HOME_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\Program Files\\taosX\\files"
        } else {
            "/usr/local/taosx/files"
        }
    }
};
#[inline]
pub fn get_file_save_home_dir() -> PathBuf {
    // let env = std::env::var(ENV_TAOSX_UPLOAD_FILE_HOME)
        // .unwrap_or_else(|_| ENV_TAOSX_UPLOAD_FILE_HOME_DEFAULT.to_string());
    std::path::Path::new(&ENV_TAOSX_UPLOAD_FILE_HOME_DEFAULT).to_path_buf()
}