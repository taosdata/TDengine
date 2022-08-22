use std::{collections::HashMap, sync::Mutex};

use actix_web::{
    delete, get, post, put,
    web::{Data, Json, Path, Query, ServiceConfig},
    HttpResponse, Responder,
};
use tokio_util::sync::CancellationToken;
// use sqlx::types::
use chrono::{DateTime, Local};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::migrate::Migrator;
use sqlx::{Sqlite, SqlitePool};
use std::str::FromStr;
use taos::{Address, Code, Dsn, DsnError};
use tokio::{runtime::Runtime, sync::RwLock};
use utoipa::{IntoParams, *};

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

use super::{LogApiKey, RequireApiKey};

// const TASK_SELECT: &str = "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks";

#[derive()]
pub(super) struct TaskController {
    pool: SqlitePool,
    runtime: Runtime,
    tasks: RwLock<
        HashMap<
            i64,
            (
                tokio::task::JoinHandle<Result<(), anyhow::Error>>,
                CancellationToken,
            ),
        >,
    >,
    // tasks: Mutex<Vec<Task>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
// #[derive(sqlx::Type)]
pub(super) enum Schedule {
    Oneshot,
    Repeated(String),
}

#[derive(Serialize, Deserialize, Component, Clone, Debug)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
pub(super) enum StreamType {
    Replicate,
    Backup,
    Restore,
    Subscribe,
    Export,
}

impl TaskController {
    pub async fn new(sqlite: &str) -> anyhow::Result<Self> {
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(sqlite)?.create_if_missing(true);
        let pool = sqlx::SqlitePool::connect_with(options).await?;
        MIGRATOR.run(&pool).await?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .max_blocking_threads(1024)
            .build()?;
        Ok(Self {
            pool,
            runtime,
            tasks: Default::default(),
        })
    }

    pub async fn tasks(&self) -> anyhow::Result<Vec<Task>> {
        let tasks = sqlx::query_as_unchecked!(
            Task,
            "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where deleted = FALSE",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(tasks)
    }

    pub async fn create(&self, task: NewTask) -> anyhow::Result<Task> {
        let res = sqlx::query(
            "INSERT INTO tasks (`from`, `to`, `stream_type`, `created_at`, `status`) VALUES(?, ?, ?, ?, ?)",
        )
        .bind(&task.from)
        .bind(&task.to)
        .bind(&task.stream_type)
        .bind(&chrono::Local::now().to_rfc3339())
        .bind(&Status::Created)
        .execute(&self.pool)
        .await
        .unwrap();
        let opts = taosx::TaskOpts::try_from(task.clone())?;
        let id = res.last_insert_rowid();
        let task = sqlx::query_as_unchecked!(Task, "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where id = ?", id)
            .fetch_one(&self.pool)
            .await
            .unwrap();
        let pool = self.pool.clone();
        // let (tx, rx) = tokio::sync::oneshot::channel();
        let token = tokio_util::sync::CancellationToken::new();
        let cloned_token = token.clone();
        let handle = self.runtime.spawn(async move {
            tokio::select! {
                _ = cloned_token.cancelled() => {
                    let now = Local::now();
                    let status = Status::Cancelled;
                    let _ = sqlx::query!(
                        "UPDATE tasks SET finished_at = ?, status = ? WHERE id = ?",
                        now,
                        status,
                        id
                    )
                    .execute(&pool)
                    .await?;
                }
                result = opts.run() => {
                    match result {
                        Ok(_) => {
                            let now = Local::now();
                            let status = Status::Completed;
                            let _ = sqlx::query!(
                                "UPDATE tasks SET finished_at = ?, status = ? WHERE id = ?",
                                now,
                                status,
                                id
                            )
                            .execute(&pool)
                            .await?;

                        }
                        Err(err) => {
                            log::error!("run task {id} failed: {err}");
                            let err = err.to_string();
                            let now = Local::now();
                            let status = Status::Failed;
                            let _ = sqlx::query!(
                                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ?",
                                now,
                                status,
                                err,
                                id
                            )
                            .execute(&pool)
                            .await?;
                        }
                    }
                }
            }
            Ok(())
        });
        self.tasks.write().await.insert(id, (handle, token));

        Ok(task)
    }

    pub async fn get(&self, id: i64) -> anyhow::Result<Option<Task>> {
        let task = sqlx::query_as_unchecked!(Task, "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where id = ?", id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(task)
    }

    pub async fn delete(&self, id: i64) -> anyhow::Result<Option<()>> {
        if let Some((handle, token)) = self.tasks.write().await.remove(&id) {
            token.cancel();
            if !handle.is_finished() {
                // token.cancel();
                log::error!("Cancel task by id {id}");
                let _ = handle.await;
            }
        }
        let res =
            sqlx::query_as_unchecked!(Task, "UPDATE tasks SET `deleted` = TRUE where id = ?", id)
                .execute(&self.pool)
                .await?;
        // dbg!(res);
        if res.rows_affected() == 1 {
            log::info!("successfully deleted task by id {id}");
        }
        Ok(Some(()))
    }
}

pub(super) fn configure(store: Data<TaskController>) -> impl FnOnce(&mut ServiceConfig) {
    |config: &mut ServiceConfig| {
        config
            .app_data(store)
            // .service(search_tasks)
            .service(get_tasks)
            .service(create_task)
            .service(delete_task)
            .service(replicate)
            .service(subscribe)
            .service(get_task_by_id)
            // .service(update_task)
            ;
    }
}

#[derive(Serialize, Deserialize, Component, Clone, Debug)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
pub(super) enum Status {
    Created,
    Running,
    Cancelled,
    Completed,
    Failed,
}

/// A streaming workflow task description.
#[derive(Serialize, Deserialize, Component, Clone, Debug)]
pub(super) struct Task {
    /// Unique id for the task item.
    #[component(read_only, example = 1)]
    id: i64,
    /// Task stream data type.
    #[component(read_only, example = "backup")]
    stream_type: StreamType,
    /// The stream data source.
    #[component(example = "tmq:///test")]
    from: String,
    /// The target of the stream.
    #[component(example = "local:/path/to/backup/test")]
    to: String,

    /// Created time.
    #[component(read_only)]
    created_at: DateTime<Local>,

    /// Stopped time.
    #[component(read_only)]
    #[serde(skip_serializing_if = "Option::is_none")]
    finished_at: Option<DateTime<Local>>,

    /// Last modified time.
    #[component(read_only)]
    #[serde(skip_serializing_if = "Option::is_none")]
    last_modified_at: Option<DateTime<Local>>,

    /// The current status of the tasks.
    #[component(read_only, value_type = String)]
    status: Status,

    /// Status reason (only for status: failed).
    #[component(read_only)]
    reason: Option<String>,

    /// Mark the task done as expected.
    #[component(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    completed: bool,
    /// Mark the task is cancelled or not.
    #[component(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    cancelled: bool,

    /// Mark the task deleted or not, deleted tasks will not be listed when query all.
    #[component(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    deleted: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Serialize, Deserialize, Component, Clone, Debug)]
pub(super) struct NewTask {
    #[component(example = "backup")]
    stream_type: StreamType,
    /// The stream data source.
    #[component(example = "tmq:///test")]
    from: String,
    /// The target of the stream.
    #[component(example = "local:/path/to/backup/test")]
    to: String,

    /// Jobs number
    #[component(example = "0")]
    #[serde(default)]
    jobs: usize,
    #[serde(default)]
    compression_level: Option<usize>,
    #[serde(default)]
    force: bool,
}

impl TryFrom<NewTask> for taosx::TaskOpts {
    type Error = anyhow::Error;

    fn try_from(value: NewTask) -> Result<Self, Self::Error> {
        let NewTask {
            from,
            to,
            jobs,
            compression_level,
            force,
            stream_type,
        } = value;
        Ok(Self {
            from: from.parse()?,
            to: to.parse()?,
            jobs,
            compression_level,
            force,
        })
    }
}

/// Task endpoint error responses
#[derive(Serialize, Deserialize, Clone, Component)]
pub(super) enum ErrorResponse {
    /// When Task is not found by search term.
    NotFound(String),
    /// When there is a conflict storing a new task.
    Conflict(String),
    /// When task endpoint was called without correct credentials
    Unauthorized(String),
}

/// Task endpoint error responses
#[derive(Serialize, Deserialize, Clone, Component)]
pub(super) struct Failed {
    /// Error code
    #[component(example = 0, value_type = i32)]
    code: Code,
    /// Error reason
    message: String,
}

/// List tasks in current.
///
/// One could call the api endpoint with following curl.
///
/// ```shell
/// curl localhost:6040/tasks
/// ```
#[utoipa::path(
    responses(
        (status = 200, description = "List current task items", body = [Task])
    )
)]
#[get("/tasks")]
pub(super) async fn get_tasks(task_store: Data<TaskController>) -> impl Responder {
    match task_store.tasks().await {
        Ok(tasks) => HttpResponse::Ok().json(tasks),
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
    request_body = NewTask,
    responses(
        (status = 201, description = "Task created successfully", body = Task),
        // (status = 409, description = "Task with id already exists", body = ErrorResponse, example = json!(ErrorResponse::Conflict(String::from("id = 1"))))
    )
)]
#[post("/tasks")]
pub(super) async fn create_task(
    task: Json<NewTask>,
    task_store: Data<TaskController>,
) -> actix_web::Result<impl Responder> {
    // let mut conn = task_store.pool.acquire().await.unwrap();
    // conn.lock_handle().await.unwrap();
    let task = task.into_inner();
    let task = task_store.create(task).await.unwrap();
    Ok(HttpResponse::Created().json(task))
}

#[derive(Serialize, Deserialize, Component, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub(super) enum FromOrTo {
    From(String),
    To(String),
}

#[derive(Serialize, Deserialize, Component, Clone, Debug)]

pub(super) struct NewReplicate {
    /// Cluster username
    #[component(example = "root")]
    username: String,
    /// Cluster password
    #[component(example = "taosdata")]
    password: String,
    /// Source or target database name(or database topic name as data source).
    #[component(example = "test2")]
    database: String,
    /// Replicate database from another TDengine data source to this.
    #[component(example = "use from or to")]
    from: Option<String>,
    /// Replicate database to another TDengine.
    to: Option<String>,
    /// Override if database if not matched.
    #[serde(default)]
    force: bool,
}

impl NewReplicate {
    pub(super) fn into_task(self) -> Result<NewTask, anyhow::Error> {
        let Self {
            database,
            from,
            to,
            force,
            username,
            password,
        } = self;
        let (from, to) = match (from, to) {
            (Some(f), None) => (f, format!("taos://{username}:{password}@/{database}")),
            (None, Some(t)) => (format!("tmq:///{username}:{password}@/{database}"), t),
            (None, None) => anyhow::bail!("from or to field should be exist"),
            (Some(_), Some(_)) => anyhow::bail!("from is conflict with to"),
        };
        Ok(NewTask {
            stream_type: StreamType::Replicate,
            from,
            to,
            force,
            jobs: 0,
            compression_level: None,
        })
    }
}

/// Create new replication task.
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
    request_body = NewReplicate,
    responses(
        (status = 201, description = "Task created successfully", body = Task),
        // (status = 409, description = "Task with id already exists", body = ErrorResponse, example = json!(ErrorResponse::Conflict(String::from("id = 1"))))
    )
)]
#[post("/tasks/replicate")]
pub(super) async fn replicate(
    task: Json<NewReplicate>,
    task_store: Data<TaskController>,
) -> actix_web::Result<impl Responder> {
    // let mut conn = task_store.pool.acquire().await.unwrap();
    // conn.lock_handle().await.unwrap();
    let task = task.into_inner();
    match task.into_task() {
        Ok(task) => {
            let task = task_store.create(task).await;
            match task {
                Ok(task) => Ok(HttpResponse::Created().json(task)),
                Err(err) => Ok(HttpResponse::BadRequest().json(Failed {
                    code: Code::Failed,
                    message: err.to_string(),
                })),
            }
        }
        Err(err) => Ok(HttpResponse::BadRequest().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        })),
    }
}

#[derive(Serialize, Deserialize, Component, Clone, Debug)]

pub(super) struct Cluster {
    #[component(example = false)]
    #[serde(default)]
    websocket: bool,
    #[component(example = "root")]
    username: Option<String>,
    #[component(example = "taosdata")]
    password: Option<String>,
    #[component(example = "")]
    #[serde(default)]
    address: Option<String>,
    #[component(example = "test")]
    database: Option<String>,
}

impl Cluster {
    fn into_dsn(self) -> Dsn {
        let Self {
            websocket,
            username,
            password,
            address,
            database,
        } = self;
        Dsn {
            username: username,
            driver: "taos".to_string(),
            protocol: if websocket {
                Some("ws".to_string())
            } else {
                None
            },
            password: password,
            addresses: address
                .and_then(|s| s.parse().ok())
                .map(|addr| vec![addr])
                .unwrap_or_default(),
            fragment: None,
            database: database,
            params: Default::default(),
        }
    }
}

pub(super) struct SubscriptionSource {
    dsn: String,
    group_id: String,
    client_id: Option<String>,
    is_stable: bool,
    auto_created: bool,
}
#[derive(Serialize, Deserialize, Component, Clone, Debug)]
pub(super) struct NewSubscribe {
    /// Data source DSN
    #[component(example = "tmq://root:taosdata@localhost:6030/demo_meters?group.id=taosx")]
    from: String,
    #[component(example = r#"{"database":"test2"}"#)]
    /// Target cluster information.
    to: Cluster,
}
impl NewSubscribe {
    pub(super) fn into_task(self) -> Result<NewTask, anyhow::Error> {
        let Self { from, to: cluster } = self;
        let to = format!("{}", cluster.into_dsn());
        Ok(NewTask {
            stream_type: StreamType::Subscribe,
            from,
            to,
            force: true,
            jobs: 0,
            compression_level: None,
        })
    }
}

/// Create new replication task.
///
/// Post a new `Task` in request body as json to store it. Api will return
/// created `Task` on success.
///
/// One could call the api with.
///
/// ```shell
/// curl localhost:8080/tasks/subscribe -d '{"username": "tmq:///test", "to": "local:test"}'
/// ```
#[utoipa::path(
    request_body = NewSubscribe,
    responses(
        (status = 201, description = "Task created successfully", body = Task),
        // (status = 409, description = "Task with id already exists", body = ErrorResponse, example = json!(ErrorResponse::Conflict(String::from("id = 1"))))
    )
)]
#[post("/tasks/subscribe")]
pub(super) async fn subscribe(
    task: Json<NewSubscribe>,
    task_store: Data<TaskController>,
) -> impl Responder {
    // let mut conn = task_store.pool.acquire().await.unwrap();
    // conn.lock_handle().await.unwrap();
    let task = task.into_inner();
    match task.into_task() {
        Ok(task) => {
            let task = task_store.create(task).await;
            match task {
                Ok(task) => HttpResponse::Created().json(task),
                Err(err) => HttpResponse::BadRequest().json(Failed {
                    code: Code::Failed,
                    message: err.to_string(),
                }),
            }
        }
        Err(err) => HttpResponse::BadRequest().json(Failed {
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
    responses(
        (status = 200, description = "Task deleted successfully"),
        // (status = 401, description = "Unauthorized to delete Task", body = ErrorResponse, example = json!(ErrorResponse::Unauthorized(String::from("missing api key")))),
        (status = 404, description = "Task not found by id", body = ErrorResponse, example = json!(ErrorResponse::NotFound(String::from("id = 1"))))
    ),
    params(
        ("id", description = "Unique storage id of Task")
    ),
)]
#[delete("/tasks/{id}")]
pub(super) async fn delete_task(id: Path<i64>, task_store: Data<TaskController>) -> impl Responder {
    match task_store.delete(id.into_inner()).await {
        Ok(Some(_)) => HttpResponse::Ok().finish(),
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
    responses(
        (status = 200, description = "Task found from storage", body = Task),
        (status = 404, description = "Task not found by id", body = Failed)
    ),
    params(
        ("id", description = "Unique storage id of Task")
    )
)]
#[get("/tasks/{id}")]
pub(super) async fn get_task_by_id(
    id: Path<i64>,
    task_store: Data<TaskController>,
) -> impl Responder {
    let id = id.into_inner();
    match task_store.get(id).await {
        Ok(Some(task)) => HttpResponse::Ok().json(task),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
    }
}
