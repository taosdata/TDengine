use std::{
    collections::HashMap,
    fmt::Display,
    time::{Duration, Instant},
};

use actix_web::{
    delete, get, patch, post,
    web::{Data, Json, Path, Query, ServiceConfig},
    HttpResponse, Responder,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::migrate::Migrator;
use sqlx::SqlitePool;
use std::str::FromStr;
use taos::{AsyncQueryable, Code, Dsn, TBuilder, TaosBuilder};
use taosx::TaskOpts;
use tokio::{runtime::Runtime, sync::RwLock};
use tokio_util::sync::CancellationToken;
use utoipa::*;

mod datetime_format {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    type Target = DateTime<Utc>;

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error> where S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &Target, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.to_rfc3339_opts(SecondsFormat::Millis, true));
        serializer.serialize_str(&s)
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<D>(D) -> Result<T, D::Error> where D: Deserializer
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Target, D::Error>
    where
        D: Deserializer<'de>,
    {
        Target::deserialize(deserializer)
    }
}

mod option_datetime_format {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    type Target = Option<DateTime<Utc>>;

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error> where S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(date) = date.as_ref() {
            let s = format!("{}", date.to_rfc3339_opts(SecondsFormat::Millis, true));
            serializer.serialize_str(&s)
        } else {
            serializer.serialize_none()
        }
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<D>(D) -> Result<T, D::Error> where D: Deserializer
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Target, D::Error>
    where
        D: Deserializer<'de>,
    {
        Target::deserialize(deserializer)
    }
}

use super::metrics::metrics_exporter;

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

// const TASK_SELECT: &str = "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks";

#[derive()]
pub(super) struct TaskController {
    pool: SqlitePool,
    runtime: Option<Runtime>,
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

impl Drop for TaskController {
    fn drop(&mut self) {
        if let Some(rt) = self.runtime.take() {
            // rt.block_on(self.clear()).unwrap();
            std::thread::spawn(move || {
                log::debug!("dropping tokio runtime in another thread");
                std::mem::drop(rt);
            })
            .join()
            .unwrap();
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
// #[derive(sqlx::Type)]
pub(super) enum Schedule {
    Oneshot,
    Repeated(String),
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Copy)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
pub(super) enum StreamType {
    Auto,
    Replicate,
    Backup,
    Restore,
    Subscribe,
    Export,
}

impl Default for StreamType {
    fn default() -> Self {
        StreamType::Auto
    }
}
impl Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamType::Auto => f.write_str("Auto"),
            StreamType::Replicate => f.write_str("Replicate"),
            StreamType::Backup => f.write_str("Backup"),
            StreamType::Restore => f.write_str("Restore"),
            StreamType::Subscribe => f.write_str("Subscribe"),
            StreamType::Export => f.write_str("Export"),
        }
    }
}

impl TaskController {
    pub async fn from_sqlite(sqlite: &str) -> anyhow::Result<Self> {
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(sqlite)?.create_if_missing(true);
        let pool = sqlx::SqlitePool::connect_with(options).await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self {
            pool,
            runtime: None,
            tasks: Default::default(),
        })
    }

    pub fn with_runtime(mut self, rt: tokio::runtime::Runtime) -> Self {
        self.runtime = Some(rt);
        self
    }

    async fn start_task(&self, task: &Task) -> anyhow::Result<()> {
        let id = task.id;
        if self.tasks.read().await.get(&id).is_some() {
            anyhow::bail!("task {id} is running");
        }

        let from = if let Some(topic) = task.oneshot_topic.as_deref() {
            let mut from: Dsn = task.from.parse()?;
            from.set("use.topic.name", topic);
            log::info!("Set task from: {from}");
            from
        } else {
            task.from.parse()?
        };

        let token = tokio_util::sync::CancellationToken::new();
        let cloned_token = token.clone();
        let opts = TaskOpts {
            transform: vec![],
            from,
            to: task.to.parse()?,
            jobs: task.jobs as _,
            compression_level: task.compression_level.map(Into::into),
            force: task.force,
            cancel: CancellationToken::new(),
        };

        let pool = self.pool.clone();
        let task_handler = async move {
            let now = Utc::now();
            let _ = sqlx::query!(
                "UPDATE tasks SET last_modified_at = ?, status = ? WHERE id = ?",
                now,
                Status::Started,
                id
            )
            .execute(&pool)
            .await?;
            tokio::select! {
                _ = cloned_token.cancelled() => {
                    opts.cancel();
                    log::debug!("cancel task {id}");
                    let now = Utc::now();
                    let status = Status::Cancelled;
                    let _ = sqlx::query!(
                        "UPDATE tasks SET finished_at = ?, status = ? WHERE id = ? AND status not in (?, ?, ?)",
                        now,
                        status,
                        id,
                        Status::Completed, Status::Stopped, Status::Failed
                    )
                    .execute(&pool)
                    .await?;
                }
                result = async {
                    if opts.from.driver == "tmq" && opts.from.get("timeout").map(|s| s == "never").unwrap_or(false) {
                        let mut restarts = 0;
                        let mut sleep = Duration::from_secs(2);
                        let mut last_restart_time = Instant::now();
                        loop {
                            let now = Utc::now();
                            let _ = sqlx::query!(
                                "UPDATE tasks SET last_modified_at = ?, status = ? WHERE id = ?",
                                now,
                                Status::Running,
                                id
                            )
                            .execute(&pool)
                            .await?;
                            if restarts > 0 {
                                log::info!("resume task {id} as {restarts} restarts");
                                last_restart_time = Instant::now();
                            } else {
                                log::info!("start task {id}");
                            }
                            let result = opts.run().await;
                            match result {
                                Ok(_) => {
                                    let now = Utc::now();
                                    let _ = sqlx::query!(
                                        "UPDATE tasks SET finished_at = ?, status = ? WHERE id = ?",
                                        now,
                                        Status::Interrupted,
                                        id
                                    )
                                    .execute(&pool)
                                    .await?;
                                }
                                Err(err) => {
                                    let err_string = err.to_string();

                                    match err_string.as_str() {
                                        e if e.contains("Unsupported HTTP method used - only GET is allowed") => {
                                            // todo(@huolinhe): we got 401 Authentication failure with HTTPS, but this error with HTTP.
                                            //   Maybe you should check the websocket implementations for the low-level reason.
                                            let err = "Authentication failure";
                                            log::error!("run task {id} failed with: {err}, please check the instance status or token");
                                            let err = err.to_string();
                                            let now = Utc::now();
                                            let _ = sqlx::query!(
                                                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ? AND deleted != TRUE",
                                                now,
                                                Status::Failed,
                                                err,
                                                id
                                            )
                                            .execute(&pool)
                                            .await?;
                                            break;
                                        }
                                        e if e.contains("WebSocket protocol error") || e.contains("WebSocket internal error") || e.contains("0x000B") => {
                                            log::warn!("run task {id} failed: {err}, wait for resume...");
                                            let err = err.to_string();
                                            let now = Utc::now();
                                            let _ = sqlx::query!(
                                                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ? AND deleted != TRUE AND status != ?",
                                                now,
                                                Status::Interrupted,
                                                err,
                                                id,
                                                Status::Failed
                                            )
                                            .execute(&pool)
                                            .await?;
                                        }
                                        _ => {
                                            log::error!("run task {id} failed with: {err}, please check the task information");
                                            let err = err.to_string();
                                            let now = Utc::now();
                                            let _ = sqlx::query!(
                                                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ? AND deleted != TRUE",
                                                now,
                                                Status::Failed,
                                                err,
                                                id
                                            )
                                            .execute(&pool)
                                            .await?;
                                            break;
                                        }
                                    }
                                }
                            }
                            log::info!("resume task {id} in {sleep:?}");
                            let running_elapsed = last_restart_time.elapsed();
                            if running_elapsed > sleep {
                                sleep = Duration::from_millis(500);
                            }
                            tokio::time::sleep(sleep).await;
                            if sleep < Duration::from_secs(60) {
                                sleep = sleep * 2;
                            }
                            restarts += 1;
                        }
                    } else {
                        let now = Utc::now();
                        let _ = sqlx::query!(
                            "UPDATE tasks SET last_modified_at = ?, status = ? WHERE id = ?",
                            now,
                            Status::Running,
                            id
                        )
                        .execute(&pool)
                        .await?;
                        let result = opts.run().await;
                        match result {
                            Ok(_) => {
                                let now = Utc::now();
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
                                let now = Utc::now();
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
                    return Ok::<(), anyhow::Error>(())
                } => {
                    let _ = result?;
                    log::info!("task {} done", id);
                }
            }
            Ok(())
        };
        let handle = if let Some(rt) = self.runtime.as_ref() {
            rt.spawn(task_handler)
        } else {
            tokio::spawn(task_handler)
        };
        self.tasks.write().await.insert(id, (handle, token));
        Ok(())
    }

    pub async fn tasks(&self, filter: TaskFilter) -> anyhow::Result<Vec<Task>> {
        let condition = filter.to_sql_conditions()?;
        let tasks = sqlx::query_as::<_, Task>(
            &format!("select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where {condition} order by created_at desc"),
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(tasks)
    }

    pub async fn tasks_count(&self, filter: TaskFilter) -> anyhow::Result<usize> {
        let condition = filter.to_sql_conditions()?;
        let tasks: i64 = sqlx::query_scalar(&format!(
            "select count(*) from tasks where {condition} order by created_at desc"
        ))
        .fetch_one(&self.pool)
        .await?;
        Ok(tasks as _)
    }

    pub async fn create(&self, task: NewTask) -> anyhow::Result<Task> {
        if let Some(topic) = task.oneshot_topic.as_deref() {
            if topic.len() > 64 {
                anyhow::bail!("Max length of topic name is 64, please rewrite the topic name");
            }
        }
        let res = sqlx::query(
            "INSERT INTO tasks (`from`, `from_cluster`, `oneshot_topic`, `to`, `to_cluster`, `stream_type`, `jobs`, `compression_level`, `force`, \
                 `created_at`, `status`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&task.from)
        .bind(&task.from_cluster)
        .bind(&task.oneshot_topic)
        .bind(&task.to)
        .bind(&task.to_cluster)
        .bind(&task.stream_type)
        .bind(&task.jobs)
        .bind(&task.compression_level)
        .bind(&task.force)
        .bind(&chrono::Utc::now().to_rfc3339())
        .bind(&Status::Created)
        .execute(&self.pool)
        .await
        .unwrap();

        if task.clear {
            let to: Dsn = task.to.parse()?;
            taosx::utils::clear_database(&to)
                .await
                .with_context(|| format!("Failed to clear target database with {to}"))?;
        }

        // let opts = taosx::TaskOpts::try_from(task.clone())?;
        let id = res.last_insert_rowid();
        let task = sqlx::query_as_unchecked!(Task, "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where id = ?", id)
            .fetch_one(&self.pool)
            .await
            .unwrap();

        self.start_task(&task).await?;
        Ok(task)
    }

    pub async fn update(&self, id: i64, task: UpdateTask) -> anyhow::Result<Option<Task>> {
        if let Some(topic) = task.oneshot_topic.as_deref() {
            if topic.len() > 64 {
                anyhow::bail!("Max length of topic name is 64, please rewrite the topic name");
            }
        }

        let mut sql = Vec::new();
        macro_rules! add_bind_sql {
            ($($field:ident )*) => {
                $(if task.$field.is_some() {
                    sql.push(concat!("`", stringify!($field), "` = ?"));
                })*
            };
        }
        add_bind_sql!(stream_type from from_cluster oneshot_topic to to_cluster jobs compression_level force);

        if sql.len() == 0 {
            let task = sqlx::query_as_unchecked!(Task, "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where id = ?", id)
            .fetch_one(&self.pool)
            .await
            .unwrap();

            self.start_task(&task).await?;
            return Ok(Some(task));
        }

        let query = format!("UPDATE `tasks` SET {} WHERE `id` = {}", sql.join(","), id);
        let mut query = sqlx::query(&query);

        macro_rules! bind_fields {
            ($($field:ident )*) => {
                $(if let Some(field) = task.$field.as_ref() {
                    query = query.bind(field);
                })*
            };
        }
        bind_fields!(stream_type from from_cluster oneshot_topic to to_cluster jobs compression_level force);

        let res = query.execute(&self.pool).await?;

        if res.rows_affected() == 1 {
            let task = sqlx::query_as_unchecked!(Task, "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where id = ?", id)
                .fetch_one(&self.pool)
                .await
                .unwrap();

            self.start_task(&task).await?;
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    pub async fn start_all(&self) -> anyhow::Result<usize> {
        let tasks = sqlx::query_as_unchecked!(
            Task,
            "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where status not in (?, ?, ?) and `deleted` != TRUE order by created_at desc",
            Status::Completed,
            Status::Failed,
            Status::Stopped,
        )
        .fetch_all(&self.pool)
        .await?;
        // Ok(tasks)
        let len = tasks.len();
        for task in tasks {
            self.start_task(&task).await?;
        }
        Ok(len)
    }

    pub async fn start(&self, id: i64) -> anyhow::Result<Option<()>> {
        let task = sqlx::query_as_unchecked!(Task, "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where id = ?", id)
        .fetch_optional(&self.pool)
        .await?;

        if task.is_none() {
            return Ok(None);
        }

        let task = task.unwrap();

        self.start_task(&task).await.map(Some)
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
                log::info!("Cancel task {id} before deleted");
                let _ = handle.await;
            }
        }
        let now = Utc::now();
        let res = sqlx::query_as_unchecked!(
            Task,
            "UPDATE tasks SET `deleted` = TRUE, `last_modified_at` = ? where id = ?",
            now,
            id
        )
        .execute(&self.pool)
        .await?;
        // dbg!(res);
        if res.rows_affected() == 1 {
            log::info!("successfully deleted task by id {id}");
        }

        let task = sqlx::query_as_unchecked!(Task, "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks where id = ?", id)
        .fetch_one(&self.pool)
        .await?;
        if let Some(topic) = task.oneshot_topic {
            let mut dsn: Dsn = task.from.parse()?;
            let _ = dsn.subject.take();
            let builder = TaosBuilder::from_dsn(dsn).context("cannot drop oneshot topic")?;
            let taos = builder.build().context("cannot drop oneshot topic")?;
            let mut retries = 0;
            loop {
                if retries > 20 {
                    log::error!("can not drop topic {topic}");
                    break;
                }
                if let Err(_err) = taos.exec(format!("drop topic if exists {topic}")).await {
                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                } else {
                    break;
                }
            }
        }
        Ok(Some(()))
    }
    pub async fn stop(&self, id: i64) -> anyhow::Result<Option<()>> {
        if let Some((handle, token)) = self.tasks.write().await.remove(&id) {
            log::error!("Cancel task by id {id}");
            token.cancel();
            let _ = handle.await?;
            // handle.abort();
            // if !handle.is_finished() {
            //     // token.cancel();
            //     log::error!("Cancel task by id {id}");
            //     let _ = handle.await;
            // }
        }
        let now = Utc::now();
        let res = sqlx::query_as_unchecked!(
            Task,
            "UPDATE tasks SET `last_modified_at` = ?, `status` = ? where id = ?",
            now,
            Status::Stopped,
            id
        )
        .execute(&self.pool)
        .await?;
        // dbg!(res);
        if res.rows_affected() == 1 {
            log::info!("successfully stop task by id {id}");
        }
        Ok(Some(()))
    }

    pub async fn stop_all(&self) -> anyhow::Result<()> {
        for (id, (handle, token)) in self.tasks.write().await.drain() {
            token.cancel();
            if !handle.is_finished() {
                // token.cancel();
                log::error!("Cancel task by id {id}");
                let _ = handle.await;
            }

            let now = Utc::now();
            let res = sqlx::query!(
                "UPDATE tasks SET finished_at = ?, status = ? WHERE id = ? AND status not in (?, ?, ?)",
                now,
                Status::Cancelled,
                id,
                Status::Completed, Status::Stopped, Status::Failed
            )
            .execute(&self.pool)
            .await?;
            // dbg!(res);
            if res.rows_affected() == 1 {
                log::info!("successfully cancelled task by id {id}");
            }
        }
        Ok(())
    }
    pub async fn _clear(&self) -> anyhow::Result<()> {
        for (id, (handle, token)) in self.tasks.write().await.drain() {
            token.cancel();
            if !handle.is_finished() {
                // token.cancel();
                log::error!("Cancel task by id {id}");
                let _ = handle.await;
            }

            let res = sqlx::query_as_unchecked!(
                Task,
                "UPDATE tasks SET `deleted` = TRUE where id = ?",
                id
            )
            .execute(&self.pool)
            .await?;
            // dbg!(res);
            if res.rows_affected() == 1 {
                log::info!("successfully deleted task by id {id}");
            }
        }
        Ok(())
    }
}

pub(super) fn configure(store: Data<TaskController>) -> impl FnOnce(&mut ServiceConfig) {
    |config: &mut ServiceConfig| {
        config
            .app_data(store)
            // .service(search_tasks)
            .service(get_tasks)
            .service(get_tasks_count)
            .service(create_task)
            .service(update_task)
            .service(delete_task)
            .service(replicate)
            .service(subscribe)
            .service(get_task_by_id)
            .service(start_task)
            .service(stop_task)
            .service(metrics_exporter)
            // .service(update_task)
            ;
    }
}

/// State.
///
/// Initial state: Created.
///
/// Final states:
/// - Completed: Oneshot task finished successfully.
/// - Failed: Oneshot task finished with some error.
/// - Stopped: Any task stopped by manual
///
/// ```
/// Created -> Running
/// Running -> Completed
/// Running -> Failed
/// Running -> Interrupted
/// Interrupted -> Running
/// Running -> Stopped
/// Running -> Cancelled
/// ```
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
pub(super) enum Status {
    /// Created by API.
    Created,
    /// Clear target database.
    Cleared,
    /// Started by start API.
    Started,
    /// In running state.
    Running,
    /// Cancelled tasks, this might be stopped or not.
    Cancelled,
    /// Task has been finished.
    Completed,
    /// Task completed with error.
    Failed,
    /// For never stop task, it's not in service, but will retry.
    Interrupted,
    /// Manually stopped by API.
    Stopped,
}

/// A streaming workflow task description.
#[derive(
    Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode, sqlx::Encode, sqlx::FromRow,
)]
pub(super) struct Task {
    /// Unique id for the task item.
    #[schema(read_only, example = 1)]
    id: i64,
    /// Task stream data type.
    #[schema(read_only, example = "backup")]
    stream_type: StreamType,
    /// The stream data source.
    #[schema(example = "tmq:///test")]
    from: String,

    /// Cluster identifier for stream from.
    #[schema(example = "null")]
    from_cluster: Option<String>,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    #[serde(default)]
    oneshot_topic: Option<String>,

    /// The target of the stream.
    #[schema(example = "local:/path/to/backup/test")]
    to: String,

    /// Cluster identifier for stream to.
    #[schema(example = "null")]
    to_cluster: Option<String>,

    /// Number of jobs for task running.
    #[schema(example = 0)]
    jobs: u16,

    /// Compression level when need (for backup only)
    compression_level: Option<u8>,

    /// Force for some risking steps.
    force: bool,

    /// Created time.
    #[schema(read_only)]
    #[serde(with = "datetime_format")]
    created_at: DateTime<Utc>,

    /// Stopped time.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "option_datetime_format")]
    finished_at: Option<DateTime<Utc>>,

    /// Last modified time.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "option_datetime_format")]
    last_modified_at: Option<DateTime<Utc>>,

    /// The current status of the tasks.
    #[schema(read_only, value_type = String)]
    status: Status,

    /// Status reason (only for status: failed).
    #[schema(read_only)]
    reason: Option<String>,

    /// Mark the task done as expected.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    completed: bool,
    /// Mark the task is cancelled or not.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    cancelled: bool,

    /// Mark the task deleted or not, deleted tasks will not be listed when query all.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    deleted: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(
    Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode, sqlx::Encode, sqlx::FromRow,
)]
pub(super) struct NewTask {
    #[schema(example = "backup")]
    #[serde(default)]
    stream_type: StreamType,
    /// The stream data source.
    #[schema(example = "tmq:///test")]
    from: String,
    /// The stream data source cluster id.
    #[schema(example = "")]
    from_cluster: Option<String>,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    // #[serde(default)]
    oneshot_topic: Option<String>,

    /// The target of the stream.
    #[schema(example = "local:/path/to/backup/test")]
    to: String,
    /// The stream data target cluster id.
    #[schema(example = "")]
    to_cluster: Option<String>,

    /// Set if the target database should be cleared before running task.
    #[schema(example = "false")]
    #[serde(default)]
    clear: bool,

    /// Jobs number
    #[schema(example = 0)]
    #[serde(default)]
    jobs: u16,
    #[serde(default)]
    compression_level: Option<u8>,
    #[serde(default)]
    force: bool,
}

#[derive(
    Serialize,
    Deserialize,
    ToSchema,
    Default,
    Clone,
    Debug,
    sqlx::Decode,
    sqlx::Encode,
    sqlx::FromRow,
)]
#[serde(default)]
pub(super) struct UpdateTask {
    stream_type: Option<StreamType>,
    /// The stream data source.
    from: Option<String>,
    /// The stream data source cluster id.
    from_cluster: Option<String>,
    /// Use oneshot topic for a task, delete the topic after task deleted.
    oneshot_topic: Option<String>,
    /// The target of the stream.
    to: Option<String>,
    /// The stream data target cluster id.
    to_cluster: Option<String>,
    /// Jobs number
    jobs: Option<u16>,
    compression_level: Option<u8>,
    force: Option<bool>,
}

/// Task endpoint error responses
#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub(super) struct Failed {
    /// Error code
    #[schema(example = 0, value_type = i32)]
    code: Code,
    /// Error reason
    message: String,
}

#[derive(Serialize, Deserialize, Default, Clone, IntoParams)]
#[serde(default)]
pub(super) struct TaskFilter {
    stream_type: Option<StreamType>,
    from_cluster: Option<String>,
    to_cluster: Option<String>,
    status: Option<String>,
    start_create_time: Option<String>,
    end_create_time: Option<String>,
    with_deleted: Option<bool>,
}

impl TaskFilter {
    fn to_sql_conditions(&self) -> std::result::Result<String, std::fmt::Error> {
        use std::fmt::Write;
        let mut sql = String::new();
        if !self.with_deleted.unwrap_or_default() {
            write!(sql, "`deleted` = FALSE")?;
        } else {
            write!(sql, "1 = 1")?;
        }
        if let Some(val) = self.stream_type {
            write!(sql, " AND `stream_type` = '{val}'")?;
        }
        if let Some(val) = self.status.as_ref() {
            write!(sql, " AND `status` = '{val}'")?;
        }
        if let Some(from_cluster) = self.from_cluster.as_deref() {
            write!(sql, " AND `from_cluster` = '{from_cluster}'")?;
        }
        if let Some(val) = self.to_cluster.as_deref() {
            write!(sql, " AND `to_cluster` = '{val}'")?;
        }
        if let Some(val) = self.start_create_time.as_deref() {
            write!(sql, " AND `created_at` >= '{val}'")?;
        }
        if let Some(val) = self.end_create_time.as_deref() {
            write!(sql, " AND `created_at` <= '{val}'")?;
        }
        Ok(sql)
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
    responses(
        (status = 200, description = "List current task items", body = [Task])
    ),
    params(
        TaskFilter,
    )
)]
#[get("/tasks")]
pub(super) async fn get_tasks(
    task_store: Data<TaskController>,
    filter: Query<TaskFilter>,
) -> impl Responder {
    match task_store.tasks(filter.into_inner()).await {
        Ok(tasks) => HttpResponse::Ok()
            .append_header(("Count", tasks.len()))
            .json(tasks),
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
    responses(
        (status = 200, description = "Tasks count (deleted tasks will not be included by default)", body = [usize])
    ),
    params(
        TaskFilter,
    )
)]
#[get("/tasks/count")]
pub(super) async fn get_tasks_count(
    task_store: Data<TaskController>,
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
) -> impl Responder {
    let task = task.into_inner();
    match task_store.create(task).await {
        Ok(task) => HttpResponse::Created().json(task),
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

impl NewReplicate {
    pub(super) fn into_task(self) -> Result<NewTask, anyhow::Error> {
        let Self {
            database,
            from,
            to,
            force,
            clear,
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
            oneshot_topic: None,
            to,
            force,
            jobs: 0,
            compression_level: None,
            from_cluster: None,
            to_cluster: None,
            clear,
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

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]

pub(super) struct Cluster {
    #[schema(example = false)]
    #[serde(default)]
    websocket: bool,
    #[schema(example = "root")]
    username: Option<String>,
    #[schema(example = "taosdata")]
    password: Option<String>,
    #[schema(example = "")]
    #[serde(default)]
    address: Option<String>,
    #[schema(example = "test")]
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
            path: None,
            subject: database,
            params: Default::default(),
        }
    }
}

// pub(super) struct SubscriptionSource {
//     dsn: String,
//     group_id: String,
//     client_id: Option<String>,
//     is_stable: bool,
//     auto_created: bool,
// }
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub(super) struct NewSubscribe {
    /// Data source DSN
    #[schema(example = "tmq://root:taosdata@localhost:6030/demo_meters?group.id=taosx")]
    from: String,
    #[schema(example = r#"{"database":"test2"}"#)]
    /// Target cluster information.
    to: Cluster,
    /// Set if the target database should be cleared before running task.
    #[schema(example = "false")]
    #[serde(default)]
    clear: bool,
}
impl NewSubscribe {
    pub(super) fn into_task(self) -> Result<NewTask, anyhow::Error> {
        let Self {
            from,
            to: cluster,
            clear,
        } = self;
        let to = format!("{}", cluster.into_dsn());
        Ok(NewTask {
            stream_type: StreamType::Subscribe,
            from,
            to,
            force: true,
            jobs: 0,
            compression_level: None,
            from_cluster: None,
            to_cluster: None,
            clear,
            oneshot_topic: None,
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
/// Update Task by given path variable id.
///
/// This endpoint needs `api_key` authentication in order to call. Api key can be found from README.md.
///
/// Api will delete task from shared in-memory storage by the provided id and return success 200.
/// If storage does not contain `Task` with given id 404 not found will be returned.
#[utoipa::path(
    request_body = UpdateTask,
    responses(
        (status = 200, description = "Task deleted successfully"),
        // (status = 401, description = "Unauthorized to delete Task", body = ErrorResponse, example = json!(ErrorResponse::Unauthorized(String::from("missing api key")))),
        (status = 404, description = "Task not found by id", body = Failed)
    ),
    params(
        ("id", description = "Unique storage id of Task")
    ),
)]
#[patch("/tasks/{id}")]
pub(super) async fn update_task(
    id: Path<i64>,
    task: Json<UpdateTask>,
    task_store: Data<TaskController>,
) -> impl Responder {
    match task_store.update(id.into_inner(), task.into_inner()).await {
        Ok(Some(_)) => HttpResponse::Ok().finish(),
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
    responses(
        (status = 200, description = "Task deleted successfully"),
        // (status = 401, description = "Unauthorized to delete Task", body = ErrorResponse, example = json!(ErrorResponse::Unauthorized(String::from("missing api key")))),
        (status = 404, description = "Task not found by id", body = Failed)
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

/// Start [Task] by given path variable id.
///
/// If storage does not contain `Task` with given id 404 not found will be returned.
#[utoipa::path(
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
pub(super) async fn start_task(id: Path<i64>, task_store: Data<TaskController>) -> impl Responder {
    let id = id.into_inner();
    match task_store.start(id).await {
        Ok(Some(_)) => HttpResponse::Ok().finish(),
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
pub(super) async fn stop_task(id: Path<i64>, task_store: Data<TaskController>) -> impl Responder {
    let id = id.into_inner();
    match task_store.stop(id).await {
        Ok(Some(_)) => HttpResponse::Ok().finish(),
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
