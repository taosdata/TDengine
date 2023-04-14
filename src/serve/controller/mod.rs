use std::collections::BTreeMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::{migrate::Migrator, sqlite::SqliteJournalMode, SqlitePool};
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use taosx_core::utils::port_pool::PortPool;
use taosx_core::TaskOpts;
use tokio::sync::OnceCell;
use tokio::{runtime::Runtime, sync::RwLock};
use tokio_cron_scheduler::{Job, JobScheduler};
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

use super::data_sources::DataSourceDefinition;

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

// const TASK_SELECT: &str = "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks";

pub(super) struct TaskController {
    pub pool: SqlitePool,
    pub runtime: Option<Runtime>,
    pub tasks: RwLock<
        HashMap<
            i64,
            (
                tokio::task::JoinHandle<Result<(), anyhow::Error>>,
                CancellationToken,
            ),
        >,
    >,
    pub scheduler: Arc<JobScheduler>,
    // tasks: Mutex<Vec<Task>>,
}

impl Debug for TaskController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskController")
            .field("pool", &self.pool)
            .field("runtime", &self.runtime)
            .field("tasks", &self.tasks)
            .field("scheduler", &"..")
            .finish()
    }
}

pub(super) async fn start_all_with_schedule(controller: Arc<TaskController>) -> anyhow::Result<()> {
    // log::info!("")
    let tasks: Vec<Task> = sqlx::query_as::<_, Task>(
            "select * from task_with_labels where status not in (?, ?, ?) and `deleted` != TRUE order by created_at desc")
            .bind(Status::Completed)
            .bind(Status::Failed)
            .bind(Status::Stopped)
            .fetch_all(&controller.pool)
            .await?;
    // Ok(tasks)
    for task in &tasks {
        controller.start_task(task).await?;
    }
    let tasks: Vec<Task> = sqlx::query_as::<_, Task>(
            "select * from task_with_labels where trigger is not null and status != ? and `deleted` != TRUE order by created_at desc")
            .bind(Status::Stopped)
            .fetch_all(&controller.pool)
            .await?;
    let sched = controller.scheduler.clone();
    for task in tasks {
        if let Some(trigger) = task.trigger.as_deref() {
            let schedule = trigger.trim_start_matches("schedule:");
            let id = task.id;
            let controller = controller.clone();
            match Job::new_async(schedule, move |uuid, mut l| {
                let controller = controller.clone();
                Box::pin(async move {
                    log::info!("waiting for next tick");
                    let next_tick = l.next_tick_for_job(uuid).await;
                    match next_tick {
                        Ok(Some(ts)) => {
                            log::info!("Next tick is {:?}", ts);
                            let _ = controller.start(id).await.unwrap();
                        }
                        _ => log::warn!("Could not get next tick"),
                    }
                })
            }) {
                Ok(job) => {
                    log::debug!("add cron job for task: {task:?}");
                    sched.add(job).await?;
                }
                Err(err) => {
                    log::error!("Scheduler task error: {err:?}, task:{task:?}");
                    Err(err).with_context(|| format!("Schedule task error, task:{:?}", task))?;
                }
            }
        }
    }
    Ok(())
}
pub(super) struct TaskControllerRef(Arc<TaskController>);

impl TaskControllerRef {
    pub async fn _start_all_with_schedule(&self) -> anyhow::Result<()> {
        let tasks: Vec<Task> = sqlx::query_as::<_, Task>(
            "select * from task_with_labels where status not in (?, ?, ?) and `deleted` != TRUE order by created_at desc")
            .bind(Status::Completed)
            .bind(Status::Failed)
            .bind(Status::Stopped)
            .fetch_all(&self.pool)
            .await?;
        // Ok(tasks)
        for task in &tasks {
            self.start_task(task).await?;
        }
        let tasks: Vec<Task> = sqlx::query_as::<_, Task>(
            "select * from task_with_labels where trigger is not null and status != ? and `deleted` != TRUE order by created_at desc")
            .bind(Status::Stopped)
            .fetch_all(&self.pool)
            .await?;
        // dbg!(&tasks);
        let sched = self.scheduler.clone();
        for task in tasks {
            // dbg!(&task.trigger);
            if let Some(trigger) = task.trigger.as_deref() {
                let schedule = trigger.trim_start_matches("schedule:");
                let id = task.id;
                let controller = self.0.clone();
                match Job::new_async(schedule, move |uuid, mut l| {
                    let controller = controller.clone();
                    Box::pin(async move {
                        log::info!("waiting for next tick");
                        let next_tick = l.next_tick_for_job(uuid).await;
                        match next_tick {
                            Ok(Some(ts)) => {
                                log::info!("Next tick is {:?}", ts);
                                let _ = controller.start(id).await.unwrap();
                            }
                            _ => log::warn!("Could not get next tick"),
                        }
                    })
                }) {
                    Ok(job) => {
                        log::debug!("add cron job for task: {task:?}");
                        sched.add(job).await?;
                    }
                    Err(err) => {
                        log::error!("Scheduler task error: {err:?}, task:{task:?}");
                        Err(err)
                            .with_context(|| format!("Schedule task error, task:{:?}", task))?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl std::ops::Deref for TaskControllerRef {
    type Target = Arc<TaskController>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<TaskController> for TaskControllerRef {
    fn from(value: TaskController) -> Self {
        Self(Arc::new(value))
    }
}
impl From<Arc<TaskController>> for TaskControllerRef {
    fn from(value: Arc<TaskController>) -> Self {
        Self(value)
    }
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
        if let Some(sched) = Arc::get_mut(&mut self.scheduler) {
            let _ = sched.shutdown();
        };
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

// #[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Copy)]
// #[serde(rename_all = "snake_case")]
// #[derive(sqlx::Type)]
// pub(super) enum StreamType {
//     Auto,
//     Replicate,
//     Backup,
//     Restore,
//     Subscribe,
//     Export,
// }

// impl StreamType {
//     fn lowercase(&self) -> &'static str {
//         match self {
//             StreamType::Auto => "auto",
//             StreamType::Replicate => "replicate",
//             StreamType::Backup => "backup",
//             StreamType::Restore => "restore",
//             StreamType::Subscribe => "subscribe",
//             StreamType::Export => "export",
//         }
//     }
// }

// impl Default for StreamType {
//     fn default() -> Self {
//         StreamType::Auto
//     }
// }
// impl Display for StreamType {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             StreamType::Auto => f.write_str("Auto"),
//             StreamType::Replicate => f.write_str("Replicate"),
//             StreamType::Backup => f.write_str("Backup"),
//             StreamType::Restore => f.write_str("Restore"),
//             StreamType::Subscribe => f.write_str("Subscribe"),
//             StreamType::Export => f.write_str("Export"),
//         }
//     }
// }

// static ONCE: OnceCell<PortPool> = OnceCell::const_new();
static ONCE: OnceCell<PortPool> = OnceCell::const_new();

impl TaskController {
    pub async fn from_sqlite(sqlite: &str) -> anyhow::Result<Self> {
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(sqlite)?
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(30))
            .journal_mode(SqliteJournalMode::Wal);
        let pool = sqlx::SqlitePool::connect_with(options).await?;
        MIGRATOR.run(&pool).await?;
        let scheduler = JobScheduler::new().await?;
        scheduler.start().await?;
        Ok(Self {
            pool,
            runtime: None,
            tasks: Default::default(),
            scheduler: Arc::new(scheduler),
        })
    }

    pub fn with_runtime(mut self, rt: tokio::runtime::Runtime) -> Self {
        self.runtime = Some(rt);
        self
    }

    async fn start_task(&self, task: &Task) -> anyhow::Result<()> {
        let id = task.id;
        
        let mut remove_finished_task = false;
        {
            // for read guard lifetime
            if let Some(h) = self.tasks.read().await.get(&id) {
                if !h.0.is_finished() {
                    log::info!("try start task {id} but it is running");
                    return Ok(());
                } else {
                    remove_finished_task = true;
                }
            }
        }

        if remove_finished_task {
            // write guard lifetime.
            let mut guard = self.tasks.write().await;
            guard.remove(&id);
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
            // port_pool: ONCE,
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
                    match opts.from.driver.as_str() {
                        "opc" | "opcua" | "opcda" | "pi" => {
                            let _ = sqlx::query!(
                                "UPDATE tasks SET finished_at = ?, status = ? WHERE id = ? AND status not in (?, ?)",
                                now,
                                status,
                                id,
                                Status::Stopped, Status::Failed
                            )
                            .execute(&pool)
                            .await?;
                        },
                        _ => {
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
                    }

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
                            let result = opts.run(ONCE.get_or_init(|| async { PortPool::default() }).await).await;
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
                        let result = opts.run(ONCE.get_or_init(|| async { PortPool::default() }).await).await;
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

    pub async fn tasks(&self, mut filter: TaskFilter) -> anyhow::Result<Vec<TaskDetail>> {
        let condition = filter.to_sql_conditions()?;
        let mut tasks = sqlx::query_as::<_, Task>(&format!(
            "select * from task_with_labels where {condition} order by created_at desc"
        ))
        .fetch_all(&self.pool)
        .await
        .unwrap();

        if filter.has_labels_filter() {
            filter.filter_task_labels(&mut tasks);
        }

        tasks.iter_mut().for_each(|task| task.backport_labels());
        Ok(tasks.into_iter().map(TaskDetail::new).collect())
    }

    pub async fn tasks_count(&self, filter: TaskFilter) -> anyhow::Result<usize> {
        let tasks = self.tasks(filter).await?;
        Ok(tasks.len())
    }

    pub async fn create(&self, mut task: NewTask) -> anyhow::Result<TaskDetail> {
        if let Some(topic) = task.oneshot_topic.as_deref() {
            if topic.len() > 64 {
                anyhow::bail!("Max length of topic name is 64, please rewrite the topic name");
            }
        }

        if task.clear {
            let to: Dsn = task.to.parse()?;
            if to.driver == "taos" {
                taosx_core::utils::clear_database(&to)
                    .await
                    .with_context(|| format!("Failed to clear target database with {to}"))?;
            }
        }
        task.patch_labels();
        let res = sqlx::query(
            "INSERT INTO tasks (`name`, `from`, `oneshot_topic`, `to`, `jobs`, `compression_level`, \
                 `created_at`, `status`, `after_delete`, `trigger`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&task.name)
        .bind(&task.from)
        .bind(&task.oneshot_topic)
        .bind(&task.to)
        .bind(&task.jobs)
        .bind(&task.compression_level)
        .bind(&chrono::Utc::now().to_rfc3339())
        .bind(&Status::Created)
        .bind(&task.after_delete)
        .bind(&task.trigger)
        .execute(&self.pool)
        .await?;
        let id = res.last_insert_rowid();

        if let Some(labels) = task.labels {
            let values = labels
                .iter()
                .map(|label| match label.split_once("::") {
                    Some((key, value)) => format!("({id}, '{key}', '{value}')"),
                    None => format!("({id}, '{label}', NULL)"),
                })
                .join(",");
            sqlx::query(&format!("INSERT INTO labels VALUES {values}"))
                .execute(&self.pool)
                .await?;
        }

        // let opts = taosx::TaskOpts::try_from(task.clone())?;
        let mut task = self.get(id).await?.unwrap();
        task.backport_labels();

        self.start_task(&task).await?;
        Ok(task.into())
    }

    pub async fn update(&self, id: i64, task: UpdateTask) -> anyhow::Result<Option<TaskDetail>> {
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
            let task = self.get(id).await?.unwrap();
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
            let task = self.get(id).await?.unwrap();

            self.start_task(&task.task).await?;
            Ok(Some(task.into()))
        } else {
            Ok(None)
        }
    }

    pub async fn start(&self, id: i64) -> anyhow::Result<Option<()>> {
        let task = self.get(id).await?;

        if task.is_none() {
            return Ok(None);
        }

        let task = task.unwrap();

        self.start_task(&task.task).await.map(Some)
    }

    pub async fn get(&self, id: i64) -> anyhow::Result<Option<TaskDetail>> {
        let task: Option<Task> = sqlx::query_as("select * from task_with_labels where id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(task
            .map(|mut t| {
                t.backport_labels();
                t
            })
            .map(Into::into))
    }

    pub async fn delete(&self, id: i64) -> anyhow::Result<Option<TaskDetail>> {
        {
            if let Some((handle, token)) = self.tasks.write().await.remove(&id) {
                token.cancel();
                if !handle.is_finished() {
                    // token.cancel();
                    log::info!("Cancel task {id} before deleted");
                    let _ = handle.await;
                }
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
        if res.rows_affected() == 1 {
            log::info!("successfully deleted task by id {id}");
        }

        let mut task: Task = sqlx::query_as("select * from task_with_labels where id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        task.backport_labels();
        if let Some(topic) = task.oneshot_topic.as_deref() {
            let mut dsn: Dsn = task.from.parse()?;
            let _ = dsn.subject.take();
            let builder = TaosBuilder::from_dsn(dsn).context("cannot drop oneshot topic")?;
            let taos = builder.build().await.context("cannot drop oneshot topic")?;
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

        if let Some(action) = task.after_delete.as_deref() {
            if task.to.starts_with("local") && action == "clear" {
                let dsn: Dsn = task.to.parse()?;
                // std::mem::drop(task);
                tokio::spawn(async move { taosx_core::utils::clear_local(&dsn).await });
            }
        }
        Ok(Some(task.into()))
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
pub struct Task {
    /// Unique id for the task item.
    #[schema(read_only, example = 1)]
    pub id: i64,

    /// Task stream data type. **Deprecated**, use labels instead.
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    stream_type: Option<String>,

    /// The stream data source.
    #[schema(example = "tmq:///test")]
    from: String,

    /// Cluster identifier for stream from. **Deprecated**, use labels instead.
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    from_cluster: Option<String>,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    #[serde(default)]
    oneshot_topic: Option<String>,

    /// The target of the stream.
    #[schema(example = "local:/path/to/backup/test")]
    to: String,

    /// Cluster identifier for stream to. **Deprecated**, use labels instead.
    #[schema(example = "null")]
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[sqlx(default)]
    to_cluster: Option<String>,

    /// Number of jobs for task running.
    #[schema(example = 0)]
    jobs: u16,

    /// Compression level when need (for backup only)
    compression_level: Option<u8>,

    /// Force for some risking steps.
    #[serde(skip_serializing)]
    #[sqlx(default)]
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
    completed: bool,
    /// Mark the task is cancelled or not.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    cancelled: bool,

    /// Mark the task deleted or not, deleted tasks will not be listed when query all.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    deleted: bool,

    /// Add after_delete hook action, the string would be action name, with or without some configuration.
    ///
    /// It will do nothing if the action is not supported by a specific task case.
    #[serde(skip_serializing_if = "Option::is_none")]
    after_delete: Option<String>,
    /// A task name.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(read_only, example = "null")]
    name: Option<String>,

    /// Task trigger events, default will be oneshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<String>,

    /// Labels for a task.
    ///
    /// You can use k-v style label such as `key::value` or key-only label `key`.
    ///
    /// You can filter tasks by some labels.
    #[serde(skip_serializing_if = "Labels::is_empty")]
    #[serde(default)]
    #[sqlx(try_from = "String", default)]
    // #[serde(deserialize_with = "labels_serde::deserialize")]
    labels: Labels,
}

lazy_static::lazy_static! {
    pub static ref DATA_SOURCE_DEFINITIONS_VEC: Vec<DataSourceDefinition> = {
        let mut def: Vec<DataSourceDefinition> = Vec::new();
        def.push(serde_yaml::from_str(include_str!("../data_sources/tmq.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/pi.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/opcua.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/opcda.yaml")).unwrap());
        def
    };
    /// This is an example for using doc comment attributes
    pub static ref DATA_SOURCE_DEFINITIONS: BTreeMap<String, DataSourceDefinition> = {
        let mut def: Vec<DataSourceDefinition> = Vec::new();
        def.push(serde_yaml::from_str(include_str!("../data_sources/tmq.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/pi.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/opc.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/opcua.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/opcda.yaml")).unwrap());
        def.into_iter().map(|ds| (ds.id.to_string(), ds)).collect()
    };
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct ExpandedDsn {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Option<String>>,
}

impl From<Dsn> for ExpandedDsn {
    fn from(value: Dsn) -> Self {
        let (host, port) = match value.addresses.into_iter().next() {
            Some(addr) => (addr.host, addr.port),
            None => (None, None),
        };
        Self {
            id: value.driver,
            protocol: value.protocol,
            path: value.path,
            host,
            port,
            username: value.username,
            password: value.password,
            subject: value.subject,
            params: value
                .params
                .into_iter()
                .map(|(k, v)| (k, if v.is_empty() { None } else { Some(v) }))
                .collect(),
        }
    }
}
/// A streaming workflow task description.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct TaskDetail {
    #[serde(flatten)]
    task: Task,

    /// Expanded DSN for source.
    from_expand: Option<ExpandedDsn>,
    /// Expanded DSN definition with values.
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    from_detail: Option<DataSourceDefinition>,

    /// Expanded DSN for sink.
    to_expand: Option<ExpandedDsn>,
    /// Expanded DSN definition with values.
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    to_detail: Option<DataSourceDefinition>,
}

impl From<Task> for TaskDetail {
    fn from(value: Task) -> Self {
        Self::new(value)
    }
}

impl std::ops::Deref for TaskDetail {
    type Target = Task;

    fn deref(&self) -> &Self::Target {
        &self.task
    }
}

impl std::ops::DerefMut for TaskDetail {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.task
    }
}
impl TaskDetail {
    pub fn new(task: Task) -> Self {
        TaskDetail {
            task,
            from_expand: None,
            from_detail: None,
            to_expand: None,
            to_detail: None,
        }
    }

    pub fn expand_detail(self) -> Self {
        let value = self.task;
        let from_dsn: Dsn = value.from.as_str().parse().unwrap();
        let to_dsn: Dsn = value.to.as_str().parse().unwrap();
        TaskDetail {
            from_expand: Some(ExpandedDsn::from(from_dsn.clone())),
            from_detail: DATA_SOURCE_DEFINITIONS
                .get(&from_dsn.driver)
                .map(|d| d.clone().values_from(from_dsn)),
            to_expand: Some(ExpandedDsn::from(to_dsn.clone())),
            to_detail: DATA_SOURCE_DEFINITIONS
                .get(&to_dsn.driver)
                .map(|d| d.clone().values_from(to_dsn)),
            task: value,
        }
    }

    pub fn expand(mut self) -> Self {
        let value = &self.task;
        let from_dsn: Dsn = value.from.as_str().parse().unwrap();
        let to_dsn: Dsn = value.to.as_str().parse().unwrap();
        self.from_expand = Some(from_dsn.into());
        self.to_expand = Some(to_dsn.into());
        self
    }

    pub fn _detail(mut self) -> Self {
        let value = &self.task;
        let from_dsn: Dsn = value.from.as_str().parse().unwrap();
        let to_dsn: Dsn = value.to.as_str().parse().unwrap();
        self.from_detail = DATA_SOURCE_DEFINITIONS
            .get(&from_dsn.driver)
            .map(|d| d.clone().values_from(from_dsn));
        self.to_detail = DATA_SOURCE_DEFINITIONS
            .get(&to_dsn.driver)
            .map(|d| d.clone().values_from(to_dsn));
        self
    }

    pub fn decorate(self, decorator: &TaskDecorator) -> Self {
        if decorator.detail.is_some() {
            self.expand_detail()
        } else if decorator.expand.unwrap_or_default() {
            self.expand()
        } else {
            self
        }
    }
}

impl Task {
    fn backport_labels(&mut self) {
        if let Some(labels) = self.labels.as_deref() {
            for label in labels {
                if label.starts_with("from_cluster") {
                    if let Some(value) = label.split_once("::") {
                        self.from_cluster = Some(value.1.to_string())
                    }
                } else if label.starts_with("to_cluster") {
                    if let Some(value) = label.split_once("::") {
                        self.to_cluster = Some(value.1.to_string())
                    }
                } else if label.starts_with("stream_type") {
                    if let Some(value) = label.split_once("::") {
                        self.stream_type = Some(value.1.to_string())
                    }
                }
            }
        }
    }
}
#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq, PartialOrd, ToSchema)]
pub struct Labels(Option<Vec<String>>);

impl Labels {
    /// Check if labels is empty.
    fn is_empty(&self) -> bool {
        self.0.as_ref().map(|v| v.is_empty()).unwrap_or(true)
    }
}

#[tokio::test]
async fn test_labels() {
    let db = sqlx::SqlitePool::connect("sqlite:./target/taosx.dev.db")
        .await
        .unwrap();

    #[derive(
        Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode, sqlx::Encode, sqlx::FromRow,
    )]
    struct JsonTest {
        #[sqlx(try_from = "String")]
        labels: Labels,
    }

    let json: JsonTest = sqlx::query_as("select labels from task_with_labels")
        .fetch_one(&db)
        .await
        .unwrap();
    dbg!(json);
}

impl TryFrom<String> for Labels {
    type Error = serde_json::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        serde_json::from_str(&value)
    }
}

impl std::ops::Deref for Labels {
    type Target = Option<Vec<String>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Labels {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Task {
    fn contains_label(&self, label: &str) -> bool {
        self.labels
            .as_deref()
            .map(|labels| labels.iter().any(|e| e == label))
            .unwrap_or(false)
    }

    fn contains_labels(&self, labels: &[impl AsRef<str>]) -> bool {
        labels
            .iter()
            .all(|label| self.contains_label(label.as_ref()))
    }

    fn contains_any_labels(&self, labels: &[impl AsRef<str>]) -> bool {
        labels
            .iter()
            .any(|label| self.contains_label(label.as_ref()))
    }
}

const fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(
    Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode, sqlx::Encode, sqlx::FromRow,
)]
pub(super) struct NewTask {
    stream_type: Option<String>,
    /// Task name.
    #[schema(example = "demo")]
    name: Option<String>,
    /// Task trigger events, default will be oneshot.
    ///
    /// For schedule trigger:
    ///
    /// - Run hourly/daily/weekly/monthly: "schedule:@daily"
    /// - Run with crontab schedule: "schedule:0 0 * * *", checkout https://crontab.guru/ for human-readable crontab.
    #[schema(example = "schedule:0 0 * * *")]
    pub trigger: Option<String>,
    /// The stream data source.
    #[schema(example = "tmq:///test")]
    from: String,
    /// The stream data source cluster id.
    from_cluster: Option<String>,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    // #[serde(default)]
    oneshot_topic: Option<String>,

    /// The target of the stream.
    #[schema(example = "local:/tmp/taosx/test")]
    to: String,
    /// The stream data target cluster id.
    to_cluster: Option<String>,

    /// Set if the target database should be cleared before running task.
    #[serde(default)]
    clear: bool,

    /// Jobs number
    #[serde(default)]
    jobs: u16,

    /// Compression level when need (for backup only)
    #[serde(default)]
    compression_level: Option<u8>,

    /// Force to do some risking steps.
    #[serde(default)]
    force: bool,

    /// Add after_delete hook action, the string would be action name, with or without some configuration.
    ///
    /// It will do nothing if the action is not supported by a specific task case.
    after_delete: Option<String>,

    /// Labels for a task.
    ///
    /// You can use k-v style label such as `key::value` or key-only label `key`.
    ///
    /// You can filter tasks by some labels.
    labels: Option<Vec<String>>,
}

impl NewTask {
    fn patch_labels(&mut self) {
        let mut labels = match self.labels.take() {
            Some(labels) => labels,
            None => {
                vec![]
            }
        };
        if let Some(value) = self.stream_type.as_ref() {
            labels.push(format!("stream_type::{}", value));
        }

        if let Some(value) = self.from_cluster.as_deref() {
            labels.push(format!("from_cluster::{value}"))
        }
        if let Some(value) = self.to_cluster.as_deref() {
            labels.push(format!("to_cluster::{value}"))
        }
        if labels.len() > 0 {
            self.labels = Some(labels)
        } else {
            self.labels = None
        }
    }
}

#[derive(
    Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode, sqlx::Encode, sqlx::FromRow,
)]
struct NewTaskV1 {
    /// The stream data source.
    #[schema(example = "tmq:///test")]
    from: String,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    // #[serde(default)]
    oneshot_topic: Option<String>,

    /// The target of the stream.
    #[schema(example = "local:/path/to/backup/test")]
    to: String,

    /// Set if the target database should be cleared before running task.
    #[schema(example = "false")]
    #[serde(default)]
    clear: bool,

    /// Jobs number
    #[schema(example = 0)]
    #[serde(default)]
    jobs: u16,

    /// Add after_delete hook action, the string would be action name, with or without some configuration.
    ///
    /// It will do nothing if the action is not supported by a specific task case.
    after_delete: Option<String>,

    /// Labels for a task.
    ///
    /// You can use k-v style label such as `key::value` or key-only label `key`.
    ///
    /// You can filter tasks by some labels.
    labels: Option<Vec<String>>,
}

impl From<NewTask> for NewTaskV1 {
    fn from(value: NewTask) -> Self {
        let mut labels = match value.labels {
            Some(labels) => labels,
            None => {
                vec![]
            }
        };
        if let Some(value) = value.stream_type {
            labels.push(format!("stream_type::{}", value));
        }
        if let Some(value) = value.from_cluster {
            labels.push(format!("from_cluster::{value}"))
        }
        if let Some(value) = value.to_cluster {
            labels.push(format!("from_cluster::{value}"))
        }
        Self {
            from: value.from,
            oneshot_topic: value.oneshot_topic,
            to: value.to,
            clear: value.clear,
            jobs: value.jobs,
            after_delete: value.after_delete,
            labels: Some(labels),
        }
    }
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
    /// Update trigger,
    trigger: Option<String>,
    stream_type: Option<String>,
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
    /// Labels for a task.
    labels: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Default, Clone, IntoParams)]
#[serde(default)]
pub(super) struct TaskFilter {
    name: Option<String>,
    stream_type: Option<String>,
    from_cluster: Option<String>,
    to_cluster: Option<String>,
    status: Option<String>,
    start_create_time: Option<String>,
    end_create_time: Option<String>,
    with_deleted: Option<bool>,
    labels: Option<String>,
    any_labels: Option<String>,
    without_labels: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Clone, IntoParams)]
#[serde(default)]
pub struct TaskDecorator {
    expand: Option<bool>,
    detail: Option<bool>,
}

impl TaskFilter {
    fn to_sql_conditions(&mut self) -> std::result::Result<String, std::fmt::Error> {
        use std::fmt::Write;
        let mut sql = String::new();
        if !self.with_deleted.unwrap_or_default() {
            write!(sql, "`deleted` = FALSE")?;
        } else {
            write!(sql, "1 = 1")?;
        }
        if let Some(val) = self.name.as_deref() {
            write!(sql, " AND `name` = '{val}'")?;
        }
        if let Some(val) = self.stream_type.as_deref() {
            // write!(sql, " AND `stream_type` = '{val}'")?;
            let val = format!("stream_type::{}", val);
            if let Some(labels) = self.labels.as_mut() {
                labels.push(',');
                labels.push_str(&val);
            } else {
                self.labels.replace(val);
            }
        }
        if let Some(val) = self.status.as_ref() {
            write!(sql, " AND `status` = '{val}'")?;
        }
        if let Some(val) = self.from_cluster.as_deref() {
            // write!(sql, " AND `from_cluster` = '{from_cluster}'")?;
            let val = format!("from_cluster::{}", val);
            if let Some(labels) = self.labels.as_mut() {
                labels.push(',');
                labels.push_str(&val);
            } else {
                self.labels.replace(val);
            }
        }
        if let Some(val) = self.to_cluster.as_deref() {
            // write!(sql, " AND `to_cluster` = '{val}'")?;
            let val = format!("to_cluster::{}", val);
            if let Some(labels) = self.labels.as_mut() {
                labels.push(',');
                labels.push_str(&val);
            } else {
                self.labels.replace(val);
            }
        }
        if let Some(val) = self.start_create_time.as_deref() {
            write!(sql, " AND `created_at` >= '{val}'")?;
        }
        if let Some(val) = self.end_create_time.as_deref() {
            write!(sql, " AND `created_at` <= '{val}'")?;
        }
        Ok(sql)
    }

    fn has_labels_filter(&self) -> bool {
        !(self.labels.is_none() && self.any_labels.is_none() && self.without_labels.is_none())
    }

    fn filter_task_labels(&self, tasks: &mut Vec<Task>) {
        if let Some(labels) = self.labels.as_deref() {
            tasks.retain(|task| task.contains_labels(&labels.split(",").collect_vec()));
        }
        if let Some(labels) = self.any_labels.as_deref() {
            tasks.retain(|task| task.contains_any_labels(&labels.split(",").collect_vec()));
        }
        if let Some(labels) = self.without_labels.as_deref() {
            // remove tasks contains any labels in `without_labels`.
            tasks.retain(|task| !task.contains_any_labels(&labels.split(",").collect_vec()));
        }
    }
}
