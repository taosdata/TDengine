use std::{
    collections::hash_map::DefaultHasher,
    collections::HashMap,
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use futures_util::TryStreamExt;
use itertools::Itertools;
use serde::Deserialize;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, Taos, TaosBuilder, TaosPool};

use crate::build;

/// Replica ID type.
type ReplicaId = String;

/// Default replica group name.
const DEFAULT_REPLICA_GROUP: &str = "__replica__";
const DEFAULT_TOPIC_PREFIX: &str = "__replica__";

/// Label name for replica id.
const REPLICA_LABEL_ID: &str = "rid";
/// Label name for replica source endpoint.
const REPLICA_LABEL_SOURCE: &str = "source";
/// Label name for replica sink endpoint.
const REPLICA_LABEL_SINK: &str = "sink";
/// Label name for replica database.
const REPLICA_LABEL_DATABASE: &str = "database";
/// Label name for replica topic.
const REPLICA_LABEL_TOPIC: &str = "topic";
/// Label name for replica consumer group.
const REPLICA_LABEL_GROUP: &str = "group";

/// Active-StandBy replication management commands
#[derive(Debug, Args)]
pub struct Cli {
    #[clap(subcommand)]
    command: ReplicaCommands,

    /// taosX server endpoint
    #[clap(flatten)]
    config: ReplicaConfig,
}

#[derive(Debug, Args)]
pub struct SourceSink {
    /// The source endpoint to replicate from.
    #[clap(short = 'f', long)]
    source: String,
    /// The endpoint to replicate to.
    #[clap(short = 't', long)]
    sink: String,
}

// #[derive(Debug, Args)]
// pub struct ReplicaStart {
//     /// The source endpoint to replicate from.
//     #[clap(short = 'f', long)]
//     source: Option<String>,
//     /// The endpoint to replicate to.
//     #[clap(short = 't', long)]
//     sink: Option<String>,

//     /// The replica identity string.
//     ///
//     /// If not specified, the replica id will be generated automatically.
//     #[clap(short, long)]
//     id: Option<ReplicaId>,
//     /// The databases to replicate.
//     databases: Vec<String>,
// }

#[derive(Debug, Subcommand)]
pub enum ReplicaCommands {
    /// Show the replication status
    Status {
        /// Replica ID list in positional arguments.
        ids: Vec<ReplicaId>,
    },
    /// Check the difference in the replication subscriptions.
    Diff {
        /// The replica id.
        id: ReplicaId,
        /// The databases to check.
        databases: Vec<String>,
    },
    /// Start replication to the specified endpoint
    Start {
        #[clap(short = 'f', long)]
        source: Option<String>,
        /// The endpoint to replicate to.
        #[clap(short = 't', long)]
        sink: Option<String>,
        /// The replica identity string.
        ///
        /// If not specified, the replica id will be generated automatically.
        #[clap(short, long)]
        id: Option<ReplicaId>,
        /// The databases to replicate.
        databases: Vec<String>,

        /// Custom topic template for replication.
        ///
        /// Replica task will use `{database}` as the topic name by default.
        #[clap(long, default_value = DEFAULT_TOPIC_PREFIX, alias = "topic-prefix")]
        topic_prefix: Option<String>,

        /// Whether to keep topic or not when remove replication.
        ///
        /// By default, the topic will be removed when remove replication.
        #[clap(long)]
        keep_topic_after_remove: bool,

        /// Custom consumer group for replication.
        ///
        /// Replica task will use `__replica__` as the consumer group by default.
        ///
        /// If set, the consumer group will be used as the consumer group name.
        #[clap(long, alias = "group.id")]
        group: Option<String>,
    },
    /// Stop replication with the specified databases or not
    Stop {
        /// The replica id.
        id: ReplicaId,
        /// The databases to replicate.
        databases: Vec<String>,
    },
    /// Restart replication with the specified databases or not
    Restart {
        /// The replica id.
        id: ReplicaId,
        /// The databases to replicate.
        databases: Vec<String>,
    },

    /// Remove replication with the specified databases
    Remove {
        /// The replica id.
        id: ReplicaId,
        /// The databases to replicate.
        #[clap()]
        databases: Vec<String>,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Replica {
    id: ReplicaId,
    source: ReplicaEndpoint,
    sink: ReplicaEndpoint,
    // word index.
    word: Option<u64>,
}

impl Hash for Replica {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.source.to_string().hash(state);
        self.sink.to_string().hash(state);
    }
}

mod words {
    use lazy_static::lazy_static;

    lazy_static! {
        /// A 2048 words list for generating replica id.
        pub static ref WORDS: Vec<&'static str> = include_str!("words.txt").lines().collect();
    }
}

impl Replica {
    fn new(
        id: impl Into<ReplicaId>,
        source: impl AsRef<str>,
        sink: impl AsRef<str>,
    ) -> anyhow::Result<Self> {
        let id = id.into();
        let source = ReplicaEndpoint::new(source.as_ref(), EndpointDirection::Source)?;
        let sink = ReplicaEndpoint::new(sink.as_ref(), EndpointDirection::Sink)?;

        Ok(Self {
            id,
            source,
            sink,
            word: None,
        })
    }

    fn prepare(source: impl AsRef<str>, sink: impl AsRef<str>) -> anyhow::Result<Self> {
        let source = ReplicaEndpoint::new(source.as_ref(), EndpointDirection::Source)?;
        let sink = ReplicaEndpoint::new(sink.as_ref(), EndpointDirection::Sink)?;

        let mut hasher = DefaultHasher::new();
        source.as_canonical_str().hash(&mut hasher);
        sink.as_canonical_str().hash(&mut hasher);
        let hash = hasher.finish();
        let word = hash % words::WORDS.len() as u64;

        let id = words::WORDS[word as usize].to_string();

        Ok(Self {
            id,
            source,
            sink,
            word: Some(word),
        })
    }

    fn update_id_with(&mut self, replicas: &[Replica]) {
        assert!(self.word.is_some(), "use prepare to generate hash id first");
        loop {
            if let Some(replica) = replicas.iter().find(|r| r.id == self.id) {
                if replica.source == self.source && replica.sink == self.sink {
                    break;
                }
                tracing::info!("replica id {} already exists, try next", self.id);
                let word = (self.word.unwrap() + 1) % words::WORDS.len() as u64;
                self.id = words::WORDS[word as usize].to_string();
            } else {
                tracing::info!("use replica id: {}", self.id);
                break;
            }
        }
    }

    fn canonical_source(&self) -> &str {
        self.source.as_canonical_str()
    }

    fn canonical_sink(&self) -> &str {
        self.sink.as_canonical_str()
    }

    fn source_pool(&self) -> &taos::TaosPool {
        self.source.pool()
    }
    fn sink_pool(&self) -> &taos::TaosPool {
        self.sink.pool()
    }

    fn source_dsn_for(&self, database: &str, topic: &str, group: Option<&str>) -> taos::Dsn {
        let mut tmq = self.source.dsn().clone();
        tmq.subject.replace(database.to_string());
        tmq.set("replica", "");
        tmq.set("timeout", "never");
        if topic != database {
            tmq.set("use.topic.name", topic);
        }
        tmq.set("group.id", group.unwrap_or(DEFAULT_REPLICA_GROUP));
        tmq
    }

    fn sink_dsn_for(&self, database: &str) -> taos::Dsn {
        let mut sink = self.sink.dsn().clone();
        sink.subject.replace(database.to_string());
        sink
    }
}

#[derive(clap::Args, Debug)]
struct ReplicaConfig {
    /// The taosX server endpoint.
    ///
    /// Default to `http://localhost:6050`.
    #[clap(long, default_value = "http://localhost:6050", global = true)]
    server: String,

    /// Connection timeout in seconds.
    #[clap(long, default_value = "30", global = true)]
    timeouts: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct ReplicaTask {
    rid: ReplicaId,
    tid: i64,
    name: String,
    source: String,
    sink: String,
    database: String,
    topic: String,
    group: String,
    status: String,
    reason: Option<String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct ReplicaTaskInner {
    id: i64,
    name: String,
    from: String,
    to: String,
    labels: Vec<String>,
    status: String,
    reason: Option<String>,
}

impl ReplicaTask {
    /// Check if the replica task is in final state.
    fn in_final_state(&self) -> bool {
        self.status == "stopped"
            || self.status == "created"
            || self.status == "failed"
            || self.status == "completed"
            || self.status == "suspended"
    }

    /// Source database name of the replica task.
    #[allow(dead_code)] // FIXME: remove dead code?
    fn source_database(&self) -> &str {
        self.database
            .split_once('.')
            .map(|(source, _)| source)
            .unwrap_or(self.database.as_str())
    }

    /// Sink database name of the replica task.
    #[allow(dead_code)] // FIXME: remove dead code?
    fn sink_database(&self) -> &str {
        self.database
            .split_once('.')
            .map(|(_, sink)| sink)
            .unwrap_or(self.database.as_str())
    }

    /// Reason of error or failed status.
    fn reason(&self) -> Option<&str> {
        match self.status.as_str() {
            "failed" | "suspending" | "stopping" | "stopped" | "suspended" | "interrupted" => {
                self.reason.as_deref()
            }
            _ => None,
        }
    }
}

impl ReplicaTaskInner {
    fn parse(self) -> Result<(Replica, ReplicaTask)> {
        let labels: HashMap<_, _> = self
            .labels
            .iter()
            .flat_map(|s| s.split_once("::"))
            .collect();

        let rid = labels[REPLICA_LABEL_ID];
        let source = labels[REPLICA_LABEL_SOURCE];
        let sink = labels[REPLICA_LABEL_SINK];
        let database = labels[REPLICA_LABEL_DATABASE];
        let group = labels
            .get(REPLICA_LABEL_GROUP)
            .copied()
            .unwrap_or(DEFAULT_REPLICA_GROUP);
        let topic = labels.get(REPLICA_LABEL_TOPIC).copied().unwrap_or(database);

        Ok((
            Replica::new(rid, source, sink)?,
            ReplicaTask {
                rid: rid.to_string(),
                tid: self.id,
                name: self.name,
                source: source.to_owned(),
                sink: sink.to_owned(),
                database: database.to_owned(),
                group: group.to_string(),
                topic: topic.to_string(),
                status: self.status,
                reason: self.reason,
            },
        ))
    }
}
struct Diff {
    database: String,
    loc: String,
    vgroup_id: i64,
    current: i64,
    latest: i64,
    rows: Option<i64>,
}

#[derive(serde::Deserialize)]
struct Profile {
    version: String,
    build_time: String,
}
impl ReplicaConfig {
    async fn is_alive(&self) -> bool {
        reqwest::get(&format!("{}/profile", self.server))
            .await
            .is_ok()
    }

    async fn profile(&self) -> anyhow::Result<Profile> {
        let profile = reqwest::get(&format!("{}/profile", self.server))
            .await?
            .json::<Profile>()
            .await?;

        Ok(profile)
    }

    async fn assert_server_alive(&self) -> anyhow::Result<()> {
        if self.is_alive().await {
            return Ok(());
        }

        // start taosx with systemctl
        let status = tokio::process::Command::new("systemctl")
            .args(["start", &format!("{}x", build::CUS_PROMPT)])
            .status()
            .await?;
        if !status.success() {
            bail!("start {}x failed", build::CUS_PROMPT);
        }

        const MAX_PING: usize = 5;

        for _ in 0..MAX_PING {
            if self.is_alive().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        bail!("taosX started but seems not run correctly");
    }

    /// List the replica tasks by the replica id.
    async fn list_replicas_of(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<(Replica, Vec<ReplicaTask>)>> {
        let replicas = reqwest::get(&format!(
            "{}/tasks?labels=type::replica,{}::{}",
            self.server, REPLICA_LABEL_ID, id
        ))
        .await?
        .json::<Vec<ReplicaTaskInner>>()
        .await?;

        if replicas.is_empty() {
            return Ok(None);
        }

        let replicas = replicas
            .into_iter()
            .map(ReplicaTaskInner::parse)
            .try_collect::<_, Vec<_>, _>()?
            .into_iter()
            .into_group_map();

        assert!(
            replicas.len() == 1,
            "replica must be configured only one endpoint, but contains: {:?}",
            replicas.keys()
        );

        let replicas = replicas.into_iter().next();

        Ok(replicas)
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeouts)
    }

    async fn search_replicas_by_source_sink(
        &self,
        source: &str,
        sink: &str,
    ) -> anyhow::Result<Option<(Replica, Vec<ReplicaTask>)>> {
        let url = format!(
            "{}/tasks?labels=type::replica,{}::{},{}::{}",
            self.server, REPLICA_LABEL_SOURCE, source, REPLICA_LABEL_SINK, sink
        );
        tracing::debug!(source, sink, "search replicas by source sink: {}", url);
        let replicas = reqwest::get(&url)
            .await?
            .json::<Vec<ReplicaTaskInner>>()
            .await?;

        if replicas.is_empty() {
            return Ok(None);
        }

        let replicas = replicas
            .into_iter()
            .map(ReplicaTaskInner::parse)
            .try_collect::<_, Vec<_>, _>()?
            .into_iter()
            .into_group_map()
            .into_iter()
            .next();

        Ok(replicas)
    }

    async fn list_replicas(&self) -> anyhow::Result<Vec<(Replica, Vec<ReplicaTask>)>> {
        let replicas = reqwest::get(&format!("{}/tasks?labels=type::replica", self.server))
            .await?
            .json::<Vec<ReplicaTaskInner>>()
            .await?;

        if replicas.is_empty() {
            return Ok(vec![]);
        }

        let replicas = replicas
            .into_iter()
            .map(ReplicaTaskInner::parse)
            .try_collect::<_, Vec<_>, _>()?
            .into_iter()
            .into_group_map();
        let replicas = replicas.into_iter().collect_vec();

        Ok(replicas)
    }

    #[tracing::instrument(skip_all, fields(replica = replica.id.as_str(), source = replica.canonical_source(), sink = replica.canonical_sink()))]
    async fn start_replica(
        &self,
        replica: &Replica,
        tasks: &[ReplicaTask],
        databases: &[String],
        topic_prefix: Option<&str>,
        group: Option<&str>,
        keep_topic_after_remove: bool,
    ) -> anyhow::Result<()> {
        let source = replica.source_pool();
        let timeout = self.timeout();
        tracing::debug!(
            source = replica.canonical_source(),
            sink = replica.canonical_sink(),
            ?timeout,
            "start replication"
        );

        let source_conn = tokio::time::timeout(timeout, source.get())
            .await
            .inspect_err(|_| {
                tracing::error!("Source connection timeout: {}", replica.canonical_source());
            })
            .with_context(|| format!("Source connection timeout: {}", replica.canonical_source()))?
            .with_context(|| {
                format!("Source connection error for {}", replica.canonical_source())
            })?;

        let sink = replica.sink_pool();
        let sink_conn = tokio::time::timeout(timeout, sink.get())
            .await
            .with_context(|| format!("Sink connection timeout: {}", replica.canonical_sink()))?
            .with_context(|| format!("Sink connection error for {}", replica.canonical_sink()))?;

        let mut source_databases = vec![];
        if databases.is_empty() {
            source_databases = source_conn
                .query("show databases")
                .await?
                .deserialize::<String>()
                .try_filter(|db| {
                    futures::future::ready(
                        db != "information_schema"
                            && db != "performance_schema"
                            && db != "log"
                            && db != "audit",
                    )
                })
                .try_collect()
                .await?;
        }

        let databases = if databases.is_empty() {
            &source_databases
        } else {
            databases
        };

        let (mut created, mut started) = (0, 0);

        for database in databases {
            if let Some(replica) = tasks.iter().find(|task| task.database == *database) {
                println!(
                    "* replicating task `{}` already exists as task {}",
                    database, replica.tid
                );
                if replica.in_final_state() {
                    if let Err(err) = self.restart_once(replica).await {
                        println!(
                            "* restart task {}:{} failed: {}",
                            replica.tid, replica.database, err
                        );
                    } else {
                        started += 1;
                    }
                }
                continue;
            }
            let (source_db, sink_db);
            if source_databases.is_empty() {
                (source_db, sink_db) = database.split_once('.').unwrap_or((database, database));
                println!("* replicating database: `{}`", source_db);

                // check if database exists in source and sink when use custom databases.
                if !source_conn.database_exists(source_db).await? {
                    bail!(
                        "Database `{}` not exists in {}",
                        source_db,
                        replica.canonical_source()
                    );
                }
                if !sink_conn.database_exists(sink_db).await? {
                    bail!(
                        "Database `{}` not exists in {}",
                        sink_db,
                        replica.canonical_sink()
                    );
                }
            } else {
                (source_db, sink_db) = (database.as_str(), database.as_str());
            }
            let url = format!("{}/tasks", self.server);
            let topic = format!(
                "{}{}",
                topic_prefix.unwrap_or(DEFAULT_TOPIC_PREFIX),
                source_db
            );
            let source = replica.source_dsn_for(source_db, &topic, group);
            let sink = replica.sink_dsn_for(sink_db);
            let body = serde_json::json!({
                "name": format!("replica-{database}"),
                "from": source.to_string(),
                "to": sink.to_string(),
                "labels": [
                    format!("type::replica"),
                    format!("{}::{}", REPLICA_LABEL_ID, replica.id),
                    format!("{}::{}", REPLICA_LABEL_SOURCE, replica.canonical_source()),
                    format!("{}::{}", REPLICA_LABEL_SINK, replica.canonical_sink()),
                    format!("{}::{}", REPLICA_LABEL_DATABASE, database),
                    format!("{}::{}", REPLICA_LABEL_TOPIC, topic),
                    format!("{}::{}", REPLICA_LABEL_GROUP, group.unwrap_or(DEFAULT_REPLICA_GROUP)),
                ],
                "oneshot_topic": if keep_topic_after_remove { None } else { Some(topic) },
            });

            println!("  creating replica task for `{}`", database);
            let response = reqwest::Client::new()
                .post(&url)
                .json(&body)
                .send()
                .await
                .context("Creating replica task error")?;

            let status = response.status();
            if !status.is_success() {
                bail!("start replica error {}: {}", status, response.text().await?);
            }
            println!("  replicating database `{}` task created", database);
            created += 1;
        }

        match (created, started) {
            (0, 0) => {
                println!("no task created or started");
            }
            (created, 0) => {
                println!("replication `{}`: created {} task(s)", replica.id, created);
            }
            (0, started) => {
                println!(
                    "replication `{}`: started {} existing task(s)",
                    replica.id, started
                );
            }
            (created, started) => {
                println!(
                    "replication `{}`: created {} task(s), started {} exist task(s)",
                    replica.id, created, started
                );
            }
        }

        Ok(())
    }

    async fn stop_once(&self, replica: &ReplicaTask) -> anyhow::Result<()> {
        println!("* stopping task {}:{}", replica.tid, replica.database);
        let url = format!("{}/tasks/{}/stop", self.server, replica.tid);
        let response = reqwest::Client::new().post(&url).send().await?;
        if !response.status().is_success() {
            bail!(
                "stop replica task {} failed: `{}`",
                replica.tid,
                response.text().await?
            );
        }
        println!(
            "* task {}:{} has been stopped successfully",
            replica.tid, replica.database
        );
        Ok(())
    }
    async fn start_once(&self, replica: &ReplicaTask) -> anyhow::Result<()> {
        println!("* starting task {}:{}", replica.tid, replica.database);
        let url = format!("{}/tasks/{}/start", self.server, replica.tid);
        let response = reqwest::Client::new().post(&url).send().await?;
        if !response.status().is_success() {
            bail!(
                "start replica task {} failed: `{}`",
                replica.tid,
                response.text().await?
            );
        }
        println!(
            "* task {}:{} has been started successfully",
            replica.tid, replica.database
        );
        Ok(())
    }

    async fn restart_once(&self, task: &ReplicaTask) -> anyhow::Result<()> {
        if !task.in_final_state() {
            if let Err(err) = self.stop_once(task).await {
                println!(
                    "* stop task {}:{} failed: {}, try start it once",
                    task.tid, task.database, err
                );
            }
        }
        self.start_once(task).await?;
        Ok(())
    }

    async fn remove_once(&self, replica: &ReplicaTask) -> anyhow::Result<()> {
        println!("* remove task {}:{}", replica.tid, replica.database);
        let url = format!("{}/tasks/{}", self.server, replica.tid);
        let response = reqwest::Client::new().delete(&url).send().await?;
        if !response.status().is_success() {
            bail!(
                "remove replica task {} failed: `{}`",
                replica.tid,
                response.text().await?
            );
        }
        println!(
            "* task {}:{} has been removed successfully",
            replica.tid, replica.database
        );
        Ok(())
    }

    async fn check_diff_once(
        &self,
        replica: &Replica,
        task: &ReplicaTask,
        src: &Taos,
    ) -> anyhow::Result<Vec<Diff>> {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Subscription {
            topic_name: String,
            consumer_group: String,
            vgroup_id: i64,
            offset: String,
            rows: Option<i64>,
        }

        let src_sub = src
            .query(format!(
                "select * from information_schema.ins_subscriptions where topic_name = '{}' and consumer_group = '{}'",
                task.topic, task.group
            ))
            .await?
            .deserialize::<Subscription>()
            .try_collect::<Vec<_>>()
            .await?;
        let mut diffs = Vec::with_capacity(src_sub.len());
        for sub in src_sub {
            if sub.offset.starts_with("wal:") {
                let loc = sub.offset.split_once(':').unwrap().1;
                let (current, latest) = loc
                    .split_once("/")
                    .map(|(a, b)| {
                        (
                            a.parse::<i64>().expect("invalid wal offset"),
                            b.parse::<i64>().expect("invalid wal offset"),
                        )
                    })
                    .ok_or_else(|| anyhow!("invalid offset {}", sub.offset))?;
                diffs.push(Diff {
                    database: task.database.clone(),
                    loc: replica.canonical_source().to_string(),
                    vgroup_id: sub.vgroup_id,
                    current,
                    latest,
                    rows: sub.rows,
                });
            }
        }
        Ok(diffs)
    }
}

fn endpoint_to_dsn(endpoint: &str) -> anyhow::Result<Dsn> {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        Ok(format!("taos+{}", endpoint).parse()?)
    } else if endpoint.starts_with("taos://") || endpoint.starts_with("tmq://") {
        Ok(endpoint.parse()?)
    } else {
        Ok(format!("taos://{}", endpoint).parse()?)
    }
}

fn canonical_endpoint(dsn: &Dsn) -> String {
    let dsn = dsn.to_owned();

    match dsn.protocol.as_deref() {
        None => dsn
            .addresses
            .first()
            .map(|s| s.to_string())
            .unwrap_or_default(),
        Some(protocol) => {
            let protocol = match protocol {
                "http" | "ws" => "http",
                "https" | "wss" => "https",
                _ => {
                    panic!("unknown protocol:{}", protocol);
                }
            };
            let addr = dsn
                .addresses
                .first()
                .map(|s| s.to_string())
                .unwrap_or_default();
            if let Some(token) = dsn.get("token") {
                format!("{}://{}?token={}", protocol, addr, token)
            } else {
                format!("{}://{}", protocol, addr)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EndpointDirection {
    Source,
    Sink,
}

#[derive(Clone)]
struct ReplicaEndpoint {
    repr: String,
    dsn: Dsn,
    pool: TaosPool,
    direction: EndpointDirection,
}

impl PartialEq for ReplicaEndpoint {
    fn eq(&self, other: &Self) -> bool {
        self.repr == other.repr && self.direction == other.direction
    }
}
impl Eq for ReplicaEndpoint {}

impl ReplicaEndpoint {
    fn new(endpoint: &str, direction: EndpointDirection) -> anyhow::Result<Self> {
        let mut dsn = endpoint_to_dsn(endpoint)?;
        if matches!(direction, EndpointDirection::Sink) {
            dsn.driver = "taos".to_string();
        } else {
            dsn.driver = "tmq".to_string();
        }
        let pool = TaosBuilder::from_dsn(&dsn)?.pool()?;

        let repr = canonical_endpoint(&dsn);
        Ok(Self {
            repr,
            dsn,
            pool,
            direction,
        })
    }

    fn as_canonical_str(&self) -> &str {
        self.repr.as_str()
    }

    fn pool(&self) -> &TaosPool {
        &self.pool
    }

    fn dsn(&self) -> &Dsn {
        &self.dsn
    }
}

impl Debug for ReplicaEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaEndpoint")
            .field("repr", &self.repr)
            .field("...", &"...")
            .finish()
    }
}

impl Display for ReplicaEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.repr)
    }
}

impl Cli {
    #[tracing::instrument(skip_all, name = "replica")]
    pub async fn run(self, opt_args: super::OptArgs) -> Result<()> {
        let config = &self.config;
        config.assert_server_alive().await?;
        tracing::info!("{}x server is alive", build::CUS_PROMPT);
        let profile = config.profile().await?;
        tracing::debug!(
            "{}x version: {} built {}",
            build::CUS_PROMPT,
            profile.version,
            profile.build_time
        );
        match self.command {
            ReplicaCommands::Start {
                source,
                sink,
                id,
                databases,
                topic_prefix,
                group,
                keep_topic_after_remove,
            } => {
                let (replica, tasks) = match (source.zip(sink), id) {
                    (Some((source, sink)), Some(id)) => {
                        let replica = Replica::new(&id, source, sink)?;
                        if let Some((replica, tasks)) = config
                            .search_replicas_by_source_sink(
                                replica.canonical_source(),
                                replica.canonical_sink(),
                            )
                            .await?
                        {
                            if id != replica.id {
                                bail!("replica id {} already exists as id {}, please remove the old one or use correct id", id, replica.id);
                            }
                            if databases.is_empty() {
                                println!(
                                    "replica id {} already exists, try to cover all databases",
                                    id
                                );
                            } else {
                                println!("replica id {} already exists, try to cover specified databases", id);
                            }
                            (replica, tasks)
                        } else {
                            (replica, vec![])
                        }
                    }
                    (Some((source, sink)), None) => {
                        let mut replica = Replica::prepare(source, sink)?;
                        if let Some((replica, tasks)) = config
                            .search_replicas_by_source_sink(
                                replica.canonical_source(),
                                replica.canonical_sink(),
                            )
                            .await?
                        {
                            (replica, tasks)
                        } else {
                            // Generate a random word from source and sink if id not set.
                            let replicas = config
                                .list_replicas()
                                .await?
                                .into_iter()
                                .map(|(r, _)| r)
                                .collect_vec();
                            replica.update_id_with(&replicas);
                            (replica, vec![])
                        }
                    }
                    (None, Some(id)) => {
                        println!("start replication for id {}", id);
                        config.list_replicas_of(&id).await?.ok_or_else(|| {
                                                        anyhow!("replica id {} not found, try `replica start -f <source> -t <sink>` to create one", id)
                                                })?
                    }
                    (None, None) => {
                        bail!("source and sink must be provided")
                    }
                };

                config
                    .start_replica(
                        &replica,
                        &tasks,
                        &databases,
                        topic_prefix.as_deref(),
                        group.as_deref(),
                        keep_topic_after_remove,
                    )
                    .await
                    .inspect_err(|err| {
                        tracing::error!("start replication failed: {:#}", err);
                    })?;
            }
            ReplicaCommands::Status { ids: replica_ids } => {
                let mut replicas = Vec::new();
                if replica_ids.is_empty() {
                    replicas = config.list_replicas().await?;
                } else {
                    for id in replica_ids {
                        let replica = config.list_replicas_of(&id).await?.ok_or_else(|| {
                            anyhow!("no replicas endpoint found, try `replica start` to create one")
                        })?;
                        replicas.push(replica);
                    }
                }

                let mut table = prettytable::Table::new();
                table.set_titles(prettytable::row![
                    "id", "task", "source", "sink", "database", "topic", "group", "status", "note",
                ]);
                for (replica, tasks) in replicas {
                    for task in tasks {
                        // println!(
                        //     "{}\t{}\t{}\t{}",
                        //     task.tid, replica.id, task.database, task.status
                        // );
                        table.add_row(prettytable::row![
                            replica.id,
                            task.tid,
                            replica.canonical_source(),
                            replica.canonical_sink(),
                            task.database,
                            task.topic,
                            task.group,
                            task.status,
                            task.reason().unwrap_or("")
                        ]);
                    }
                }
                table.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                table.printstd();
            }

            ReplicaCommands::Stop { id, databases } => {
                tracing::info!("stopping replication");
                if databases.is_empty() {
                    let (replica, tasks) =
                        config.list_replicas_of(&id).await?.ok_or_else(|| {
                            anyhow!("no replicas endpoint found, try `replica start` to create one")
                        })?;
                    println!("stopping replication {}", replica.id);
                    println!("stopping replication databases: {:?}", databases);
                    for task in tasks {
                        config.stop_once(&task).await?;
                    }
                } else {
                    let (replica, tasks) =
                        config.list_replicas_of(&id).await?.ok_or_else(|| {
                            anyhow!("no replicas endpoint found, try `replica start` to create one")
                        })?;
                    println!("stopping replication {}", replica.id);
                    for task in tasks {
                        if databases.contains(&task.database) {
                            config.stop_once(&task).await?;
                        }
                    }
                    // config.stop().await?;
                }
            }
            ReplicaCommands::Remove { id, databases } => {
                let force = opt_args.yes_i_really_mean_it;
                tracing::info!("stopping replication");
                if databases.is_empty() {
                    let (replica, tasks) =
                        config.list_replicas_of(&id).await?.ok_or_else(|| {
                            anyhow!("no replicas endpoint found, try `replica start` to create one")
                        })?;
                    println!("removing replication {}", replica.id);
                    for task in tasks {
                        if !task.in_final_state() {
                            if force {
                                config.stop_once(&task).await?;
                            } else {
                                bail!("replica task {}:{} is not in final state, use -y/--yes-i-really-mean-it to force remove", task.tid, task.database);
                            }
                        }
                        config.remove_once(&task).await?;
                    }
                } else {
                    let (replica, tasks) =
                        config.list_replicas_of(&id).await?.ok_or_else(|| {
                            anyhow!("no replicas endpoint found, try `replica start` to create one")
                        })?;
                    println!("removing replication {}", replica.id);
                    println!("stopping replication for databases: {:?}", databases);
                    for task in tasks {
                        if databases.contains(&task.database) {
                            if !task.in_final_state() {
                                if force {
                                    config.stop_once(&task).await?;
                                } else {
                                    bail!("replica task {}:{} is not in final state, use -y/--yes-i-really-mean-it to force remove", task.tid, task.database);
                                }
                            }
                            config.remove_once(&task).await?;
                        }
                    }
                }
            }
            ReplicaCommands::Restart { id, databases } => {
                if databases.is_empty() {
                    let (replica, tasks) =
                        config.list_replicas_of(&id).await?.ok_or_else(|| {
                            anyhow!("no replicas endpoint found, try `replica start` to create one")
                        })?;
                    println!("restarting replication {}", replica.id);
                    println!("restarting replication for databases: {:?}", databases);
                    for task in tasks {
                        config.restart_once(&task).await?;
                        // if !task.ready_to_remove() {
                        //     config.stop_once(&task).await?;
                        // }
                        // config.start_once(&task).await?;
                    }
                } else {
                    let (replica, tasks) =
                        config.list_replicas_of(&id).await?.ok_or_else(|| {
                            anyhow!("no replicas endpoint found, try `replica start` to create one")
                        })?;
                    println!("restarting replication {}", replica.id);
                    for task in tasks {
                        if databases.contains(&task.database) {
                            config.restart_once(&task).await?;
                        }
                    }
                }
            }
            ReplicaCommands::Diff { id, databases } => {
                let (replica, tasks) = config.list_replicas_of(&id).await?.ok_or_else(|| {
                    anyhow!(
                        "no replicas found by id {}, try `replica start` to create one",
                        id
                    )
                })?;

                let src = replica.source_pool().get().await?;
                let mut table = prettytable::Table::new();
                table.set_titles(prettytable::row![
                    "replica",
                    "database",
                    "endpoint",
                    "vgroup_id",
                    "current",
                    "latest",
                    "diff",
                    "rows",
                ]);
                for task in tasks {
                    if databases.is_empty() || databases.contains(&task.database) {
                        let diffs = config.check_diff_once(&replica, &task, &src).await?;
                        for diff in diffs {
                            table.add_row(prettytable::row![
                                replica.id,
                                diff.database,
                                diff.loc,
                                diff.vgroup_id,
                                diff.current,
                                diff.latest,
                                diff.latest - diff.current,
                                diff.rows.unwrap_or(0)
                            ]);
                        }
                    }
                }
                table.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                table.printstd();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::mem::transmute;

    use rand::distributions::DistString;
    use taos::{AsAsyncConsumer, IsAsyncData, IsAsyncMeta, IsOffset, MessageSet};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test_replica_func() {
        std::env::set_var("RUST_LOG", "debug");
        use tracing_subscriber::EnvFilter;

        let filter = EnvFilter::from_default_env();
        let _ = tracing_subscriber::fmt::fmt()
            .with_env_filter(filter)
            .try_init();
        let taos_builder = taos::TaosBuilder::from_dsn("taos://localhost").unwrap();
        let taos = taos_builder.build().await.unwrap();

        taos.exec_many([
            "drop topic if exists rep1",
            "drop topic if exists rep2",
            "drop database if exists rep1",
            "drop database if exists rep2",
            "create database rep1",
            "create database rep2",
        ])
        .await
        .unwrap();
        taos.create_topic_as_database("rep1", "rep1").await.unwrap();
        taos.create_topic_as_database("rep2", "rep2").await.unwrap();

        let mut rng = rand::thread_rng();
        let group = rand::distributions::Alphanumeric.sample_string(&mut rng, 10);

        println!("Using group {}", group);
        let tmq = format!("tmq:///?msg.consume.excluded=1&group.id={group}&timeout=never");
        let tmq_builder = taos::TmqBuilder::from_dsn(tmq).unwrap();
        let mut rep1 = tmq_builder.build().await.unwrap();
        rep1.subscribe(["rep1".to_string()]).await.unwrap();
        let mut rep2 = tmq_builder.build().await.unwrap();
        rep2.subscribe(["rep2".to_string()]).await.unwrap();

        let replica_runner = |consumer: taos::Consumer, target: &'static str| async move {
            let taos = taos::TaosBuilder::from_dsn(&format!("taos:///{}", target))?
                .build()
                .await?;
            let mut stream = consumer.stream_with_timeout(taos::Timeout::from_millis(1000));
            let mut msgs = 0;
            while let Ok(msg) = stream.try_next().await {
                let (offset, message) = msg.unwrap();
                msgs += 1;
                println!("{}: {}", target, offset.vgroup_id());
                match message {
                    MessageSet::Data(data) => {
                        let raw = data.as_raw_data().await?;
                        taos.write_raw_meta(unsafe { &transmute(raw) }).await?;
                    }
                    MessageSet::Meta(data) => {
                        println!("{target}: {:?}", data.as_json_meta().await?);
                        let raw = data.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;
                    }
                    _ => (),
                }
                consumer.commit(offset).await?;
                if msgs > 10 {
                    println!("{}: 10 messages received", target);
                    break;
                }
            }
            anyhow::Ok(msgs)
        };

        println!("start replicas: rep1 and rep2");
        let rep1_handler = tokio::spawn(async move { replica_runner(rep1, "rep1").await });
        let rep2_handler = tokio::spawn(async move { replica_runner(rep2, "rep2").await });

        taos.exec("create table rep1.tb1 (ts timestamp, val int)")
            .await
            .unwrap();
        taos.exec("create table rep2.tb2 (ts timestamp, val int)")
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(40)).await;
        let num1 = rep1_handler.await.unwrap().unwrap();
        let num2 = rep2_handler.await.unwrap().unwrap();

        assert!(num1 == 1, "rep1 should have received 1 message");
        assert!(num2 == 1, "rep2 should have received 1 message");
    }
}
