use std::{
    fmt::{Debug, Display},
    hash::{DefaultHasher, Hash, Hasher},
    time::Duration,
};

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;
use taos::{
    AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder, TaosPool, TryStreamExt as _,
};
use utoipa::{IntoParams, ToResponse, ToSchema};

use super::{NewTask, Status, TaskDetail};

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

#[derive(
    Serialize,
    Deserialize,
    Default,
    Clone,
    IntoParams,
    ToSchema,
    ToResponse,
    FromRow,
    Debug,
    PartialEq,
)]
pub struct ReplicaOpts {
    /// The source cluster.
    pub source: String,
    /// The sink cluster.
    pub sink: String,
    /// Replica ID, if not set, it will be generated automatically.
    pub id: Option<String>,
    /// Task uuid.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jid: Option<String>,
    /// The topic prefix of the consumer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_prefix: Option<String>,
    /// The group of the consumer for replica task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// Whether to keep the topic after removing a replica task.
    pub keep_topic_after_remove: bool,
    /// The interval to check new databases, in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_databases_checking_interval: Option<u32>,
}

impl ReplicaOpts {
    pub fn build_replica(&self) -> anyhow::Result<Replica> {
        Replica::from_opts(self)
    }

    pub fn new_databases_checking_interval(&self) -> u32 {
        self.new_databases_checking_interval.unwrap_or(60 * 30) // 30 minutes by default.
    }
}

pub type ReplicaId = String;

fn endpoint_to_dsn(endpoint: &str) -> anyhow::Result<Dsn> {
    if endpoint.starts_with("http://")
        || endpoint.starts_with("https://")
        || endpoint.starts_with("ws://")
        || endpoint.starts_with("wss://")
    {
        Ok(format!("taos+{}", endpoint).parse()?)
    } else if endpoint.starts_with("taos://")
        || endpoint.starts_with("tmq://")
        || endpoint.starts_with("taos+ws://")
        || endpoint.starts_with("tmq+ws://")
        || endpoint.starts_with("taos+wss://")
        || endpoint.starts_with("tmq+wss://")
        || endpoint.starts_with("taos+http://")
        || endpoint.starts_with("tmq+http://")
        || endpoint.starts_with("taos+https://")
        || endpoint.starts_with("tmq+https://")
    {
        Ok(endpoint.parse()?)
    } else if endpoint.contains("://") {
        bail!("Invalid endpoint: {endpoint}")
    } else {
        Ok(format!("taos://{}", endpoint).parse()?)
    }
}

fn canonical_endpoint(dsn: &Dsn) -> anyhow::Result<String> {
    let dsn = dsn.to_owned();

    match dsn.protocol.as_deref() {
        None => dsn
            .addresses
            .first()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Empty address in dsn: {dsn}")),
        Some(protocol) => {
            let protocol = match protocol {
                "http" | "ws" => "http",
                "https" | "wss" => "https",
                _ => {
                    bail!("unknown protocol:{}", protocol);
                }
            };
            let addr = dsn
                .addresses
                .first()
                .map(|s| s.to_string())
                .unwrap_or_default();
            if let Some(token) = dsn.get("token") {
                Ok(format!("{}://{}?token={}", protocol, addr, token))
            } else {
                Ok(format!("{}://{}", protocol, addr))
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

        let repr = canonical_endpoint(&dsn)?;
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Replica {
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
    fn from_opts(opts: &ReplicaOpts) -> anyhow::Result<Self> {
        if let Some(id) = opts.id.as_ref() {
            Self::new(id, &opts.source, &opts.sink)
        } else {
            Self::prepare(&opts.source, &opts.sink)
        }
    }
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

    /// Replica id. It's a unique identifier for a pair of source and sink.
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn update_id_with(&mut self, replicas: &[Replica]) {
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

    pub fn canonical_source(&self) -> &str {
        self.source.as_canonical_str()
    }

    pub fn canonical_sink(&self) -> &str {
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

    pub fn labels_with_source_sink(&self) -> String {
        format!(
            "type::replica,{}::{},{}::{}",
            REPLICA_LABEL_SOURCE, self.source, REPLICA_LABEL_SINK, self.sink
        )
    }

    #[allow(dead_code)] // FIXME: remove dead code?
    pub fn labels_with_id(&self) -> String {
        format!("type::replica,{}::{}", REPLICA_LABEL_ID, self.id,)
    }

    /// Get databases to replicate from source.
    pub async fn source_databases(&self, timeout: Duration) -> anyhow::Result<Vec<String>> {
        let source = self.source_pool();
        tracing::debug!(
            source = self.canonical_source(),
            sink = self.canonical_sink(),
            ?timeout,
            "Fetching databases from source and sink"
        );

        let source_conn = tokio::time::timeout(timeout, source.get())
            .await
            .inspect_err(|_| {
                tracing::error!("Source connection timeout: {}", self.canonical_source());
            })
            .with_context(|| format!("Source connection timeout: {}", self.canonical_source()))?
            .with_context(|| format!("Source connection error for {}", self.canonical_source()))?;

        source_conn
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
            .await
            .map_err(|err| {
                tracing::error!("Failed to fetch databases from source: {}", err);
                err.into()
            })
    }

    /// Get databases to replicate from source.
    pub async fn sink_databases(&self, timeout: Duration) -> anyhow::Result<Vec<String>> {
        let source = self.sink_pool();
        tracing::debug!(
            source = self.canonical_source(),
            sink = self.canonical_sink(),
            ?timeout,
            "Fetching databases from source and sink"
        );

        let sink_conn = tokio::time::timeout(timeout, source.get())
            .await
            .inspect_err(|_| {
                tracing::error!("Source connection timeout: {}", self.canonical_source());
            })
            .with_context(|| format!("Source connection timeout: {}", self.canonical_source()))?
            .with_context(|| format!("Source connection error for {}", self.canonical_source()))?;

        sink_conn
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
            .await
            .map_err(|err| {
                tracing::error!("Failed to fetch databases from source: {}", err);
                err.into()
            })
    }

    /// Databases ready for replication in both source and sink.
    pub async fn databases(&self, timeout: Duration) -> anyhow::Result<Vec<String>> {
        let source_databases = self.source_databases(timeout).await?;
        let sink_databases = self.sink_databases(timeout).await?;
        Ok(source_databases
            .into_iter()
            .filter(|db| sink_databases.contains(db))
            .collect())
    }

    pub fn build_task(&self, opts: &ReplicaOpts, source_db: &str, sink_db: &str) -> NewTask {
        let rid = self.id();
        let group = opts.group.as_deref();
        let topic_prefix = opts.topic_prefix.as_deref().unwrap_or(DEFAULT_TOPIC_PREFIX);
        let topic = format!("{}{}", topic_prefix, source_db);
        let source = self.source_dsn_for(source_db, &topic, group);
        let sink = self.sink_dsn_for(sink_db);
        let body = serde_json::json!({
            "name": format!("replica-{rid}-{source_db}"),
            "from": source.to_string(),
            "to": sink.to_string(),
            "labels": [
                format!("type::replica"),
                format!("{}::{}", REPLICA_LABEL_ID, rid),
                format!("{}::{}", REPLICA_LABEL_SOURCE, self.canonical_source()),
                format!("{}::{}", REPLICA_LABEL_SINK, self.canonical_sink()),
                format!("{}::{}", REPLICA_LABEL_DATABASE, source_db),
                format!("{}::{}", REPLICA_LABEL_TOPIC, topic),
                format!("{}::{}", REPLICA_LABEL_GROUP, group.unwrap_or(DEFAULT_REPLICA_GROUP)),
            ],
            "oneshot_topic": if opts.keep_topic_after_remove { None } else { Some(topic) },
        });
        serde_json::from_value(body).unwrap()
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ReplicaTask {
    rid: ReplicaId,
    tid: i64,
    name: String,
    source: String,
    sink: String,
    pub database: String,
    topic: String,
    group: String,
    status: Status,
    reason: Option<String>,
}

impl ReplicaTask {
    pub fn from_task(task: TaskDetail) -> anyhow::Result<Self> {
        let labels = task.labels.to_hash_map();

        let rid = labels[REPLICA_LABEL_ID];
        let source = labels[REPLICA_LABEL_SOURCE];
        let sink = labels[REPLICA_LABEL_SINK];
        let database = labels[REPLICA_LABEL_DATABASE];
        let group = labels
            .get(REPLICA_LABEL_GROUP)
            .copied()
            .unwrap_or(DEFAULT_REPLICA_GROUP);
        let topic = labels.get(REPLICA_LABEL_TOPIC).copied().unwrap_or(database);

        Ok(ReplicaTask {
            rid: rid.to_string(),
            tid: task.id,
            name: task.task.name.clone().unwrap_or_default(),
            source: source.to_owned(),
            sink: sink.to_owned(),
            database: database.to_owned(),
            group: group.to_string(),
            topic: topic.to_string(),
            status: task.task.status,
            reason: task.task.reason,
        })
    }
    /// Check if the replica task is in final state.
    #[allow(dead_code)] // FIXME: remove dead code?
    fn in_final_state(&self) -> bool {
        self.status.in_final_state()
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

    pub fn replica(&self) -> anyhow::Result<Replica> {
        Replica::new(self.rid.as_str(), self.source.as_str(), self.sink.as_str())
    }

    pub fn replica_id(&self) -> &str {
        &self.rid
    }

    /// Reason of error or failed status.
    #[allow(dead_code)] // FIXME: remove dead code?
    fn reason(&self) -> Option<&str> {
        match self.status.as_str() {
            "failed" | "suspending" | "stopping" | "stopped" | "suspended" | "interrupted" => {
                self.reason.as_deref()
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_with_taos() {
        let replica = Replica::new("id", "localhost:6030", "other:6030").unwrap();
        assert_eq!(replica.canonical_source(), "localhost:6030");
        assert_eq!(replica.canonical_sink(), "other:6030");
        assert_eq!(
            replica
                .source_dsn_for("db1", "top", Some("replica"))
                .to_string(),
            "tmq://localhost:6030/db1?group.id=replica&replica=&timeout=never&use.topic.name=top"
        );
        assert_eq!(
            replica.sink_dsn_for("db1").to_string(),
            "taos://other:6030/db1"
        );
        let replica = Replica::new("id", "taos://localhost:6030", "taos://other:6030").unwrap();
        assert_eq!(replica.canonical_source(), "localhost:6030");
        assert_eq!(replica.canonical_sink(), "other:6030");
        assert_eq!(
            replica
                .source_dsn_for("db1", "top", Some("replica"))
                .to_string(),
            "tmq://localhost:6030/db1?group.id=replica&replica=&timeout=never&use.topic.name=top"
        );
        assert_eq!(
            replica.sink_dsn_for("db1").to_string(),
            "taos://other:6030/db1"
        );
        let replica = Replica::new("id", "http://localhost:6041", "https://other:6041").unwrap();
        assert_eq!(replica.canonical_source(), "http://localhost:6041");
        assert_eq!(replica.canonical_sink(), "https://other:6041");
        assert_eq!(
            replica
                .source_dsn_for("db1", "top", Some("replica"))
                .to_string(),
            "tmq+http://localhost:6041/db1?group.id=replica&replica=&timeout=never&use.topic.name=top"
        );
        assert_eq!(
            replica.sink_dsn_for("db1").to_string(),
            "taos+https://other:6041/db1"
        );
        let replica = Replica::new("id", "ws://localhost:6041", "wss://other:6041").unwrap();
        assert_eq!(replica.canonical_source(), "http://localhost:6041");
        assert_eq!(replica.canonical_sink(), "https://other:6041");
        assert_eq!(
            replica
                .source_dsn_for("db1", "top", Some("replica"))
                .to_string(),
            "tmq+ws://localhost:6041/db1?group.id=replica&replica=&timeout=never&use.topic.name=top"
        );
        assert_eq!(
            replica.sink_dsn_for("db1").to_string(),
            "taos+wss://other:6041/db1"
        );
        let replica = Replica::new(
            "id",
            "taos+http://localhost:6041",
            "taos+https://other:6041",
        )
        .unwrap();
        assert_eq!(replica.canonical_source(), "http://localhost:6041");
        assert_eq!(replica.canonical_sink(), "https://other:6041");
        assert_eq!(
            replica
                .source_dsn_for("db1", "top", Some("replica"))
                .to_string(),
            "tmq+http://localhost:6041/db1?group.id=replica&replica=&timeout=never&use.topic.name=top"
        );
        assert_eq!(
            replica.sink_dsn_for("db1").to_string(),
            "taos+https://other:6041/db1"
        );
        let replica = Replica::new("id", "ww://localhost:6030", "taos://other:6030");
        assert!(replica.is_err());

        let replica =
            Replica::prepare("taos+http://localhost:6041", "taos+https://other:6041").unwrap();
        assert_eq!(replica.id(), "amazing");
    }
}
