use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use futures_util::TryStreamExt;
use itertools::Itertools;
use reqwest::Url;
use serde::Deserialize;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Taos};

const DEFAULT_REPLICA_GROUP: &str = "replica";

/// Active-StandBy replication management commands
#[derive(Debug, Args)]
#[command(subcommand_negates_reqs = true)]
pub(super) struct Cli {
    #[clap(subcommand)]
    command: ReplicaCommands,
}

#[derive(Debug, Subcommand)]
pub enum ReplicaCommands {
    /// Show the replication status
    Status,
    /// Check the difference in the replication subscriptions.
    Diff {
        /// The databases to check.
        databases: Vec<String>,
    },
    /// Start replication to the specified endpoint
    Start {
        /// The endpoint to replicate to.
        #[clap(required = true)]
        endpoint: String,
        /// The databases to replicate.
        databases: Vec<String>,
    },
    /// Stop replication with the specified databases or not
    Stop {
        /// The databases to replicate.
        #[clap()]
        databases: Option<Vec<String>>,
    },
    /// Restart replication with the specified databases or not
    Restart {
        /// The databases to replicate.
        #[clap()]
        databases: Option<Vec<String>>,
    },

    /// Remove replication with the specified databases
    Remove {
        /// The databases to replicate.
        #[clap()]
        databases: Vec<String>,
    },
}

struct ReplicaConfig {
    server: String,
}

struct Replica {
    id: i64,
    #[allow(dead_code)]
    name: String,
    database: String,
    status: String,
}

struct Diff {
    database: String,
    loc: String,
    vgroup_id: i64,
    current: i64,
    latest: i64,
}

#[derive(serde::Deserialize)]
struct Profile {
    version: String,
    build_time: String,
}
impl ReplicaConfig {
    fn new(server: impl Into<String>) -> Self {
        Self {
            server: server.into(),
        }
    }

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
            .args(["start", "taosx"])
            .status()
            .await?;
        if !status.success() {
            bail!("start taosx failed");
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

    async fn list_replicas(&self) -> anyhow::Result<Option<(String, Vec<Replica>)>> {
        #[derive(Deserialize)]
        struct ReplicaTask {
            id: i64,
            name: String,
            labels: Vec<String>,
            status: String,
        }

        impl ReplicaTask {
            fn parse(self) -> (String, Replica) {
                let labels: HashMap<_, _> = self
                    .labels
                    .iter()
                    .flat_map(|s| s.split_once("::"))
                    .collect();

                let endpoint = labels["endpoint"];
                let database = labels["database"];

                (
                    endpoint.to_owned(),
                    Replica {
                        id: self.id,
                        name: self.name,
                        database: database.to_owned(),
                        status: self.status,
                    },
                )
            }
        }
        let replicas = reqwest::get(&format!("{}/tasks?labels=type::replica", self.server))
            .await?
            .json::<Vec<ReplicaTask>>()
            .await?;

        if replicas.is_empty() {
            return Ok(None);
        }

        let replicas = replicas
            .into_iter()
            .map(ReplicaTask::parse)
            .into_group_map();

        assert!(
            replicas.len() == 1,
            "replica must be configured only one endpoint, but contains: {:?}",
            replicas.keys()
        );

        let replicas = replicas.into_iter().next().unwrap();

        Ok(Some(replicas))
    }

    async fn start_replica(&self, endpoint: &str, databases: &[String]) -> anyhow::Result<()> {
        let exists: HashMap<_, _> = if let Some((ep, replicas)) = self.list_replicas().await? {
            anyhow::ensure!(
                endpoint == ep,
                "endpoint already been configured but not match, expected: {}, got: {}",
                endpoint,
                ep
            );
            replicas
                .into_iter()
                .map(|s| (s.database.clone(), s))
                .collect()
        } else {
            HashMap::new()
        };

        let taos = taos::TaosBuilder::from_dsn(&format!("taos://{}", endpoint))?
            .build()
            .await?;

        for database in databases {
            if let Some(replica) = exists.get(database.as_str()) {
                println!(
                    "* replicating task `{}` already exists as task {}",
                    database, replica.id
                );
                continue;
            }
            println!("* replicating database: `{}`", database);
            if !taos.database_exists(&database).await? {
                bail!("database `{}` not exists in {}", database, endpoint);
            }
            println!("  database `{}` exists", database);
            let url = format!("{}/tasks", self.server);
            let body = serde_json::json!({
                "name": format!("replica-{database}"),
                "from": format!("tmq:///{database}?replica&timeout=never&group.id={group}", group = DEFAULT_REPLICA_GROUP, database = database),
                "to": format!("taos://{endpoint}/{database}"),
                "labels": [
                    format!("type::replica"),
                    format!("endpoint::{endpoint}"),
                    format!("database::{database}")
                ]
            });

            println!("  creating replica task for `{}`", database);
            let response = reqwest::Client::new()
                .post(&url)
                .json(&body)
                .send()
                .await
                .context("creating replica task error")?;

            let status = response.status();
            if !status.is_success() {
                bail!("start replica error {}: {}", status, response.text().await?);
            }
            println!("  replicating database `{}` task created", database);
        }

        println!("replication started");
        Ok(())
    }

    async fn restart(&self) -> anyhow::Result<()> {
        let (_, replicas) = self
            .list_replicas()
            .await?
            .ok_or_else(|| anyhow!("no replicas endpoint found"))?;
        for replica in replicas {
            self.restart_once(&replica).await?;
        }
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        let (endpoint, replicas) = self
            .list_replicas()
            .await?
            .ok_or_else(|| anyhow!("no replicas endpoint found"))?;
        println!("stopping replication to {}", endpoint);
        for replica in replicas {
            self.stop_once(&replica).await?;
        }
        Ok(())
    }

    async fn stop_once(&self, replica: &Replica) -> anyhow::Result<()> {
        println!("* stopping task {}:{}", replica.id, replica.database);
        let url = format!("{}/tasks/{}/stop", self.server, replica.id);
        let response = reqwest::Client::new().post(&url).send().await?;
        if !response.status().is_success() {
            bail!(
                "stop replica task {} failed: `{}`",
                replica.id,
                response.text().await?
            );
        }
        println!(
            "* task {}:{} has been stopped successfully",
            replica.id, replica.database
        );
        Ok(())
    }
    async fn start_once(&self, replica: &Replica) -> anyhow::Result<()> {
        println!("* starting task {}:{}", replica.id, replica.database);
        let url = format!("{}/tasks/{}/start", self.server, replica.id);
        let response = reqwest::Client::new().post(&url).send().await?;
        if !response.status().is_success() {
            bail!(
                "start replica task {} failed: `{}`",
                replica.id,
                response.text().await?
            );
        }
        println!(
            "* task {}:{} has been started successfully",
            replica.id, replica.database
        );
        Ok(())
    }

    async fn restart_once(&self, replica: &Replica) -> anyhow::Result<()> {
        if let Err(err) = self.stop_once(replica).await {
            println!(
                "* stop task {}:{} failed: {}, try start it once",
                replica.id, replica.database, err
            );
        }
        self.start_once(replica).await?;
        Ok(())
    }

    async fn remove_once(&self, replica: &Replica) -> anyhow::Result<()> {
        println!("* remove task {}:{}", replica.id, replica.database);
        let url = format!("{}/tasks/{}", self.server, replica.id);
        let response = reqwest::Client::new().delete(&url).send().await?;
        if !response.status().is_success() {
            bail!(
                "remove replica task {} failed: `{}`",
                replica.id,
                response.text().await?
            );
        }
        println!(
            "* task {}:{} has been removed successfully",
            replica.id, replica.database
        );
        Ok(())
    }

    async fn check_diff_once(
        &self,
        endpoint: &str,
        replica: &Replica,
        src: &Taos,
        dst: &Taos,
    ) -> anyhow::Result<Vec<Diff>> {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Subscription {
            topic_name: String,
            consumer_group: String,
            vgroup_id: i64,
            offset: String,
            rows: i64,
        }
        let src_sub = src
            .query(format!(
                "select * from information_schema.ins_subscriptions where topic_name = '{}' and consumer_group = '{}'",
                replica.database, DEFAULT_REPLICA_GROUP
            ))
            .await?
            .deserialize::<Subscription>()
            .try_collect::<Vec<_>>()
            .await?;
        let mut diffs = Vec::with_capacity(src_sub.len() * 2);
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
                    database: replica.database.clone(),
                    loc: "localhost:6030".to_string(),
                    vgroup_id: sub.vgroup_id,
                    current,
                    latest,
                });
            }
        }

        if endpoint == "localhost:6030" {
            return Ok(diffs);
        }
        let dst_sub = dst
            .query(format!(
                "select * from information_schema.ins_subscriptions where topic_name = '{}' and consumer_group = '{}'",
                replica.database, DEFAULT_REPLICA_GROUP
            ))
            .await?
            .deserialize::<Subscription>()
            .try_collect::<Vec<_>>()
            .await.ok();

        if let Some(dst_sub) = dst_sub {
            for sub in dst_sub {
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
                        database: replica.database.clone(),
                        loc: endpoint.to_string(),
                        vgroup_id: sub.vgroup_id,
                        current,
                        latest,
                    });
                }
            }
        }
        Ok(diffs)
    }
}

impl Cli {
    #[tracing::instrument(skip_all, name = "replica")]
    pub(super) async fn run(self) -> Result<()> {
        let config = ReplicaConfig::new("http://localhost:6050");
        match self.command {
            ReplicaCommands::Status => {
                let profile = config.profile().await?;
                println!(
                    "taosx version: {} built {}",
                    profile.version, profile.build_time
                );
                let (endpoint, replicas) = config.list_replicas().await?.ok_or_else(|| {
                    anyhow!("no replicas endpoint found, try `replica start` to create one")
                })?;
                println!("replica endpoint: {}", endpoint);
                println!("databases number to be replicated: {}", replicas.len());

                for replica in replicas {
                    println!("* {}:{}\t{}", replica.id, replica.database, replica.status);
                }
            }
            ReplicaCommands::Diff { databases } => {
                let (endpoint, replicas) = config.list_replicas().await?.ok_or_else(|| {
                    anyhow!("no replicas endpoint found, try `replica start` to create one")
                })?;

                let src = taos::TaosBuilder::from_dsn("taos://")?.build().await?;
                let dst = taos::TaosBuilder::from_dsn(&format!("taos://{}", endpoint))?
                    .build()
                    .await?;
                let mut table = prettytable::Table::new();
                table.set_titles(prettytable::row![
                    "database",
                    "endpoint",
                    "vgroup_id",
                    "current",
                    "latest",
                    "diff",
                ]);
                for replica in replicas {
                    if databases.is_empty() || databases.contains(&replica.database) {
                        let diffs = config
                            .check_diff_once(&endpoint, &replica, &src, &dst)
                            .await?;
                        for diff in diffs {
                            table.add_row(prettytable::row![
                                diff.database,
                                diff.loc,
                                diff.vgroup_id,
                                diff.current,
                                diff.latest,
                                diff.latest - diff.current
                            ]);
                        }
                    }
                }
                table.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                table.printstd();
            }
            ReplicaCommands::Start {
                endpoint,
                mut databases,
            } => {
                tracing::info!("starting replication to {}", endpoint);
                tracing::info!("replicating databases: {:?}", databases);
                let _ =
                    Url::parse(&endpoint).map_err(|err| anyhow!("invalid endpoint: {}", err))?;
                println!("starting replication to {}", endpoint);
                config.assert_server_alive().await?;
                println!("taosX server is alive");
                let profile = config.profile().await?;
                println!(
                    "taosx version: {} built {}",
                    profile.version, profile.build_time
                );
                if databases.is_empty() {
                    let taos = taos::TaosBuilder::from_dsn(&format!("taos://"))?
                        .build()
                        .await?;
                    databases = taos
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
                config.start_replica(&endpoint, &databases).await?;
            }
            ReplicaCommands::Stop { databases } => {
                let profile = config.profile().await?;
                println!(
                    "taosx version: {} built {}",
                    profile.version, profile.build_time
                );
                tracing::info!("stopping replication");
                if let Some(databases) = databases {
                    println!("stopping replication for databases: {:?}", databases);
                    let (_, replicas) = config.list_replicas().await?.ok_or_else(|| {
                        anyhow!("no replicas endpoint found, try `replica start` to create one")
                    })?;
                    for replica in replicas {
                        if databases.contains(&replica.database) {
                            config.stop_once(&replica).await?;
                        }
                    }
                } else {
                    config.stop().await?;
                }
            }
            ReplicaCommands::Restart { databases } => {
                let profile = config.profile().await?;
                println!(
                    "taosx version: {} built {}",
                    profile.version, profile.build_time
                );
                tracing::info!("restarting replication");
                if let Some(databases) = databases {
                    println!("stopping replication for databases: {:?}", databases);
                    let (_, replicas) = config.list_replicas().await?.ok_or_else(|| {
                        anyhow!("no replicas endpoint found, try `replica start` to create one")
                    })?;
                    for replica in replicas {
                        if databases.contains(&replica.database) {
                            config.restart_once(&replica).await?;
                        }
                    }
                } else {
                    config.restart().await?;
                }
            }
            ReplicaCommands::Remove { databases } => {
                let profile = config.profile().await?;
                println!(
                    "taosx version: {} built {}",
                    profile.version, profile.build_time
                );

                println!("removing replication for databases: {:?}", databases);
                let (_, replicas) = config.list_replicas().await?.ok_or_else(|| {
                    anyhow!("no replicas endpoint found, try `replica start` to create one")
                })?;
                for replica in replicas {
                    if databases.is_empty() || databases.contains(&replica.database) {
                        config.remove_once(&replica).await?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::mem::transmute;

    use futures::TryStreamExt;
    use rand::distributions::DistString;
    use taos::{AsAsyncConsumer, IsAsyncData, IsAsyncMeta, IsOffset, MessageSet};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
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
                        println!("{target}: {}", data.as_json_meta().await?);
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
