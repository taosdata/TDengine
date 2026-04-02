use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use itertools::Itertools;
use taos_query::common::RawMeta;
use taos_query::prelude::{RawError, RawResult};
use taos_query::tmq::{
    AsAsyncConsumer, AsConsumer, Assignment, AsyncOnSync, IsAsyncData, IsData, IsMeta, IsOffset,
    MessageSet, Timeout, Timing, VGroupId,
};
use taos_query::util::Edition;
use taos_query::{Dsn, IntoDsn, RawBlock};
use tokio::sync::oneshot;
use tokio::{self, time};
use tracing::instrument;

use crate::raw::{ApiEntry, RawRes};
use crate::types::tmq_res_t;
use crate::TaosBuilder;

mod raw;

use raw::RawTmq;

use self::raw::{Conf, Topics};

#[derive(Debug)]
pub struct TmqBuilder {
    dsn: Dsn,
    builder: TaosBuilder,
    lib: Arc<ApiEntry>,
    conf: Conf,
    timeout: Timeout,
}

unsafe impl Send for TmqBuilder {}
unsafe impl Sync for TmqBuilder {}

impl TmqBuilder {
    fn from_dsn_inner<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        use taos_query::TBuilder;

        let mut dsn = dsn
            .into_dsn()
            .map_err(|e| RawError::from_string(format!("Parse dsn error: {e}")))?;

        let lib = if let Some(path) = dsn.params.remove("libraryPath") {
            ApiEntry::dlopen(path).map_err(taos_query::RawError::any)?
        } else {
            ApiEntry::open_default().map_err(taos_query::RawError::any)?
        };

        let token = dsn
            .remove("td.connect.token")
            .or_else(|| dsn.remove("bearerToken"))
            .or_else(|| dsn.remove("bearer_token"));
        if let Some(token) = token {
            dsn.set("td.connect.token".to_string(), token.clone());
            dsn.set("bearerToken".to_string(), token.clone());
            dsn.set("bearer_token".to_string(), token);
        }

        let conf = Conf::from_dsn(&dsn, lib.tmq.unwrap().conf_api)?;
        let timeout = if let Some(timeout) = dsn.params.remove("timeout") {
            Timeout::from_str(&timeout).map_err(RawError::from_any)?
        } else {
            Timeout::from_millis(500)
        };

        Ok(Self {
            builder: TaosBuilder::from_dsn(&dsn).map_err(RawError::from_any)?,
            dsn,
            lib: Arc::new(lib),
            conf,
            timeout,
        })
    }
}

impl taos_query::TBuilder for TmqBuilder {
    type Target = Consumer;

    fn available_params() -> &'static [&'static str] {
        &[
            "group.id",
            "client.id",
            "timeout",
            "enable.auto.commit",
            "bearerToken",
            "bearer_token",
            "td.connect.token",
        ]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::from_dsn_inner(dsn)
    }

    fn client_version() -> &'static str {
        ""
    }

    fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        self.build().map(|_| ())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> RawResult<Self::Target> {
        let ptr = self.conf.build()?;
        let tmq = RawTmq::new(
            self.lib.clone(),
            Arc::new(self.lib.tmq.unwrap()),
            ptr,
            self.timeout.as_raw_timeout(),
        );
        Ok(Consumer {
            tmq,
            timeout: self.timeout,
            dsn: self.dsn.clone(),
        })
    }

    fn server_version(&self) -> RawResult<&str> {
        self.builder.server_version().map_err(RawError::from_any)
    }

    fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        let taos = self.builder.inner_connection()?;
        use taos_query::prelude::sync::Queryable;
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            taos,
            "select version, (expire_time < now) as valid from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                tracing::warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TmqBuilder {
    type Target = Consumer;

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::from_dsn_inner(dsn)
    }

    fn client_version() -> &'static str {
        ""
    }

    async fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        self.build().await.map(|_| ())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> RawResult<Self::Target> {
        let ptr = self.conf.build()?;
        let tmq = RawTmq::new(
            self.lib.clone(),
            Arc::new(self.lib.tmq.unwrap()),
            ptr,
            self.timeout.as_raw_timeout(),
        );
        Ok(Consumer {
            tmq,
            timeout: self.timeout,
            dsn: self.dsn.clone(),
        })
    }

    async fn server_version(&self) -> RawResult<&str> {
        self.builder
            .server_version()
            .await
            .map_err(RawError::from_any)
    }

    async fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        let taos = self.builder.inner_connection()?;
        use taos_query::prelude::AsyncQueryable;

        // the latest version of 3.x should work
        let grant: RawResult<Option<(String, bool)>> = time::timeout(
            Duration::from_secs(60),
            AsyncQueryable::query_one(
                taos,
                "select version, (expire_time < now) as valid from information_schema.ins_cluster",
            ),
        )
        .await
        .context("Check cluster edition timeout")?;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> = time::timeout(
                Duration::from_secs(60),
                AsyncQueryable::query_one(taos, "show grants"),
            )
            .await
            .context("Check legacy grants timeout")?;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                tracing::warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

/// Consumer offset.
///
/// When offset is dropped, the message is destroyed.
pub struct Offset(RawRes);

unsafe impl Send for Offset {}
unsafe impl Sync for Offset {}

impl Debug for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Offset")
            .field("ptr", &self.0)
            .field("topic", &self.topic())
            .field("vgroup_id", &self.vgroup_id())
            .field("database", &self.database())
            .finish()
    }
}

impl IsOffset for Offset {
    fn database(&self) -> &str {
        self.0
            .tmq_db_name()
            .expect("a message should belong to a database")
    }
    fn topic(&self) -> &str {
        self.0
            .tmq_topic_name()
            .expect("a message should belong to a topic")
    }
    fn vgroup_id(&self) -> VGroupId {
        self.0
            .tmq_vgroup_id()
            .expect("a message should belong to a vgroup")
    }

    #[doc(hidden)]
    fn offset(&self) -> taos_query::tmq::Offset {
        todo!()
    }

    #[doc(hidden)]
    fn timing(&self) -> Timing {
        todo!()
    }
}

impl Drop for Offset {
    fn drop(&mut self) {
        self.0.free_result();
    }
}

#[derive(Debug)]
pub struct Consumer {
    tmq: RawTmq,
    timeout: Timeout,
    dsn: Dsn,
}

unsafe impl Send for Consumer {}
unsafe impl Sync for Consumer {}

pub struct Messages {
    tmq: RawTmq,
    timeout: Option<Duration>,
}

impl Iterator for Messages {
    type Item = RawResult<(Offset, MessageSet<Meta, Data>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let timeout = self.timeout.map_or(-1, |t| t.as_millis() as _);
        match self.tmq.poll_timeout(timeout) {
            Ok(Some(raw)) => Some(Ok((Offset(raw.clone()), MessageSet::from(raw)))),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

#[derive(Debug)]
pub struct Meta {
    res: RawRes,
    // raw: RawData,
}

impl AsyncOnSync for Meta {}

impl IsMeta for Meta {
    fn as_raw_meta(&self) -> RawResult<RawMeta> {
        self.res.tmq_get_raw()
    }

    fn as_json_meta(&self) -> RawResult<taos_query::common::JsonMeta> {
        self.res.tmq_get_json_meta()
    }
}
impl Meta {
    fn new(res: RawRes) -> Self {
        // let raw = res.tmq_get_raw().expect("get raw message error");
        Self { res }
    }

    #[cfg(test)]
    pub fn to_json(&self) -> serde_json::Value {
        self.res
            .tmq_get_json_meta()
            .ok()
            .and_then(|v| serde_json::to_value(v).ok())
            .unwrap_or_else(|| serde_json::Value::Null)
    }
}
#[derive(Debug)]
pub struct Data {
    raw: RawRes,
}

impl Data {
    fn new(raw: RawRes) -> Self {
        Self { raw }
    }
}

#[async_trait::async_trait]
impl IsAsyncData for Data {
    async fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        self.raw.tmq_get_raw()
    }

    async fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        Ok(self.raw.fetch_raw_message())
    }
}

impl IsData for Data {
    fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        self.raw.tmq_get_raw()
    }

    fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        Ok(self.raw.fetch_raw_message())
    }
}

impl From<RawRes> for MessageSet<Meta, Data> {
    fn from(raw: RawRes) -> Self {
        match raw.tmq_message_type() {
            tmq_res_t::TMQ_RES_INVALID => unreachable!(),
            tmq_res_t::TMQ_RES_DATA => Self::Data(Data::new(raw)),
            tmq_res_t::TMQ_RES_TABLE_META => Self::Meta(Meta::new(raw)),
            // tmq_res_t::TMQ_RES_METADATA => Self::MetaData(Meta::new(raw.clone()), Data::new(raw)),
            // TODO: New variant RAWDATA since 3.3.6.0
            _ => Self::MetaData(Meta::new(raw.clone()), Data::new(raw)),
        }
    }
}

impl Iterator for Data {
    type Item = RawResult<RawBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.fetch_raw_message().map(Ok)
    }
}

impl AsConsumer for Consumer {
    type Offset = Offset;
    type Meta = Meta;
    type Data = Data;

    fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        let topics = topics.into_iter().map(|item| item.into()).collect_vec();
        let topics = Topics::from_topics(self.tmq.tmq().list_api, topics)?;
        self.tmq.subscribe(&topics)
    }

    fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> RawResult<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
    > {
        Ok(self.tmq.poll_timeout(timeout.as_raw_timeout())?.map(|raw| {
            (
                Offset(raw.clone()),
                match raw.tmq_message_type() {
                    tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                    tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
                    tmq_res_t::TMQ_RES_TABLE_META => {
                        taos_query::tmq::MessageSet::Meta(Meta::new(raw))
                    }
                    // tmq_res_t::TMQ_RES_METADATA => taos_query::tmq::MessageSet::MetaData(
                    //     Meta::new(raw.clone()),
                    //     Data::new(raw),
                    // ),
                    // TODO: New variant RAWDATA since 3.3.6.0
                    _ => taos_query::tmq::MessageSet::MetaData(
                        Meta::new(raw.clone()),
                        Data::new(raw),
                    ),
                },
            )
        }))
    }

    fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        self.tmq.commit_sync(offset.0.clone()).map(|_| ())
    }

    #[doc(hidden)]
    fn commit_all(&self) -> RawResult<()> {
        todo!()
    }

    fn commit_offset(&self, topic_name: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()> {
        self.tmq.commit_offset_sync(topic_name, vgroup_id, offset)
    }

    fn list_topics(&self) -> RawResult<Vec<String>> {
        let topics = self.tmq.subscription();
        let topics = topics.to_strings();
        Ok(topics)
    }

    fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        let topics = self.tmq.subscription();
        let topics = topics.to_strings();
        let ret = topics
            .into_iter()
            .map(|topic| {
                let assignments = self.tmq.get_topic_assignment(&topic);
                (topic, assignments)
            })
            .collect();
        Some(ret)
    }

    fn offset_seek(&mut self, topic: &str, vg_id: VGroupId, offset: i64) -> RawResult<()> {
        self.tmq.offset_seek(topic, vg_id, offset)
    }

    fn committed(&self, topic: &str, vg_id: VGroupId) -> RawResult<i64> {
        self.tmq.committed(topic, vg_id)
    }

    fn position(&self, topic: &str, vg_id: VGroupId) -> RawResult<i64> {
        self.tmq.position(topic, vg_id)
    }
}

#[async_trait::async_trait]
impl AsAsyncConsumer for Consumer {
    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        let topics = Topics::from_topics(
            self.tmq.tmq().list_api,
            topics.into_iter().map(|s| s.into()),
        )?;
        let r = self.tmq.subscribe(&topics);

        if let Some(offset) = self.dsn.get("offset") {
            let offsets = offset
                .split(',')
                .map(|s| {
                    s.split(':')
                        .map(|i| i.parse::<i64>().unwrap())
                        .collect_vec()
                })
                .collect_vec();
            let topic_name = &self.tmq.subscription().to_strings()[0];
            for offset in offsets {
                let vgroup_id = offset[0];
                let offset = offset[1];
                tracing::trace!(
                    "topic {} seeking to offset {} for vgroup {}",
                    &topic_name,
                    offset,
                    vgroup_id
                );
                let _ = self.tmq.offset_seek(topic_name, vgroup_id as i32, offset);
            }
        }

        self.tmq.spawn_thread();

        r
    }

    #[instrument(skip_all)]
    async fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> RawResult<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
    > {
        let recv_fut = async move {
            let (tx, rx) = oneshot::channel();
            self.tmq
                .sender()
                .send_async(tx)
                .await
                .map_err(RawError::from_any)?;
            let res = rx.await;
            let raw = res.map_err(RawError::from_any)??.map(|raw| {
                (
                    Offset(raw.clone()),
                    match raw.tmq_message_type() {
                        tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                        tmq_res_t::TMQ_RES_DATA => MessageSet::Data(Data::new(raw)),
                        tmq_res_t::TMQ_RES_TABLE_META => MessageSet::Meta(Meta::new(raw)),
                        // TODO: new variant RAWDATA since 3.3.6.0
                        _ => MessageSet::MetaData(Meta::new(raw.clone()), Data::new(raw)),
                    },
                )
            });
            Ok(raw)
        };

        let now = time::Instant::now();

        let duration = match timeout {
            Timeout::Duration(duration) => duration,
            _ => Duration::MAX,
        };
        let sleep = tokio::time::sleep(duration);
        tokio::pin!(sleep);

        tracing::trace!("Waiting for next message");

        let res = tokio::select! {
            _ = &mut sleep, if !sleep.is_elapsed() => {
                tracing::trace!("sleep is elapsed");
                Ok(None)
            }
            res = recv_fut => res,
        };

        let elapsed = now.elapsed();
        match res {
            Ok(res) => {
                tracing::trace!(
                    consumer.recv.elapsed.ms = elapsed.as_millis(),
                    "Got a new message"
                );
                Ok(res)
            }
            Err(err) => {
                tracing::warn!(
                    consumer.recv.elapsed.ms = elapsed.as_millis(),
                    "Polling message error: {err:?}"
                );
                Err(err)
            }
        }
    }

    async fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        self.tmq.commit(offset.0.clone()).await.map(|_| ())
    }

    #[doc(hidden)]
    async fn commit_all(&self) -> RawResult<()> {
        todo!()
    }

    async fn commit_offset(
        &self,
        topic_name: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
        self.tmq
            .commit_offset_async(topic_name, vgroup_id, offset)
            .await
            .map(|_| ())
    }

    fn default_timeout(&self) -> Timeout {
        self.timeout
    }

    async fn list_topics(&self) -> RawResult<Vec<String>> {
        let topics = self.tmq.subscription();
        let topics = topics.to_strings();
        Ok(topics)
    }

    async fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        let topics = self.tmq.subscription();
        let topics = topics.to_strings();
        let ret: Vec<(String, Vec<Assignment>)> = topics
            .into_iter()
            .map(|topic| {
                let assignments = self.tmq.get_topic_assignment(&topic);
                (topic, assignments)
            })
            .collect();
        Some(ret)
    }

    async fn topic_assignment(&self, topic: &str) -> Vec<Assignment> {
        self.tmq.get_topic_assignment(topic)
    }

    async fn offset_seek(
        &mut self,
        topic: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
        self.tmq.offset_seek(topic, vgroup_id, offset)
    }

    async fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        self.tmq.committed(topic, vgroup_id)
    }

    async fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        self.tmq.position(topic, vgroup_id)
    }
}

#[cfg(test)]
mod tests {
    use super::TmqBuilder;
    use crate::TaosBuilder;

    #[test]
    fn test_write_raw_block_with_req_id() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        let db = "test_write_raw_block_req_id";
        taos.query(format!("drop topic if exists {db}"))?;
        taos.query(format!("drop database if exists {db}"))?;
        taos.query(format!("create database {db} keep 36500 vgroups 1"))?;
        taos.query(format!("use {db}"))?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create stable stb1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "insert into tb2 using stb1 tags(2, 2.2) values(now, 0) (now + 1s, 0) tb3 using stb1 tags (3, 3.3) values (now, 3) (now +1s, 3)",
        )?;

        taos.query(format!("create topic {db} with meta as database {db}"))?;

        taos.query(format!("drop database if exists {db}2"))?;
        taos.query(format!("create database {db}2"))?;
        taos.query(format!("use {db}2"))?;

        let builder =
            TmqBuilder::from_dsn("taos://localhost:6030/db?group.id=5&auto.offset.reset=earliest")?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            tracing::debug!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        dbg!(raw.table_name().unwrap());
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                        let req_id = 1002;
                        taos.write_raw_block_with_req_id(&raw, req_id)?;
                    }
                }
                MessageSet::MetaData(meta, data) => {
                    // meta
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;

                    // data
                    for raw in data {
                        let raw = raw?;
                        tracing::debug!("raw: {:?}", raw);
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                tracing::debug!("value in col {}\n", value);
                            }
                        }
                        println!();
                        let req_id = 1003;
                        taos.write_raw_block_with_req_id(&raw, req_id)?;
                    }
                }
            }

            let _ = consumer.commit(offset);
        }

        consumer.unsubscribe();

        let mut query = taos.query("describe stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }
        let mut query = taos.query("select count(*) from stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }

        taos.query(format!("drop database {db}2"))?;
        taos.query(format!("drop topic {db}")).unwrap();
        taos.query(format!("drop database {db}"))?;
        Ok(())
    }

    #[test]
    fn metadata() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        let db = "tmq_metadata";
        taos.query(format!("drop topic if exists {db}"))?;
        taos.query(format!("drop database if exists {db}"))?;
        taos.query(format!("create database {db} keep 36500 vgroups 1"))?;
        taos.query(format!("use {db}"))?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create stable stb1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "insert into tb2 using stb1 tags(2, 2.2) values(now, 0) (now + 1s, 0) tb3 using stb1 tags (3, 3.3) values (now, 3) (now +1s, 3)",
        )?;

        taos.query(format!("create topic {db} with meta as database {db}"))?;

        taos.query(format!("drop database if exists {db}2"))?;
        taos.query(format!("create database {db}2"))?;
        taos.query(format!("use {db}2"))?;

        let builder =
            TmqBuilder::from_dsn("taos://localhost:6030/db?group.id=5&auto.offset.reset=earliest")?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            tracing::debug!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        dbg!(raw.table_name().unwrap());
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                        taos.write_raw_block(&raw)?;
                    }
                }
                MessageSet::MetaData(meta, data) => {
                    // meta
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;

                    // data
                    for raw in data {
                        let raw = raw?;
                        tracing::debug!("raw: {:?}", raw);
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                tracing::debug!("value in col {}\n", value);
                            }
                        }
                        println!();
                        taos.write_raw_block(&raw)?;
                    }
                }
            }

            let _ = consumer.commit(offset);
        }

        consumer.unsubscribe();

        let mut query = taos.query("describe stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }
        let mut query = taos.query("select count(*) from stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }

        taos.query(format!("drop database {db}2"))?;
        taos.query(format!("drop topic {db}")).unwrap();
        taos.query(format!("drop database {db}"))?;
        Ok(())
    }

    #[test]
    fn meta() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        // let _ = pretty_env_logger::formatted_timed_builder().try_init();

        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        let db = "tmq_meta";
        taos.query(format!("drop topic if exists {db}"))?;
        taos.query(format!("drop database if exists {db}"))?;
        taos.query(format!("create database {db} keep 36500"))?;
        taos.query(format!("use {db}"))?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create stable stb1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create table tb1 using stb1 tags(1, 1.1)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create table cb1 (ts timestamp, v int, c2 bool, c3 varchar(10))",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "alter table cb1 add column c4 nchar(10)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "alter table cb1 drop column c4",
        )?;

        taos.query("alter table cb1 modify column c3 varchar(100)")?;
        taos.query("alter table cb1 rename column c2 n2")?;

        taos.query(format!("create topic {db} with meta as database {db}"))?;

        taos.query(format!("drop database if exists {db}2"))?;
        taos.query(format!("create database {db}2"))?;
        taos.query(format!("use {db}2"))?;

        let builder =
            TmqBuilder::from_dsn("taos://localhost:6030/db?group.id=5&auto.offset.reset=earliest")?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        tracing::trace!("polling start");

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            tracing::debug!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                    }
                }
                MessageSet::MetaData(meta, data) => {
                    let json = meta.to_json();
                    dbg!(json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;
                    for raw in data {
                        let raw = raw?;
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                    }
                }
            }

            let _ = consumer.commit(offset);
        }

        consumer.unsubscribe();

        let mut query = taos.query("describe stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }

        taos.query("drop database tmq_meta2")?;
        taos.query("drop topic tmq_meta").unwrap();
        taos.query("drop database tmq_meta")?;
        Ok(())
    }

    #[test]
    fn test_tmq_meta_sync() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        // pretty_env_logger::formatted_builder()
        //     .filter_level(tracing::tracing::LevelFilter::Debug)
        //     .init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build()?;
        taos.exec_many([
            "drop topic if exists sys_tmq_meta_sync",
            "drop database if exists sys_tmq_meta_sync",
            "create database sys_tmq_meta_sync vgroups 1",
            "use sys_tmq_meta_sync",
            "show databases",
            "select database()",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            "describe stb1",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // ***** we can not drop table, for the table will be checked later by desc tablename
            // // kind 9: drop normal table
            // "drop table `table`",
            // // kind 10: drop child table
            // "drop table `tb2`, `tb1`",
            // // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
            "create topic if not exists sys_tmq_meta_sync with meta as database sys_tmq_meta_sync",
        ])?;
        taos.exec_many([
            "drop database if exists sys_tmq_meta_sync2",
            "create database if not exists sys_tmq_meta_sync2",
            "use sys_tmq_meta_sync2",
        ])?;

        let builder = TmqBuilder::from_dsn(
            "taos://localhost:6030?group.id=10&timeout=1000ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["sys_tmq_meta_sync"])?;

        let iter = consumer.iter_with_timeout(Timeout::from_millis(500));

        for msg in iter {
            let (offset, message) = msg?;
            // Offset contains information for topic name, database name and vgroup id,
            //  similar to kafka topic/partition/offset.
            let _ = offset.topic();
            let _ = offset.database();
            let _ = offset.vgroup_id();

            // Different to kafka message, TDengine consumer would consume two kind of messages.
            //
            // 1. meta
            // 2. data
            match message {
                MessageSet::Meta(meta) => {
                    let raw = meta.as_raw_meta()?;
                    taos.write_raw_meta(&raw)?;

                    let json = meta.as_json_meta()?;
                    for json in json {
                        if let taos_query::common::MetaUnit::Create(m) = &json {
                            match m {
                                taos_query::common::MetaCreate::Super {
                                    table_name,
                                    columns: _,
                                    tags: _,
                                } => {
                                    let _desc = taos.describe(table_name.as_str())?;
                                    // dbg!(_desc);
                                }
                                taos_query::common::MetaCreate::Child {
                                    table_name,
                                    using: _,
                                    tags: _,
                                    tag_num: _,
                                } => {
                                    let _desc = taos.describe(table_name.as_str())?;
                                    // dbg!(_desc);
                                }
                                taos_query::common::MetaCreate::Normal {
                                    table_name,
                                    columns: _,
                                } => {
                                    let _desc = taos.describe(table_name.as_str())?;
                                    // dbg!(_desc);
                                }
                            }
                        }

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let sql = json.to_string();
                        tracing::debug!("sql: {}", sql);
                        if let Err(err) = taos.exec(sql) {
                            match err.code() {
                                Code::TAG_ALREADY_EXIST => tracing::trace!("tag already exists"),
                                Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                Code::INVALID_COLUMN_NAME => tracing::trace!("invalid column name"),
                                Code::MODIFIED_ALREADY => tracing::trace!("modified already done"),
                                Code::TABLE_NOT_EXIST => tracing::trace!("table does not exists"),
                                Code::STABLE_NOT_EXIST => tracing::trace!("stable does not exists"),
                                Code::INVALID_ROW_BYTES => tracing::trace!("invalid row bytes"),
                                Code::DUPLICATED_COLUMN_NAMES => {
                                    tracing::trace!("duplicated column names")
                                }
                                Code::NO_COLUMN_CAN_BE_DROPPED => {
                                    tracing::trace!("no column can be dropped")
                                }
                                _ => {
                                    tracing::error!("{}", err);
                                }
                            }
                        }
                    }
                }
                MessageSet::Data(data) => {
                    // data message may have more than one data block for various tables.
                    for block in data {
                        let block = block?;
                        // let block = block?;
                        dbg!(block.table_name());
                        dbg!(block);
                    }
                }
                _ => (),
            }
            consumer.commit(offset)?;
        }

        consumer.unsubscribe();

        taos.exec_many([
            "drop database sys_tmq_meta_sync2",
            "drop topic sys_tmq_meta_sync",
            "drop database sys_tmq_meta_sync",
        ])?;
        Ok(())
    }

    #[test]
    fn test_tmq_committed() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build()?;

        let source = "tmq_source_sync_1";
        let target = "tmq_target_sync_1";

        taos.exec_many([
            format!("drop topic if exists {source}").as_str(),
            format!("drop database if exists {source}").as_str(),
            format!("create database {source} wal_retention_period 3600").as_str(),
            format!("create topic {source} with meta as database {source}").as_str(),
            format!("use {source}").as_str(),
            "show databases",
            "select database()",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            "describe stb1",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // ***** we can not drop table, for the table will be checked later by desc tablename
            // // kind 9: drop normal table
            // "drop table `table`",
            // // kind 10: drop child table
            // "drop table `tb2`, `tb1`",
            // // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
        ])?;

        taos.exec_many([
            format!("drop database if exists {target}").as_str(),
            format!("create database if not exists {target} wal_retention_period 3600").as_str(),
            format!("use {target}").as_str(),
        ])?;

        let builder = TmqBuilder::from_dsn(
            "taos://localhost:6030?group.id=10&timeout=1000ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build()?;

        let topics = consumer.list_topics()?;
        tracing::debug!("topics before subcribe:{:?}", topics);

        consumer.subscribe([source])?;

        let topics = consumer.list_topics()?;
        tracing::debug!("topics after subcribe:{:?}", topics);

        let iter = consumer.iter_with_timeout(Timeout::from_millis(500));

        for msg in iter {
            let (offset, message) = msg?;
            // Offset contains information for topic name, database name and vgroup id,
            //  similar to kafka topic/partition/offset.
            let topic: &str = offset.topic();
            let _database = offset.database();
            let vgroup_id = offset.vgroup_id();

            // Different to kafka message, TDengine consumer would consume two kind of messages.
            //
            // 1. meta
            // 2. data
            match message {
                MessageSet::Meta(meta) => {
                    let raw = meta.as_raw_meta()?;
                    taos.write_raw_meta(&raw)?;

                    let metas = meta.as_json_meta()?;
                    for json in metas {
                        if let taos_query::common::MetaUnit::Create(m) = &json {
                            match m {
                                taos_query::common::MetaCreate::Super {
                                    table_name,
                                    columns: _,
                                    tags: _,
                                } => {
                                    let desc = taos.describe(table_name.as_str())?;
                                    tracing::trace!("{:?}", desc);
                                }
                                taos_query::common::MetaCreate::Child {
                                    table_name,
                                    using: _,
                                    tags: _,
                                    tag_num: _,
                                } => {
                                    let desc = taos.describe(table_name.as_str())?;
                                    tracing::trace!("{:?}", desc);
                                }
                                taos_query::common::MetaCreate::Normal {
                                    table_name,
                                    columns: _,
                                } => {
                                    let desc = taos.describe(table_name.as_str())?;
                                    tracing::trace!("{:?}", desc);
                                }
                            }
                        }

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let sql = json.to_string();
                        if let Err(err) = taos.exec(sql) {
                            match err.code() {
                                Code::TAG_ALREADY_EXIST => tracing::trace!("tag already exists"),
                                Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                Code::INVALID_COLUMN_NAME => tracing::trace!("invalid column name"),
                                Code::MODIFIED_ALREADY => tracing::trace!("modified already done"),
                                Code::TABLE_NOT_EXIST => tracing::trace!("table does not exists"),
                                Code::STABLE_NOT_EXIST => tracing::trace!("stable does not exists"),
                                Code::INVALID_ROW_BYTES => tracing::trace!("invalid row bytes"),
                                Code::DUPLICATED_COLUMN_NAMES => {
                                    tracing::trace!("duplicated column names")
                                }
                                Code::NO_COLUMN_CAN_BE_DROPPED => {
                                    tracing::trace!("no column can be dropped")
                                }
                                _ => {
                                    tracing::debug!("{}", err);
                                }
                            }
                        }
                    }
                }
                MessageSet::Data(data) => {
                    // data message may have more than one data block for various tables.
                    for block in data {
                        let block = block?;
                        // let block = block?;
                        tracing::trace!("{:?}", block.table_name());
                        tracing::trace!("{:?}", block);
                    }
                }
                _ => (),
            }
            let committed = consumer.committed(topic, vgroup_id);
            tracing::debug!("committed: {:?}", committed);

            let pos = consumer.position(topic, vgroup_id);
            tracing::debug!("position: {:?}", pos);

            consumer.commit(offset)?;
        }

        let assignments = consumer.assignments().unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        for (topic, vec_assignment) in assignments {
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let end = assignment.end();

                let committed = consumer.committed(topic.as_str(), vgroup_id);
                tracing::debug!("committed before: {:?}", committed);

                let _ = consumer.commit_offset(&topic, vgroup_id, end);

                let committed = consumer.committed(topic.as_str(), vgroup_id);
                tracing::debug!("committed after: {:?}", committed);
            }
        }

        let assignments = consumer.assignments().unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        consumer.unsubscribe();

        taos.exec_many([
            format!("drop database {target}").as_str(),
            format!("drop topic {source}").as_str(),
            format!("drop database {source}").as_str(),
        ])?;
        Ok(())
    }

    #[test]
    fn test_tmq_recv_timeout() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;
        taos.exec_many([
            "drop topic if exists topic_1765852570",
            "drop database if exists test_1765852570",
            "create database test_1765852570",
            "create table test_1765852570.t0 (ts timestamp, c1 int)",
            "create topic topic_1765852570 as select * from test_1765852570.t0",
        ])?;

        let tmq = TmqBuilder::from_dsn("taos://localhost:6030?group.id=10&timeout=never")?;
        let mut consumer = tmq.build()?;
        consumer.subscribe(["topic_1765852570"])?;
        let res = consumer.recv_timeout(Timeout::from_millis(1))?;
        assert!(res.is_none());
        consumer.unsubscribe();

        std::thread::sleep(std::time::Duration::from_secs(3));

        taos.exec_many([
            "drop topic if exists topic_1765852570",
            "drop database if exists test_1765852570",
        ])?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[test]
    fn test_connect_with_token() -> anyhow::Result<()> {
        use crate::tmq::{Data, Meta};
        use std::sync::mpsc;
        use std::thread;
        use taos_query::prelude::sync::*;
        use taos_query::util::test_utils::test_username;

        let taos = crate::TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;
        taos.exec_many([
            "drop token if exists token_1772592400",
            "drop topic if exists topic_1772592400",
            "drop database if exists test_1772592400",
            "create database test_1772592400",
            "create topic topic_1772592400 as database test_1772592400",
            "use test_1772592400",
            "create table t0 (ts timestamp, c1 int)",
        ])?;

        let mut rs = taos.query(format!(
            "create token token_1772592400 from user {}",
            test_username()
        ))?;
        let mut rows: Vec<String> = rs.deserialize().try_collect()?;
        assert_eq!(rows.len(), 1);
        let token = rows.remove(0);

        let num = 100;
        let (msg_tx, msg_rx) = mpsc::channel::<(MessageSet<Meta, Data>, mpsc::Sender<bool>)>();

        let cnt_handle = thread::spawn(move || -> anyhow::Result<()> {
            let mut cnt = 0;
            while let Ok((mut msg, done_tx)) = msg_rx.recv() {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_raw_block()? {
                        cnt += block.nrows();
                    }
                }
                if cnt >= num {
                    let _ = done_tx.send(true);
                    break;
                } else {
                    let _ = done_tx.send(false);
                }
            }
            assert_eq!(cnt, num);
            Ok(())
        });

        let poll_handle = thread::spawn(move || -> anyhow::Result<()> {
            let tmq = TmqBuilder::from_dsn(format!(
                "taos://invalid_user:invalid_pass@localhost:6030?group.id=3836&auto.offset.reset=earliest&bearer_token={token}"
            ))?;
            let mut consumer = tmq.build()?;
            consumer.subscribe(["topic_1772592400"])?;

            let timeout = Timeout::from_secs(5);

            loop {
                if let Some((offset, message)) = consumer.recv_timeout(timeout)? {
                    let (done_tx, done_rx) = mpsc::channel();
                    msg_tx.send((message, done_tx))?;
                    if done_rx.recv()? {
                        break;
                    }
                    consumer.commit(offset)?;
                }
            }

            consumer.unsubscribe();

            Ok(())
        });

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut sql = "insert into t0 values ".to_string();
        for i in 0..num {
            sql.push_str(&format!("({}, {}), ", ts + i as i64, i));
        }

        taos.exec(sql)?;

        poll_handle.join().unwrap()?;
        cnt_handle.join().unwrap()?;

        std::thread::sleep(std::time::Duration::from_secs(3));

        taos.exec_many([
            "drop token if exists token_1772592400",
            "drop topic if exists topic_1772592400",
            "drop database if exists test_1772592400",
        ])?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[test]
    fn test_connect_with_invalid_token() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = crate::TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;
        taos.exec_many([
            "drop topic if exists topic_1772593323",
            "drop database if exists test_1772593323",
            "create database test_1772593323",
            "create topic topic_1772593323 as database test_1772593323",
        ])?;

        let tmq = TmqBuilder::from_dsn(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=1512&td.connect.token=invalid_token"
        )?;
        let err = tmq.build().unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        let tmq = TmqBuilder::from_dsn(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=7265&bearer_token=invalid_token"
        )?;
        let err = tmq.build().unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        let tmq = TmqBuilder::from_dsn(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=7363&bearerToken=invalid_token"
        )?;
        let err = tmq.build().unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        let tmq = TmqBuilder::from_dsn(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=7363&bearerToken=",
        )?;
        let err = tmq.build().unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        std::thread::sleep(std::time::Duration::from_secs(3));

        taos.exec_many([
            "drop topic if exists topic_1772593323",
            "drop database if exists test_1772593323",
        ])?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[test]
    fn test_token_priority() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        use taos_query::util::test_utils::test_username;

        let taos = crate::TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;
        taos.exec_many([
            "drop token if exists token_1772593616",
            "drop topic if exists topic_1772593616",
            "drop database if exists test_1772593616",
            "create database test_1772593616",
            "create topic topic_1772593616 as database test_1772593616",
        ])?;

        let mut rs = taos.query(format!(
            "create token token_1772593616 from user {}",
            test_username()
        ))?;
        let mut rows: Vec<String> = rs.deserialize().try_collect()?;
        assert_eq!(rows.len(), 1);
        let token = rows.remove(0);

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=3731&td.connect.token={token}&bearer_token=invalid_token"
        ))?;
        let mut consumer = tmq.build()?;
        consumer.subscribe(["topic_1772593616"])?;
        consumer.unsubscribe();

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=9823&td.connect.token={token}&bearerToken=invalid_token"
        ))?;
        let mut consumer = tmq.build()?;
        consumer.subscribe(["topic_1772593616"])?;
        consumer.unsubscribe();

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=8222&bearerToken={token}&bearer_token=invalid_token"
        ))?;
        let mut consumer = tmq.build()?;
        consumer.subscribe(["topic_1772593616"])?;
        consumer.unsubscribe();

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=9827&bearerToken={token}"
        ))?;
        let mut consumer = tmq.build()?;
        consumer.subscribe(["topic_1772593616"])?;
        consumer.unsubscribe();

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=8371&bearer_token={token}"
        ))?;
        let mut consumer = tmq.build()?;
        consumer.subscribe(["topic_1772593616"])?;
        consumer.unsubscribe();

        std::thread::sleep(std::time::Duration::from_secs(3));

        taos.exec_many([
            "drop token if exists token_1772593616",
            "drop topic if exists topic_1772593616",
            "drop database if exists test_1772593616",
        ])?;

        Ok(())
    }

    #[test]
    fn test_available_params() {
        use taos_query::TBuilder;

        let expected = [
            "group.id",
            "client.id",
            "timeout",
            "enable.auto.commit",
            "bearerToken",
            "bearer_token",
            "td.connect.token",
        ];
        assert_eq!(TmqBuilder::available_params(), expected);
    }
}

#[cfg(test)]
mod async_tests {
    use std::time::Duration;

    use bytes::Bytes;

    use super::TmqBuilder;

    #[tokio::test]
    async fn test_write_raw_block_with_req_id() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build().await?;
        let db = "test_write_raw_block_req_id_async";
        taos.exec_many([
            format!("drop topic if exists {db}").as_str(),
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} keep 36500 vgroups 1").as_str(),
            format!("use {db}").as_str(),
            "create stable stb1(ts timestamp, v int) tags(jt int, t1 float)",
            "insert into tb2 using stb1 tags(2, 2.2) values(now, 0) (now + 1s, 0) tb3 using stb1 tags (3, 3.3) values (now, 3) (now +1s, 3)",
            format!("create topic {db} with meta as database {db}").as_str(),
            format!("drop database if exists {db}2").as_str(),
            format!("create database {db}2").as_str(),
            format!("use {db}2").as_str(),

        ]).await?;

        let builder =
            TmqBuilder::from_dsn("taos://localhost:6030/db?group.id=5&auto.offset.reset=earliest")?;
        let mut consumer = builder.build().await?;

        consumer.subscribe([db]).await?;

        for message in
            taos_query::tmq::AsConsumer::iter_with_timeout(&consumer, Timeout::from_secs(1))
        {
            let (offset, msg) = message?;
            tracing::debug!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta().await?).await?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        dbg!(raw.table_name().unwrap());
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                        let req_id = 1002;
                        taos.write_raw_block_with_req_id(&raw, req_id).await?;
                    }
                }
                MessageSet::MetaData(meta, data) => {
                    // meta
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta().await?).await?;

                    // data
                    for raw in data {
                        let raw = raw?;
                        tracing::debug!("raw: {:?}", raw);
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                tracing::debug!("value in col {}\n", value);
                            }
                        }
                        println!();
                        let req_id = 1003;
                        taos.write_raw_block_with_req_id(&raw, req_id).await?;
                    }
                }
            }

            let _ = consumer.commit(offset).await;
        }

        consumer.unsubscribe().await;

        taos.exec_many([
            format!("drop database {db}2").as_str(),
            format!("drop topic {db}").as_str(),
            format!("drop database {db}").as_str(),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_meta() -> anyhow::Result<()> {
        // use futures::TryStreamExt;
        use taos_query::prelude::*;

        // pretty_env_logger::formatted_builder()
        //     .filter_level(tracing::tracing::LevelFilter::Debug)
        //     .init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop topic if exists sys_tmq_meta",
            "drop database if exists sys_tmq_meta",
            "create database sys_tmq_meta",
            "use sys_tmq_meta",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL, \
            NULL, NULL, NULL, NULL, NULL, \
            NULL, NULL, NULL, NULL) \
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            "insert into tb4 using stb2 tags(false, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)  (ts, c1) values (now, false)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // kind 9: drop normal table
            "drop table `table`",
            // kind 10: drop child table
            "drop table `tb2`,`tb1`",
            // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
            "create topic if not exists sys_tmq_meta with meta as database sys_tmq_meta",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists sys_tmq_meta2",
            "create database if not exists sys_tmq_meta2",
            "use sys_tmq_meta2",
        ])
        .await?;

        let builder =
            TmqBuilder::from_dsn("taos:///?group.id=10&timeout=1000ms&auto.offset.reset=earliest")?;
        let mut consumer = builder.build().await?;
        consumer.subscribe(["sys_tmq_meta"]).await?;

        consumer
            .stream_with_timeout(Timeout::from_millis(500))
            .try_for_each(|(offset, message)| async {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                match message {
                    MessageSet::Meta(meta) => {
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        // dbg!(json);
                        for meta in json {
                            let sql = meta.to_string();
                            tracing::debug!("sql: {}", &sql);
                            if let Err(err) = taos.exec(sql).await {
                                match err.code() {
                                    Code::TAG_ALREADY_EXIST => {
                                        tracing::trace!("tag already exists")
                                    }
                                    Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                    Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                    Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                    Code::INVALID_COLUMN_NAME => {
                                        tracing::trace!("invalid column name")
                                    }
                                    Code::MODIFIED_ALREADY => {
                                        tracing::trace!("modified already done")
                                    }
                                    Code::TABLE_NOT_EXIST => {
                                        tracing::trace!("table does not exists")
                                    }
                                    Code::STABLE_NOT_EXIST => {
                                        tracing::trace!("stable does not exists")
                                    }
                                    Code::INVALID_ROW_BYTES => tracing::trace!("invalid row bytes"),
                                    Code::DUPLICATED_COLUMN_NAMES => {
                                        tracing::trace!("duplicated column names")
                                    }
                                    Code::NO_COLUMN_CAN_BE_DROPPED => {
                                        tracing::trace!("no column can be dropped")
                                    }
                                    _ => {
                                        tracing::error!("{}", err);
                                    }
                                }
                            }
                        }
                    }
                    MessageSet::Data(mut data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(_data) = data.next().transpose()? {
                            // dbg!(data.table_name());
                            // dbg!(data);
                        }
                    }
                    _ => (),
                }
                consumer.commit(offset).await?;
                Ok(())
            })
            .await?;

        consumer.unsubscribe().await;

        taos.exec_many([
            "drop database sys_tmq_meta2",
            "drop topic sys_tmq_meta",
            "drop database sys_tmq_meta",
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let mut dsn = "taos://localhost:6030".to_string();
        tracing::info!("dsn: {}", dsn);

        let taos = crate::TaosBuilder::from_dsn(&dsn)?.build().await?;
        let db_name = "test_tmq";
        let topic_name = "test_tmq";
        taos.exec_many([
            format!("drop topic if exists {db_name}").as_str(),
            format!("drop database if exists {db_name}").as_str(),
            format!("create database {db_name}").as_str(),
            format!("create topic {topic_name} with meta as database {db_name}").as_str(),
            format!("use {db_name}").as_str(),
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // kind 9: drop normal table
            // "drop table `table`",
            // kind 10: drop child table
            // "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
        ])
        .await?;

        let db2_name = "test_tmq2";
        taos.exec_many([
            format!("drop database if exists {db2_name}").as_str(),
            format!("create database {db2_name} wal_retention_period 3600").as_str(),
            format!("use {db2_name}").as_str(),
        ])
        .await?;

        dsn.push_str("?group.id=10&timeout=1000ms&auto.offset.reset=earliest");
        let builder = TmqBuilder::from_dsn(&dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe([topic_name]).await?;

        {
            let mut stream = consumer.stream_with_timeout(Timeout::from_secs(1));

            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let topic: &str = offset.topic();
                let database = offset.database();
                let vgroup_id = offset.vgroup_id();
                tracing::debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    topic,
                    database,
                    vgroup_id
                );

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                match message {
                    MessageSet::Meta(meta) => {
                        tracing::debug!("Meta");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        for meta in json {
                            let sql = meta.to_string();
                            if let Err(err) = taos.exec(sql).await {
                                println!("maybe error: {}", err);
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        tracing::debug!("Data");
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                    MessageSet::MetaData(meta, data) => {
                        tracing::debug!("MetaData");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = json.iter().next().unwrap().to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
                        }
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                }
                consumer.commit(offset).await?;
            }
        }

        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::debug!(
                    "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                    topic,
                    vgroup_id,
                    current,
                    begin,
                    end
                );
                let res = consumer.offset_seek(topic, vgroup_id, end).await;
                if res.is_err() {
                    tracing::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    tracing::error!("assignments: {:?}", a);
                    // panic!()
                }
            }

            let topic_assignment = consumer.topic_assignment(topic).await;
            tracing::debug!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop topic {topic_name}").as_str(),
            format!("drop database {db_name}").as_str(),
            format!("drop database {db2_name}").as_str(),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_committed() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let mut dsn = "tmq://localhost:6030?".to_string();
        tracing::info!("dsn: {}", dsn);

        let taos = crate::TaosBuilder::from_dsn(&dsn)?.build().await?;

        let source = "tmq_source_1";
        let target = "tmq_target_1";

        taos.exec_many([
            format!("drop topic if exists {source}").as_str(),
            format!("drop database if exists {source}").as_str(),
            format!("create database {source} wal_retention_period 3600").as_str(),
            format!("create topic {source} with meta as database {source}").as_str(),
            format!("use {source}").as_str(),
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // ***** we can not drop table, for the table will be checked later by desc tablename
            // // kind 9: drop normal table
            // "drop table `table`",
            // // kind 10: drop child table
            // "drop table `tb2`, `tb1`",
            // // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
        ])
        .await?;

        taos.exec_many([
            format!("drop database if exists {target}").as_str(),
            format!("create database if not exists {target} wal_retention_period 3600").as_str(),
            format!("use {target}").as_str(),
        ])
        .await?;

        dsn.push_str("&group.id=10&timeout=1000ms&auto.offset.reset=earliest");
        let builder = TmqBuilder::from_dsn(&dsn)?;

        let mut consumer = builder.build().await?;

        let topics = consumer.list_topics().await?;
        tracing::debug!("topics before subcribe:{:?}", topics);

        consumer.subscribe([source]).await?;

        let topics = consumer.list_topics().await?;
        tracing::debug!("topics after subcribe:{:?}", topics);

        {
            let mut stream = consumer.stream_with_timeout(Timeout::from_secs(1));

            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let topic: &str = offset.topic();
                let database = offset.database();
                let vgroup_id = offset.vgroup_id();
                tracing::debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    topic,
                    database,
                    vgroup_id
                );

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                match message {
                    MessageSet::Meta(meta) => {
                        tracing::debug!("Meta");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        for meta in &json {
                            let sql = meta.to_string();
                            if let Err(err) = taos.exec(sql).await {
                                println!("maybe error: {}", err);
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        tracing::debug!("Data");
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                    MessageSet::MetaData(meta, data) => {
                        tracing::debug!("MetaData");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = json.iter().next().unwrap().to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
                        }
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                }
                let committed = consumer.committed(topic, vgroup_id).await;
                tracing::debug!("committed: {:?} vgroup_id: {}", committed, vgroup_id);

                let pos = consumer.position(topic, vgroup_id).await;
                tracing::debug!("position: {:?} vgroup_id: {}", pos, vgroup_id);

                consumer.commit(offset).await?;
            }
        }

        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::debug!(
                    "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                    topic,
                    vgroup_id,
                    current,
                    begin,
                    end
                );

                let committed = consumer.committed(topic, vgroup_id).await;
                tracing::debug!(
                    "before commit_offset committed: {:?} vgroup_id: {}",
                    committed,
                    vgroup_id
                );

                let pos = consumer.position(topic, vgroup_id).await;
                tracing::debug!(
                    "before commit_offset position: {:?} vgroup_id: {}",
                    pos,
                    vgroup_id
                );

                let res = consumer.commit_offset(topic, vgroup_id, end).await;

                let committed = consumer.committed(topic, vgroup_id).await;
                tracing::debug!(
                    "after commit_offset committed: {:?} vgroup_id: {}",
                    committed,
                    vgroup_id
                );

                let pos = consumer.position(topic, vgroup_id).await;
                tracing::debug!(
                    "after commit_offset position: {:?} vgroup_id: {}",
                    pos,
                    vgroup_id
                );

                if res.is_err() {
                    tracing::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    tracing::error!("assignments: {:?}", a);
                }

                // commit offset out of range, expect error

                let res = consumer.commit_offset(topic, vgroup_id, end + 1).await;

                let committed = consumer.committed(topic, vgroup_id).await;
                tracing::info!(
                    "after commit_offset committed: {:?} vgroup_id: {}",
                    committed,
                    vgroup_id
                );

                let pos = consumer.position(topic, vgroup_id).await;
                tracing::info!(
                    "after commit_offset position: {:?} vgroup_id: {}",
                    pos,
                    vgroup_id
                );

                if res.is_err() {
                    tracing::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    tracing::error!("assignments: {:?}", a);
                }
            }

            let topic_assignment = consumer.topic_assignment(topic).await;
            tracing::debug!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop database {target}").as_str(),
            format!("drop topic {source}").as_str(),
            format!("drop database {source}").as_str(),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_offset() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let mut dsn = "tmq://localhost:6030?offset=10:20,11:40".to_string();
        tracing::info!("dsn: {}", dsn);

        let taos = crate::TaosBuilder::from_dsn(&dsn)?.build().await?;
        taos.exec_many([
            "drop topic if exists ws_abc1",
            "drop database if exists ws_abc1",
            "create database ws_abc1 wal_retention_period 3600",
            "create topic ws_abc1 with meta as database ws_abc1",
            "use ws_abc1",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, \
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16), \
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned) \
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL, NULL, \
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思', \
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, \
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10), \
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned) \
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint, \
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16), \
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思', \
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL, \
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // kind 9: drop normal table
            // "drop table `table`",
            // kind 10: drop child table
            // "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists db2",
            "create database if not exists db2 wal_retention_period 3600",
            "use db2",
        ])
        .await?;

        dsn.push_str("&group.id=10&timeout=1000ms&auto.offset.reset=earliest");
        let builder = TmqBuilder::from_dsn(&dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe(["ws_abc1"]).await?;

        {
            let mut stream = consumer.stream_with_timeout(Timeout::from_secs(1));

            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let topic: &str = offset.topic();
                let database = offset.database();
                let vgroup_id = offset.vgroup_id();
                tracing::debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    topic,
                    database,
                    vgroup_id
                );

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                match message {
                    MessageSet::Meta(meta) => {
                        tracing::debug!("Meta");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        for meta in json {
                            let sql = meta.to_string();
                            if let Err(err) = taos.exec(sql).await {
                                println!("maybe error: {}", err);
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        tracing::debug!("Data");
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                    MessageSet::MetaData(meta, data) => {
                        tracing::debug!("MetaData");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        for meta in json {
                            let sql = meta.to_string();
                            if let Err(err) = taos.exec(sql).await {
                                println!("maybe error: {}", err);
                            }
                        }
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                }
                consumer.commit(offset).await?;
            }
        }

        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::debug!(
                    "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                    topic,
                    vgroup_id,
                    current,
                    begin,
                    end
                );
                let res = consumer.offset_seek(topic, vgroup_id, end).await;
                if res.is_err() {
                    tracing::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    tracing::error!("assignments: {:?}", a);
                    // panic!()
                }
            }

            let topic_assignment = consumer.topic_assignment(topic).await;
            tracing::debug!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            "drop database db2",
            "drop topic ws_abc1",
            "drop database ws_abc1",
        ])
        .await?;
        Ok(())
    }

    async fn prepare(taos: crate::Taos) -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let inserted = taos.exec_many([
            // create child table
            "CREATE TABLE `d0` USING `meters` TAGS(0, 'California.LosAngles')",
            // insert into child table
            "INSERT INTO `d0` values(now - 10s, 10, 116, 0.32, '\\x123456')",
            // insert with NULL values
            //"INSERT INTO `d0` values(now - 8s, NULL, NULL, NULL, NULL)",
            // insert and automatically create table with tags if not exists
            "INSERT INTO `d1` USING `meters` TAGS(1, 'California.SanFrancisco') values(now - 9s, 10.1, 119, 0.33, '\\x123456')",
            // insert many records in a single sql
            "INSERT INTO `d1` values (now-8s, 10, 120, 0.33, '\\x123456') (now - 6s, 10, 119, 0.34, '\\x123456') (now - 4s, 11.2, 118, 0.322, '\\x123456')",
        ]).await?;
        assert_eq!(inserted, 5);
        Ok(())
    }

    // Query options 2, use deserialization with serde.
    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        // deserialize timestamp to String
        ts: String,
        // float to f32
        current: Option<f32>,
        // int to i32
        voltage: Option<i32>,
        phase: Option<f32>,
        cvb1: Option<Bytes>,
    }

    #[tokio::test]
    async fn test_tmq_records() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let mut dsn = "tmq://localhost:6030?offset=10:20,11:40".to_string();
        tracing::info!("dsn: {}", dsn);
        let db = "test_tmq_records";

        let taos = crate::TaosBuilder::from_dsn(&dsn)?.build().await?;
        taos.exec_many([
            "DROP TOPIC IF EXISTS tmq_meters".to_string(),
            format!("DROP DATABASE IF EXISTS `{db}`"),
            format!("CREATE DATABASE `{db}` WAL_RETENTION_PERIOD 3600"),
            format!("USE `{db}`"),
            // create super table
            "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT, cvb1 varbinary(50)) TAGS (`groupid` INT, `location` BINARY(24))".to_string(),
            // create topic for subscription
            "CREATE TOPIC tmq_meters AS SELECT * FROM `meters`".to_string()
        ])
        .await?;

        let task = tokio::spawn(prepare(taos));
        task.await??;

        dsn.push_str("&group.id=10&timeout=1000ms&auto.offset.reset=earliest");
        let builder = crate::TmqBuilder::from_dsn(&dsn)?;
        // dbg!(&builder.dsn);
        let mut consumer = builder.build().await?;
        consumer.subscribe(["tmq_meters"]).await?;

        consumer
            .stream()
            .try_for_each(|(offset, message)| async {
                let topic = offset.topic();
                // the vgroup id, like partition id in kafka.
                let vgroup_id = offset.vgroup_id();

                let assignments = consumer.assignments().await;
                println!(
                    "* in vgroup id {vgroup_id} of topic {topic}, assignments: {assignments:?}\n"
                );

                if let Some(data) = message.into_data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        let records: Vec<Record> = block.deserialize().try_collect()?;
                        println!("** read {} records: {:#?}\n", records.len(), records);
                        assert_eq!(
                            records[records.len() - 1].cvb1,
                            Some(Bytes::from(vec![0x12, 0x34, 0x56]))
                        );
                    }
                }
                consumer.commit(offset).await?;
                Ok(())
            })
            .await?;

        consumer.unsubscribe().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_recv_timeout() -> anyhow::Result<()> {
        use std::str::FromStr;

        use itertools::Itertools;
        use taos_query::prelude::TryStreamExt;
        use taos_query::tmq::{AsAsyncConsumer, IsAsyncData, IsOffset};
        use taos_query::{AsyncQueryable, AsyncTBuilder, Dsn};

        use crate::TaosBuilder;

        let _ = pretty_env_logger::try_init();

        let dsn = "taos://localhost:6030";
        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

        let db = "test_recv_timeout";
        let topic = "tmq_meters_202411260917";

        taos.exec_many([
            format!("DROP TOPIC IF EXISTS {topic}"),
            format!("DROP DATABASE IF EXISTS `{db}`"),
            format!("CREATE DATABASE `{db}`"),
            format!("USE `{db}`"),
            "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(20))".into(),
            format!("CREATE TOPIC {topic} with META AS DATABASE {db}")
        ]).await?;

        let inserted = taos.exec_many([
            "CREATE TABLE `d0` USING `meters` TAGS(0, 'Los Angles')",
            "INSERT INTO `d0` values(now - 10s, 10, 116, 0.32)",
            "INSERT INTO `d0` values(now - 8s, NULL, NULL, NULL)",
            "INSERT INTO `d1` USING `meters` TAGS(1, 'San Francisco') values(now - 9s, 10.1, 119, 0.33)",
            "INSERT INTO `d1` values (now-8s, 10, 120, 0.33) (now - 6s, 10, 119, 0.34) (now - 4s, 11.2, 118, 0.322)",
        ]).await?;
        assert_eq!(inserted, 6);

        let mut dsn = Dsn::from_str("tmq://localhost:6030")?;
        dsn.params.insert("group.id".into(), "test".into());
        dsn.params.insert("timeout".into(), "1s".into());
        dsn.params
            .insert("auto.offset.reset".into(), "earliest".into());

        let mut consumer = TmqBuilder::from_dsn(dsn)?.build().await?;
        consumer.subscribe([topic]).await?;

        {
            let mut stream = consumer.stream();
            let mut cnt = 0;
            while let Some((offset, message)) = stream.try_next().await? {
                tracing::debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    offset.topic(),
                    offset.database(),
                    offset.vgroup_id()
                );

                if let Some(data) = message.into_data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        let table_name = block.table_name().unwrap();
                        let records: Vec<Record> = block.deserialize().try_collect()?;
                        cnt += records.len();
                        tracing::debug!(
                            "table_name: {}, got {} records: {:#?}",
                            table_name,
                            records.len(),
                            records
                        );
                    }
                }

                consumer.commit(offset).await?;
            }

            assert_eq!(inserted, cnt);
        }

        consumer.unsubscribe().await;

        taos.exec_many(vec![
            format!("DROP TOPIC {topic}"),
            format!("DROP DATABASE {db}"),
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_no_auto_commit() -> anyhow::Result<()> {
        use std::str::FromStr;

        use itertools::Itertools;
        use taos_query::prelude::TryStreamExt;
        use taos_query::tmq::{AsAsyncConsumer, IsAsyncData, IsOffset};
        use taos_query::{AsyncQueryable, AsyncTBuilder, Dsn};
        use tracing::debug;

        use crate::TaosBuilder;

        unsafe { std::env::set_var("RUST_LOG", "TRACE") };
        let _ = pretty_env_logger::formatted_timed_builder()
            .parse_default_env()
            .try_init();

        let dsn = "taos://localhost:6030";
        let builder = TaosBuilder::from_dsn(dsn)?;
        let taos = builder.build().await?;
        let db = "tmq_ts5679";

        taos.exec_many([
            "DROP TOPIC IF EXISTS tmq_ts5679".into(),
            format!("DROP DATABASE IF EXISTS `{db}`"),
            format!("CREATE DATABASE `{db}` vgroups 1"),
            format!("USE `{db}`"),
            "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(20))".into(),
            format!("CREATE TOPIC tmq_ts5679 with META AS DATABASE {db}")
        ]).await?;

        let inserted = taos.exec_many([
            "CREATE TABLE `d0` USING `meters` TAGS(0, 'Los Angles')",
            "INSERT INTO `d0` values(now - 10s, 10, 116, 0.32)",
            "INSERT INTO `d0` values(now - 8s, NULL, NULL, NULL)",
            "INSERT INTO `d1` USING `meters` TAGS(1, 'San Francisco') values(now - 9s, 10.1, 119, 0.33)",
            "INSERT INTO `d1` values (now-8s, 10, 120, 0.33) (now - 6s, 10, 119, 0.34) (now - 4s, 11.2, 118, 0.322)",
        ]).await?;
        assert_eq!(inserted, 6);

        let mut dsn = Dsn::from_str("tmq://localhost:6030")?;
        dsn.params.insert("group.id".into(), "test".into());
        dsn.params.insert("timeout".into(), "1s".into());
        dsn.params
            .insert("auto.offset.reset".into(), "earliest".into());

        let builder = TmqBuilder::from_dsn(dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe(["tmq_ts5679"]).await?;

        let mut vgroup_id = 0;
        {
            let mut stream = consumer.stream();
            let mut cnt = 0;
            while let Some((offset, message)) = stream.try_next().await? {
                debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    offset.topic(),
                    offset.database(),
                    offset.vgroup_id()
                );
                vgroup_id = offset.vgroup_id();

                if let Some(data) = message.into_data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        let table_name = block.table_name().unwrap();
                        let records: Vec<Record> = block.deserialize().try_collect()?;
                        cnt += records.len();
                        debug!(
                            "table_name: {}, got {} records: {:#?}",
                            table_name,
                            records.len(),
                            records
                        );
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }

            assert_eq!(inserted, cnt);
        }

        let committed = consumer.committed("tmq_ts5679", vgroup_id).await;
        let err = committed.expect_err("No committed info");
        assert_eq!(
            err.to_string(),
            "[0x4011] Internal error: `get committed failed: No committed info`"
        );

        consumer.unsubscribe().await;

        taos.exec_many([
            "DROP TOPIC IF EXISTS tmq_ts5679",
            "DROP DATABASE IF EXISTS `tmq_ts5679`",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_auto_commit() -> anyhow::Result<()> {
        use std::str::FromStr;

        use itertools::Itertools;
        use taos_query::prelude::TryStreamExt;
        use taos_query::tmq::{AsAsyncConsumer, IsAsyncData, IsOffset};
        use taos_query::{AsyncQueryable, AsyncTBuilder, Dsn};
        use tracing::debug;

        use crate::TaosBuilder;

        unsafe { std::env::set_var("RUST_LOG", "TRACE") };
        let _ = pretty_env_logger::formatted_timed_builder()
            .parse_default_env()
            .try_init();

        let dsn = "taos://localhost:6030";
        let builder = TaosBuilder::from_dsn(dsn)?;
        let taos = builder.build().await?;
        let db = "tmq_ts5679_auto_commit";

        taos.exec_many([
            "DROP TOPIC IF EXISTS tmq_ts5679_auto_commit".into(),
            format!("DROP DATABASE IF EXISTS `{db}`"),
            format!("CREATE DATABASE `{db}` vgroups 1"),
            format!("USE `{db}`"),
            "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(20))".into(),
            format!("CREATE TOPIC tmq_ts5679_auto_commit with META AS DATABASE {db}")
        ]).await?;

        let inserted = taos.exec_many([
            "CREATE TABLE `d0` USING `meters` TAGS(0, 'Los Angles')",
            "INSERT INTO `d0` values(now - 10s, 10, 116, 0.32)",
            "INSERT INTO `d0` values(now - 8s, NULL, NULL, NULL)",
            "INSERT INTO `d1` USING `meters` TAGS(1, 'San Francisco') values(now - 9s, 10.1, 119, 0.33)",
            "INSERT INTO `d1` values (now-8s, 10, 120, 0.33) (now - 6s, 10, 119, 0.34) (now - 4s, 11.2, 118, 0.322)",
        ]).await?;
        assert_eq!(inserted, 6);

        let mut dsn = Dsn::from_str("tmq://localhost:6030")?;
        dsn.params.insert("group.id".into(), "test".into());
        dsn.params.insert("timeout".into(), "1s".into());
        dsn.params
            .insert("auto.offset.reset".into(), "earliest".into());
        dsn.params
            .insert("enable.auto.commit".into(), "true".into());
        dsn.params
            .insert("auto.commit.interval.ms".into(), "1000".into());

        let builder = TmqBuilder::from_dsn(dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe(["tmq_ts5679_auto_commit"]).await?;

        let mut vgroup_id = 0;
        {
            let mut stream = consumer.stream();
            let mut cnt = 0;
            while let Some((offset, message)) = stream.try_next().await? {
                debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    offset.topic(),
                    offset.database(),
                    offset.vgroup_id()
                );
                vgroup_id = offset.vgroup_id();

                if let Some(data) = message.into_data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        let table_name = block.table_name().unwrap();
                        let records: Vec<Record> = block.deserialize().try_collect()?;
                        cnt += records.len();
                        debug!(
                            "table_name: {}, got {} records: {:#?}",
                            table_name,
                            records.len(),
                            records
                        );
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }

            assert_eq!(inserted, cnt);
        }

        let committed = consumer
            .committed("tmq_ts5679_auto_commit", vgroup_id)
            .await;
        let offset = committed.expect("With committed info");
        assert!(offset > 0);

        consumer.unsubscribe().await;

        taos.exec_many([
            "DROP TOPIC IF EXISTS tmq_ts5679_auto_commit",
            "DROP DATABASE IF EXISTS `tmq_ts5679_auto_commit`",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_poll() -> anyhow::Result<()> {
        use taos_query::prelude::{AsAsyncConsumer, AsyncQueryable, AsyncTBuilder, TryStreamExt};
        use taos_query::tmq::Timeout;

        use crate::{TaosBuilder, TmqBuilder};

        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop topic if exists tmq_poll_topic",
            "drop database if exists tmq_poll_db",
            "create database if not exists tmq_poll_db",
            "create topic if not exists tmq_poll_topic with meta as database tmq_poll_db",
            "create table if not exists tmq_poll_db.t0 (ts timestamp, c1 int, c2 double)",
            "insert into tmq_poll_db.t0 values(now, 1, 0.12)",
        ])
        .await?;

        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        let tmq = TmqBuilder::from_dsn(format!(
            "taos:///tmq_poll_db?group.id={ms}&timeout=10s&auto.offset.reset=earliest"
        ))?;

        let mut consumer = tmq.build().await?;
        consumer.subscribe(["tmq_poll_topic"]).await?;

        {
            let mut stream = consumer.stream_with_timeout(Timeout::from_millis(9000));
            while let Ok(Some(msg)) = stream.try_next().await {
                println!("msg: {msg:?}");
            }
        }

        consumer.unsubscribe().await;

        taos.exec_many(["drop topic tmq_poll_topic", "drop database tmq_poll_db"])
            .await?;

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[tokio::test]
    async fn test_poll_blob() -> anyhow::Result<()> {
        use taos_query::prelude::{AsAsyncConsumer, AsyncQueryable, AsyncTBuilder, TryStreamExt};
        use taos_query::tmq::IsAsyncData;
        use taos_query::tmq::Timeout;

        use crate::{TaosBuilder, TmqBuilder};

        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop topic if exists topic_1753096703",
            "drop database if exists test_1753096703",
            "create database test_1753096703",
            "use test_1753096703",
            "create topic topic_1753096703 with meta as database test_1753096703",
            "create table t0 (ts timestamp, c1 int, c2 blob)",
            "insert into t0 values (now, 1, '\\x12345678')",
            "insert into t0 values (now, 1, '\\x12345678')",
        ])
        .await?;

        let tmq = TmqBuilder::from_dsn(format!(
            "taos:///test_1753096703?group.id=10&timeout=10s&auto.offset.reset=earliest"
        ))?;

        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1753096703"]).await?;

        {
            let mut cnt = 0;
            let mut stream = consumer.stream_with_timeout(Timeout::from_millis(9000));
            while let Ok(Some((_, mut msg))) = stream.try_next().await {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        cnt += block.nrows();
                    }
                }
            }
            assert_eq!(cnt, 2);
        }

        consumer.unsubscribe().await;

        taos.exec_many([
            "drop topic topic_1753096703",
            "drop database test_1753096703",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_recv_timeout() -> anyhow::Result<()> {
        use taos_query::prelude::{AsAsyncConsumer, AsyncQueryable, AsyncTBuilder};
        use taos_query::tmq::Timeout;

        use crate::{TaosBuilder, TmqBuilder};

        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;
        taos.exec_many([
            "drop topic if exists topic_1757409605",
            "drop database if exists test_1757409605",
            "create database test_1757409605",
            "create table test_1757409605.t0 (ts timestamp, c1 int)",
            "create topic topic_1757409605 as select * from test_1757409605.t0",
        ])
        .await?;

        let tmq = TmqBuilder::from_dsn("taos://localhost:6030?group.id=10&timeout=never")?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1757409605"]).await?;

        for _ in 0..15 {
            let res = consumer.recv_timeout(Timeout::from_millis(1)).await?;
            assert!(res.is_none());
        }

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_token() -> anyhow::Result<()> {
        use crate::tmq::{Data, Meta};
        use crate::{TaosBuilder, TmqBuilder};
        use taos_query::prelude::*;
        use taos_query::tmq::IsAsyncData;
        use taos_query::util::test_utils::test_username;
        use tokio::sync::{mpsc, oneshot};

        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec_many([
            "drop token if exists token_1772587050",
            "drop topic if exists topic_1772587050",
            "drop database if exists test_1772587050",
            "create database test_1772587050",
            "create topic topic_1772587050 as database test_1772587050",
            "use test_1772587050",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let mut rs = taos
            .query(format!(
                "create token token_1772587050 from user {}",
                test_username()
            ))
            .await?;
        let mut rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let token = rows.remove(0);

        let num = 100;
        let (msg_tx, mut msg_rx) =
            mpsc::channel::<(MessageSet<Meta, Data>, oneshot::Sender<bool>)>(32);

        let cnt_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut cnt = 0;
            while let Some((mut msg, done_tx)) = msg_rx.recv().await {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        cnt += block.nrows();
                    }
                }
                if cnt >= num {
                    let _ = done_tx.send(true);
                    break;
                } else {
                    let _ = done_tx.send(false);
                }
            }
            assert_eq!(cnt, num);
            Ok(())
        });

        let poll_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq = TmqBuilder::from_dsn(format!(
                "taos://invalid_user:invalid_pass@localhost:6030?group.id=8939&auto.offset.reset=earliest&bearer_token={token}"
            ))?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1772587050"]).await?;

            let timeout = Timeout::Duration(Duration::from_secs(5));

            loop {
                if let Some((offset, message)) = consumer.recv_timeout(timeout).await? {
                    let (done_tx, done_rx) = oneshot::channel();
                    msg_tx.send((message, done_tx)).await?;
                    if done_rx.await? {
                        break;
                    }
                    consumer.commit(offset).await?;
                }
            }

            consumer.unsubscribe().await;

            Ok(())
        });

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut sql = "insert into t0 values ".to_string();
        for i in 0..num {
            sql.push_str(&format!("({}, {}), ", ts + i as i64, i));
        }

        taos.exec(sql).await?;

        poll_handle.await??;
        cnt_handle.await??;

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop token if exists token_1772587050",
            "drop topic if exists topic_1772587050",
            "drop database if exists test_1772587050",
        ])
        .await?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_invalid_token() -> anyhow::Result<()> {
        use crate::{TaosBuilder, TmqBuilder};
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1772587689",
            "drop database if exists test_1772587689",
            "create database test_1772587689",
            "create topic topic_1772587689 as database test_1772587689",
        ])
        .await?;

        let tmq = TmqBuilder::from_dsn(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=9378&td.connect.token=invalid_token"
        )?;
        let err = tmq.build().await.unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        let tmq = TmqBuilder::from_dsn(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=7362&bearer_token=invalid_token"
        )?;
        let err = tmq.build().await.unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        let tmq = TmqBuilder::from_dsn(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=8392&bearerToken=invalid_token"
        )?;
        let err = tmq.build().await.unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic if exists topic_1772587689",
            "drop database if exists test_1772587689",
        ])
        .await?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_token_priority() -> anyhow::Result<()> {
        use crate::{TaosBuilder, TmqBuilder};
        use taos_query::prelude::*;
        use taos_query::util::test_utils::test_username;

        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec_many([
            "drop token if exists token_1772590919",
            "drop topic if exists topic_1772590919",
            "drop database if exists test_1772590919",
            "create database test_1772590919",
            "create topic topic_1772590919 as database test_1772590919",
        ])
        .await?;

        let mut rs = taos
            .query(format!(
                "create token token_1772590919 from user {}",
                test_username()
            ))
            .await?;
        let mut rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let token = rows.remove(0);

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=7465&td.connect.token={token}&bearer_token=invalid_token"
        ))?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1772590919"]).await?;
        consumer.unsubscribe().await;

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=6152&td.connect.token={token}&bearerToken=invalid_token"
        ))?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1772590919"]).await?;
        consumer.unsubscribe().await;

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=8764&bearerToken={token}&bearer_token=invalid_token"
        ))?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1772590919"]).await?;
        consumer.unsubscribe().await;

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=9876&bearerToken={token}"
        ))?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1772590919"]).await?;
        consumer.unsubscribe().await;

        let tmq = TmqBuilder::from_dsn(format!(
            "taos://invalid_user:invalid_pass@localhost:6030?group.id=1823&bearer_token={token}"
        ))?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1772590919"]).await?;
        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop token if exists token_1772590919",
            "drop topic if exists topic_1772590919",
            "drop database if exists test_1772590919",
        ])
        .await?;

        Ok(())
    }
}
