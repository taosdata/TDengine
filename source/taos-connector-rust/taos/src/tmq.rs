use taos_query::prelude::{AsAsyncConsumer, RawMeta, Timeout};
use taos_query::tmq::{Assignment, VGroupId};
use taos_query::{RawBlock, RawResult};

#[derive(Debug)]
enum TmqBuilderInner {
    Native(crate::sys::TmqBuilder),
    Ws(taos_ws::consumer::TmqBuilder),
}

#[derive(Debug)]
enum ConsumerInner {
    Native(crate::sys::Consumer),
    Ws(taos_ws::consumer::Consumer),
}

#[derive(Debug)]
enum OffsetInner {
    Native(crate::sys::tmq::Offset),
    Ws(taos_ws::consumer::Offset),
}

#[derive(Debug)]
enum MetaInner {
    Native(crate::sys::tmq::Meta),
    Ws(taos_ws::consumer::Meta),
}

#[derive(Debug)]
enum DataInner {
    Native(crate::sys::tmq::Data),
    Ws(taos_ws::consumer::Data),
}

#[derive(Debug)]
pub struct Offset(OffsetInner);

#[derive(Debug)]
pub struct Meta(MetaInner);

#[derive(Debug)]
pub struct Data(DataInner);

pub type MessageSet<Meta, Data> = taos_query::tmq::MessageSet<Meta, Data>;

#[derive(Debug)]
pub struct TmqBuilder(TmqBuilderInner);

#[derive(Debug)]
pub struct Consumer(ConsumerInner);

impl taos_query::TBuilder for TmqBuilder {
    type Target = Consumer;

    fn available_params() -> &'static [&'static str] {
        &[]
    }

    fn from_dsn<D: taos_query::IntoDsn>(dsn: D) -> RawResult<Self> {
        let dsn = dsn.into_dsn()?;
        match (dsn.driver.as_str(), dsn.protocol.as_deref()) {
            ("taos" | "tmq", None) => Ok(Self(TmqBuilderInner::Native(
                crate::sys::TmqBuilder::from_dsn(dsn)?,
            ))),
            ("taos" | "tmq", Some("ws" | "wss" | "http" | "https"))
            | ("ws" | "wss" | "http" | "https" | "taosws", _) => Ok(Self(TmqBuilderInner::Ws(
                taos_ws::consumer::TmqBuilder::from_dsn(dsn)?,
            ))),
            (driver, _) => Err(taos_query::DsnError::InvalidDriver(driver.to_string()).into()),
        }
    }

    fn client_version() -> &'static str {
        ""
    }

    fn ping(&self, conn: &mut Self::Target) -> RawResult<()> {
        match &self.0 {
            TmqBuilderInner::Native(b) => match &mut conn.0 {
                ConsumerInner::Native(taos) => Ok(b.ping(taos)?),
                ConsumerInner::Ws(_) => unreachable!(),
            },
            TmqBuilderInner::Ws(b) => match &mut conn.0 {
                ConsumerInner::Ws(taos) => Ok(b.ping(taos)?),
                ConsumerInner::Native(_) => unreachable!(),
            },
        }
    }

    fn ready(&self) -> bool {
        match &self.0 {
            TmqBuilderInner::Native(b) => b.ready(),
            TmqBuilderInner::Ws(b) => b.ready(),
        }
    }

    fn build(&self) -> RawResult<Self::Target> {
        match &self.0 {
            TmqBuilderInner::Native(b) => Ok(Consumer(ConsumerInner::Native(b.build()?))),
            TmqBuilderInner::Ws(b) => Ok(Consumer(ConsumerInner::Ws(b.build()?))),
        }
    }

    fn server_version(&self) -> RawResult<&str> {
        todo!()
    }

    fn is_enterprise_edition(&self) -> RawResult<bool> {
        todo!()
    }

    fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        match &self.0 {
            TmqBuilderInner::Native(b) => Ok(b.get_edition()?),
            TmqBuilderInner::Ws(b) => Ok(b.get_edition()?),
        }
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TmqBuilder {
    type Target = Consumer;

    fn from_dsn<D: taos_query::IntoDsn>(dsn: D) -> RawResult<Self> {
        let dsn = dsn.into_dsn()?;
        match (dsn.driver.as_str(), dsn.protocol.as_deref()) {
            ("taos" | "tmq", None) => Ok(Self(TmqBuilderInner::Native(
                crate::sys::TmqBuilder::from_dsn(dsn)?,
            ))),
            ("taos" | "tmq", Some("ws" | "wss" | "http" | "https"))
            | ("ws" | "wss" | "http" | "https" | "taosws", _) => Ok(Self(TmqBuilderInner::Ws(
                taos_ws::consumer::TmqBuilder::from_dsn(dsn)?,
            ))),
            (driver, _) => Err(taos_query::DsnError::InvalidDriver(driver.to_string()).into()),
        }
    }

    fn client_version() -> &'static str {
        ""
    }

    async fn ping(&self, conn: &mut Self::Target) -> RawResult<()> {
        match &self.0 {
            TmqBuilderInner::Native(b) => match &mut conn.0 {
                ConsumerInner::Native(taos) => Ok(b.ping(taos).await?),
                ConsumerInner::Ws(_) => unreachable!(),
            },
            TmqBuilderInner::Ws(b) => match &mut conn.0 {
                ConsumerInner::Ws(taos) => Ok(b.ping(taos).await?),
                ConsumerInner::Native(_) => unreachable!(),
            },
        }
    }

    async fn ready(&self) -> bool {
        match &self.0 {
            TmqBuilderInner::Native(b) => b.ready().await,
            TmqBuilderInner::Ws(b) => b.ready().await,
        }
    }

    async fn build(&self) -> RawResult<Self::Target> {
        match &self.0 {
            TmqBuilderInner::Native(b) => Ok(Consumer(ConsumerInner::Native(b.build().await?))),
            TmqBuilderInner::Ws(b) => Ok(Consumer(ConsumerInner::Ws(b.build().await?))),
        }
    }

    async fn server_version(&self) -> RawResult<&str> {
        match &self.0 {
            TmqBuilderInner::Native(b) => Ok(b.server_version().await?),
            TmqBuilderInner::Ws(b) => Ok(b.server_version().await?),
        }
    }

    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        match &self.0 {
            TmqBuilderInner::Native(b) => Ok(b.is_enterprise_edition().await?),
            TmqBuilderInner::Ws(b) => Ok(b.is_enterprise_edition().await?),
        }
    }

    async fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        match &self.0 {
            TmqBuilderInner::Native(b) => Ok(b.get_edition().await?),
            TmqBuilderInner::Ws(b) => Ok(b.get_edition().await?),
        }
    }
}

impl taos_query::tmq::IsOffset for Offset {
    fn database(&self) -> &str {
        match &self.0 {
            OffsetInner::Native(offset) => {
                <crate::sys::tmq::Offset as taos_query::tmq::IsOffset>::database(offset)
            }
            OffsetInner::Ws(offset) => {
                <taos_ws::consumer::Offset as taos_query::tmq::IsOffset>::database(offset)
            }
        }
    }

    fn topic(&self) -> &str {
        match &self.0 {
            OffsetInner::Native(offset) => {
                <crate::sys::tmq::Offset as taos_query::tmq::IsOffset>::topic(offset)
            }
            OffsetInner::Ws(offset) => {
                <taos_ws::consumer::Offset as taos_query::tmq::IsOffset>::topic(offset)
            }
        }
    }

    fn vgroup_id(&self) -> taos_query::tmq::VGroupId {
        match &self.0 {
            OffsetInner::Native(offset) => {
                <crate::sys::tmq::Offset as taos_query::tmq::IsOffset>::vgroup_id(offset)
            }
            OffsetInner::Ws(offset) => {
                <taos_ws::consumer::Offset as taos_query::tmq::IsOffset>::vgroup_id(offset)
            }
        }
    }

    fn offset(&self) -> taos_query::tmq::Offset {
        match &self.0 {
            OffsetInner::Native(offset) => {
                <crate::sys::tmq::Offset as taos_query::tmq::IsOffset>::offset(offset)
            }
            OffsetInner::Ws(offset) => {
                <taos_ws::consumer::Offset as taos_query::tmq::IsOffset>::offset(offset)
            }
        }
    }

    fn timing(&self) -> taos_query::tmq::Timing {
        match &self.0 {
            OffsetInner::Native(offset) => {
                <crate::sys::tmq::Offset as taos_query::tmq::IsOffset>::timing(offset)
            }
            OffsetInner::Ws(offset) => {
                <taos_ws::consumer::Offset as taos_query::tmq::IsOffset>::timing(offset)
            }
        }
    }
}

#[async_trait::async_trait]
impl taos_query::tmq::IsAsyncMeta for Meta {
    async fn as_raw_meta(&self) -> RawResult<RawMeta> {
        match &self.0 {
            MetaInner::Native(data) => {
                <crate::sys::tmq::Meta as taos_query::tmq::IsAsyncMeta>::as_raw_meta(data).await
            }
            MetaInner::Ws(data) => {
                <taos_ws::consumer::Meta as taos_query::tmq::IsAsyncMeta>::as_raw_meta(data).await
            }
        }
    }

    async fn as_json_meta(&self) -> RawResult<taos_query::common::JsonMeta> {
        match &self.0 {
            MetaInner::Native(data) => {
                <crate::sys::tmq::Meta as taos_query::tmq::IsAsyncMeta>::as_json_meta(data).await
            }
            MetaInner::Ws(data) => {
                <taos_ws::consumer::Meta as taos_query::tmq::IsAsyncMeta>::as_json_meta(data).await
            }
        }
    }
}

#[async_trait::async_trait]
impl taos_query::tmq::IsAsyncData for Data {
    async fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        match &self.0 {
            DataInner::Native(data) => {
                <crate::sys::tmq::Data as taos_query::tmq::IsAsyncData>::as_raw_data(data).await
            }
            DataInner::Ws(data) => {
                <taos_ws::consumer::Data as taos_query::tmq::IsAsyncData>::as_raw_data(data).await
            }
        }
    }

    async fn fetch_raw_block(&self) -> RawResult<Option<taos_query::RawBlock>> {
        match &self.0 {
            DataInner::Native(data) => {
                <crate::sys::tmq::Data as taos_query::tmq::IsAsyncData>::fetch_raw_block(data).await
            }
            DataInner::Ws(data) => {
                <taos_ws::consumer::Data as taos_query::tmq::IsAsyncData>::fetch_raw_block(data)
                    .await
            }
        }
    }
}

#[async_trait::async_trait]
impl AsAsyncConsumer for Consumer {
    type Offset = Offset;
    type Meta = Meta;
    type Data = Data;

    fn default_timeout(&self) -> Timeout {
        match &self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::default_timeout(c)
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::default_timeout(c)
            }
        }
    }

    async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        match &mut self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::subscribe(c, topics).await
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::subscribe(c, topics).await
            }
        }
    }

    async fn unsubscribe(self) {
        match self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::unsubscribe(c).await;
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::unsubscribe(c).await;
            }
        }
    }

    async fn recv_timeout(
        &self,
        timeout: Timeout,
    ) -> RawResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>> {
        match &self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::recv_timeout(c, timeout)
                    .await
                    .map(|msg| {
                        msg.map(|(offset, msg)| {
                            (
                                Offset(OffsetInner::Native(offset)),
                                match msg {
                                    MessageSet::Meta(meta) => {
                                        MessageSet::Meta(Meta(MetaInner::Native(meta)))
                                    }
                                    MessageSet::Data(data) => {
                                        MessageSet::Data(Data(DataInner::Native(data)))
                                    }
                                    MessageSet::MetaData(meta, data) => MessageSet::MetaData(
                                        Meta(MetaInner::Native(meta)),
                                        Data(DataInner::Native(data)),
                                    ),
                                },
                            )
                        })
                    })
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::recv_timeout(c, timeout)
                    .await
                    .map(|msg| {
                        msg.map(|(offset, msg)| {
                            (
                                Offset(OffsetInner::Ws(offset)),
                                match msg {
                                    taos_query::tmq::MessageSet::Meta(meta) => {
                                        MessageSet::Meta(Meta(MetaInner::Ws(meta)))
                                    }
                                    taos_query::tmq::MessageSet::Data(data) => {
                                        MessageSet::Data(Data(DataInner::Ws(data)))
                                    }
                                    taos_query::tmq::MessageSet::MetaData(meta, data) => {
                                        MessageSet::MetaData(
                                            Meta(MetaInner::Ws(meta)),
                                            Data(DataInner::Ws(data)),
                                        )
                                    }
                                },
                            )
                        })
                    })
            }
        }
    }

    async fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        match &self.0 {
            ConsumerInner::Native(c) => match offset.0 {
                OffsetInner::Native(offset) => {
                    <crate::sys::Consumer as AsAsyncConsumer>::commit(c, offset).await
                }
                OffsetInner::Ws(_) => unreachable!(),
            },
            ConsumerInner::Ws(c) => match offset.0 {
                OffsetInner::Ws(offset) => {
                    <taos_ws::consumer::Consumer as AsAsyncConsumer>::commit(c, offset).await
                }
                OffsetInner::Native(_) => unreachable!(),
            },
        }
    }

    async fn commit_all(&self) -> RawResult<()> {
        match &self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::commit_all(c).await
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::commit_all(c).await
            }
        }
    }

    async fn commit_offset(&self, topic: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()> {
        match &self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::commit_offset(
                    c, topic, vgroup_id, offset,
                )
                .await
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::commit_offset(
                    c, topic, vgroup_id, offset,
                )
                .await
            }
        }
    }

    async fn list_topics(&self) -> RawResult<Vec<String>> {
        match &self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::list_topics(c).await
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::list_topics(c).await
            }
        }
    }

    async fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        match &self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::assignments(c).await
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::assignments(c).await
            }
        }
    }

    async fn topic_assignment(&self, topic: &str) -> Vec<Assignment> {
        match &self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::topic_assignment(c, topic).await
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::topic_assignment(c, topic).await
            }
        }
    }

    async fn offset_seek(
        &mut self,
        topic: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
        match &mut self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::offset_seek(c, topic, vgroup_id, offset)
                    .await
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::offset_seek(
                    c, topic, vgroup_id, offset,
                )
                .await
            }
        }
    }

    async fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        match &self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::committed(c, topic, vgroup_id).await
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::committed(c, topic, vgroup_id)
                    .await
            }
        }
    }

    async fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        match &self.0 {
            ConsumerInner::Native(c) => {
                <crate::sys::Consumer as AsAsyncConsumer>::position(c, topic, vgroup_id).await
            }
            ConsumerInner::Ws(c) => {
                <taos_ws::consumer::Consumer as AsAsyncConsumer>::position(c, topic, vgroup_id)
                    .await
            }
        }
    }
}

impl taos_query::tmq::SyncOnAsync for Consumer {}
impl taos_query::tmq::SyncOnAsync for Data {}
impl taos_query::tmq::SyncOnAsync for Meta {}

impl Iterator for Data {
    type Item = RawResult<RawBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.0 {
            DataInner::Native(data) => {
                <crate::sys::tmq::Data as taos_query::tmq::IsData>::fetch_raw_block(data)
                    .transpose()
            }
            DataInner::Ws(data) => {
                <taos_ws::consumer::Data as taos_query::tmq::IsData>::fetch_raw_block(data)
                    .transpose()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TmqBuilder;

    #[test]
    fn builder() -> taos_query::RawResult<()> {
        use taos_query::prelude::*;
        let mut dsn: Dsn = "taos://".parse()?;
        dsn.set("group.id", "group1");
        dsn.set("client.id", "test");
        dsn.set("auto.offset.reset", "earliest");

        let _tmq = TmqBuilder::from_dsn(dsn)?;
        Ok(())
    }
}

#[cfg(test)]
mod async_tests {
    use std::str::FromStr;
    use std::time::Duration;

    use serde::Deserialize;

    use super::TmqBuilder;
    use crate::TaosBuilder;

    #[tokio::test]
    async fn test_ws_tmq_meta() -> taos_query::RawResult<()> {
        use taos_query::prelude::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos+ws://localhost:6041".to_string());
        let mut dsn = Dsn::from_str(&dsn)?;

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "ws_abc1";

        taos.exec(format!("drop topic if exists {db}")).await?;
        taos.exec(format!("drop database if exists {db}")).await?;

        std::thread::sleep(std::time::Duration::from_secs(3));

        taos.exec(format!(
            "create database if not exists {db} wal_retention_period 3600"
        ))
        .await?;

        std::thread::sleep(std::time::Duration::from_secs(3));

        taos.exec_many([
            "create topic ws_abc1 with meta as database ws_abc1",
            "use ws_abc1",
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
            // kind 9: alter child table tag
            "alter table `tb2` set tag t2 = 1",
            "alter table `tb2` set tag t7 = 1.1",
            "alter table `tb2` set tag t9 = 'hello'",
            "alter table `tb2` set tag t10 = '中文'",
            "alter table `tb2` set tag t2 = 2, t7 = 2.2",
            "alter table `tb2` set tag t2 = 3, t7 = 3.3, t9 = 'world'",
            "alter table `tb2` set tag t2 = 4, t7 = 4.4, t9 = 'helloworld', t10 = '中文中文'",
            // kind 10: drop normal table
            "drop table `table`",
            // kind 11: drop child table
            "drop table `tb2`, `tb1`",
            // kind 12: drop super table
            "drop table `stb2`",
            "drop table `stb1`",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists db2",
            "create database if not exists db2 wal_retention_period 3600",
            "use db2",
        ])
        .await?;

        dsn.params.insert("group.id".to_string(), "abc".to_string());

        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());

        let builder = TmqBuilder::from_dsn(&dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe(["ws_abc1"]).await?;

        {
            let mut stream = consumer.stream_with_timeout(Timeout::from_secs(1));

            while let Some((offset, message)) = stream.try_next().await? {
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
                        let meta = meta.as_json_meta().await?;
                        for unit in meta.iter() {
                            let sql = unit.to_string();
                            println!("meta exec sql: {sql}");
                            if let Err(err) = taos.exec(sql).await {
                                println!("meta maybe error: {err}");
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            dbg!(data.table_name());
                            dbg!(data);
                        }
                    }
                    MessageSet::MetaData(meta, data) => {
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let meta = meta.as_json_meta().await?;
                        for unit in meta.iter() {
                            let sql = unit.to_string();
                            println!("metadata exec sql: {sql}");
                            if let Err(err) = taos.exec(sql).await {
                                println!("metadata maybe error: {err}");
                            }
                        }
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            dbg!(data.table_name());
                            dbg!(data);
                        }
                    }
                }
                consumer.commit(offset).await?;
            }
        }
        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        taos.exec_many([
            "drop database db2",
            "drop topic ws_abc1",
            "drop database ws_abc1",
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_tmq() -> taos_query::RawResult<()> {
        // pretty_env_logger::formatted_timed_builder()
        //     .filter_level(tracing::LevelFilter::Info)
        //     .init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = "taos://localhost:6030".to_string();
        tracing::info!("dsn: {}", dsn);
        let mut dsn = Dsn::from_str(&dsn)?;

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;
        taos.exec_many([
            "drop topic if exists ws_abc1",
            "drop database if exists ws_abc1",
            "create database ws_abc1 wal_retention_period 3600",
            "create topic ws_abc1 with meta as database ws_abc1",
            "use ws_abc1",
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

        taos.exec_many([
            "drop database if exists db2",
            "create database if not exists db2 wal_retention_period 3600",
            "use db2",
        ])
        .await?;

        dsn.params.insert("group.id".to_string(), "abc".to_string());

        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());

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
                        let sql = json.iter().next().unwrap().to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
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
            "drop database db2",
            "drop topic ws_abc1",
            "drop database ws_abc1",
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_tmq_offset() -> taos_query::RawResult<()> {
        // pretty_env_logger::formatted_timed_builder()
        //     .filter_level(tracing::LevelFilter::Info)
        //     .init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = "tmq://localhost:6030?offset=10:20,11:40".to_string();
        tracing::info!("dsn: {}", dsn);
        let mut dsn = Dsn::from_str(&dsn)?;
        // dbg!(&dsn);

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;
        taos.exec_many([
            "drop topic if exists ws_abc1",
            "drop database if exists ws_abc1",
            "create database ws_abc1 wal_retention_period 3600",
            "create topic ws_abc1 with meta as database ws_abc1",
            "use ws_abc1",
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

        taos.exec_many([
            "drop database if exists db2",
            "create database if not exists db2 wal_retention_period 3600",
            "use db2",
        ])
        .await?;

        dsn.params.insert("group.id".to_string(), "abc".to_string());

        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());

        let builder = TmqBuilder::from_dsn(&dsn)?;
        // dbg!(&builder);
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
                        let sql = json.iter().next().unwrap().to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
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
            "drop database db2",
            "drop topic ws_abc1",
            "drop database ws_abc1",
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ws_tmq() -> taos_query::RawResult<()> {
        // pretty_env_logger::formatted_timed_builder()
        // .filter_level(tracing::LevelFilter::Info)
        // .init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = "taosws://localhost:6041".to_string();
        tracing::info!("dsn: {}", dsn);
        let mut dsn = Dsn::from_str(&dsn)?;

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "ws_tmq_1";
        let db2 = "ws_tmq_1_dest";

        taos.exec_many([
            format!("drop topic if exists {db}").as_str(),
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} wal_retention_period 1").as_str(),
            format!("create topic {db} with meta as database {db}").as_str(),
            format!("use {db}").as_str(),
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
        ])
        .await?;

        taos.exec_many([
            format!("drop database if exists {db2}"),
            format!("create database if not exists {db2} wal_retention_period 1"),
            format!("use {db2}"),
        ])
        .await?;

        dsn.params.insert("group.id".to_string(), "abc".to_string());

        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());

        let builder = TmqBuilder::from_dsn(&dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe([db]).await?;

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
                        let sql = json.iter().next().unwrap().to_string();
                        // dbg!(&sql);
                        if let Err(err) = taos.exec(sql).await {
                            tracing::error!("meta error: {}", err);
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
                            println!("metadata error: {}", err);
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
        // dbg!(&assignments);
        tracing::info!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::info!(
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
            tracing::info!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::info!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop database {db2}"),
            format!("drop topic {db}"),
            format!("drop database {db}"),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ws_raw_block_table_name() -> taos_query::RawResult<()> {
        // pretty_env_logger::formatted_timed_builder()
        // .filter_level(tracing::LevelFilter::Info)
        // .init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = "taosws://localhost:6041".to_string();
        tracing::info!("dsn: {}", dsn);
        let mut dsn = Dsn::from_str(&dsn)?;

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "ws_tmq_block_1";
        let db2 = "ws_tmq_block_1_dest";

        taos.exec_many([
            format!("drop topic if exists {db}").as_str(),
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} wal_retention_period 1").as_str(),
            format!("create topic {db} with meta as database {db}").as_str(),
            format!("use {db}").as_str(),
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
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
        ])
        .await?;

        taos.exec_many([
            format!("drop database if exists {db2}"),
            format!("create database if not exists {db2} wal_retention_period 1"),
            format!("use {db2}"),
        ])
        .await?;

        dsn.params.insert("group.id".to_string(), "abc".to_string());

        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());

        let builder = TmqBuilder::from_dsn(&dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe([db]).await?;

        {
            let mut stream = consumer.stream_with_timeout(Timeout::from_secs(1));

            while let Some((offset, message)) = stream.try_next().await? {
                let topic: &str = offset.topic();
                let database = offset.database();
                let vgroup_id = offset.vgroup_id();
                tracing::debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    topic,
                    database,
                    vgroup_id
                );

                match message {
                    MessageSet::Meta(meta) => {
                        tracing::debug!("Meta");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        let json = meta.as_json_meta().await?;
                        let sql = json.iter().next().unwrap().to_string();
                        // dbg!(&sql);
                        if let Err(err) = taos.exec(sql).await {
                            tracing::error!("meta error: {}", err);
                        }
                    }
                    MessageSet::Data(data) => {
                        tracing::info!("Data");
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::info!("table_name: {:?}", data.table_name());
                            assert_eq!(data.table_name(), Some("tb0"));
                            tracing::info!("data: {}", data.pretty_format());
                            assert!(data
                                .pretty_format()
                                .to_string()
                                .contains("table name \"tb0\""));
                        }
                    }
                    MessageSet::MetaData(meta, data) => {
                        tracing::info!("MetaData");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = json.iter().next().unwrap().to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("metadata error: {}", err);
                        }
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::info!("MetaData table_name: {:?}", data.table_name());
                            tracing::info!("MetaData data: {:?}", data);
                        }
                    }
                }
                consumer.commit(offset).await?;
            }
        }

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop database {db2}"),
            format!("drop topic {db}"),
            format!("drop database {db}"),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ws_flush_db() -> taos_query::RawResult<()> {
        // pretty_env_logger::formatted_timed_builder()
        // .filter_level(tracing::LevelFilter::Info)
        // .init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = "taosws://localhost:6041".to_string();
        tracing::info!("dsn: {}", dsn);
        let mut dsn = Dsn::from_str(&dsn)?;

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "ws_tmq_flush_1";
        let db2 = "ws_tmq_flush_1_dest";

        taos.exec_many([
            format!("drop topic if exists {db}").as_str(),
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} wal_retention_period 1").as_str(),
            format!("create topic {db} with meta as database {db}").as_str(),
            format!("use {db}").as_str(),
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
        ])
        .await?;

        for _ in 0..1000 {
            taos.exec(
                "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            )
            .await?;
        }
        taos.exec(format!("flush database {db}")).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop database if exists {db2}"),
            format!("create database if not exists {db2} wal_retention_period 1"),
            format!("use {db2}"),
        ])
        .await?;

        dsn.params.insert("group.id".to_string(), "abc".to_string());

        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());
        dsn.params.insert(
            "experimental.snapshot.enable".to_string(),
            "true".to_string(),
        );

        let builder = TmqBuilder::from_dsn(&dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe([db]).await?;

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
                        let sql = json.iter().next().unwrap().to_string();
                        // dbg!(&sql);
                        if let Err(err) = taos.exec(sql).await {
                            tracing::error!("maybe error: {}", err);
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
        // dbg!(&assignments);
        tracing::info!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::info!(
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
            tracing::info!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::info!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop database {db2}"),
            format!("drop topic {db}"),
            format!("drop database {db}"),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ws_tmq_snapshot() -> taos_query::RawResult<()> {
        // std::env::set_var("RUST_LOG", "tokio=warn,taos_ws=trace,info");
        // pretty_env_logger::init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = "taosws://localhost:6041".to_string();
        tracing::info!("dsn: {}", dsn);
        let mut dsn = Dsn::from_str(&dsn)?;

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "ws_abc1_snapshot";
        let db2 = "ws_abc1_snapshot_dest";

        taos.exec_many([
            format!("drop topic if exists {db}").as_str(),
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} wal_retention_period 3600").as_str(),
            format!("create topic {db} with meta as database {db}").as_str(),
            format!("use {db}").as_str(),
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
        ])
        .await?;

        for _ in 0..100 {
            taos.exec(
                "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            )
            .await?;
        }

        taos.exec_many([
            format!("drop database if exists {db2}"),
            format!("create database if not exists {db2} wal_retention_period 3600"),
            format!("use {db2}"),
        ])
        .await?;

        dsn.params.insert("group.id".to_string(), "abc".to_string());

        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());

        let builder = TmqBuilder::from_dsn(&dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe([db]).await?;

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
                        let sql = json.iter().next().unwrap().to_string();
                        // dbg!(&sql);
                        if let Err(err) = taos.exec(sql).await {
                            tracing::debug!("maybe error: {}", err);
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
        dbg!(&assignments);
        tracing::info!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::info!(
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
            tracing::info!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::info!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop database {db2}"),
            format!("drop topic {db}"),
            format!("drop database {db}"),
        ])
        .await?;
        Ok(())
    }
    #[tokio::test]
    async fn test_ws_tmq_offset() -> taos_query::RawResult<()> {
        // pretty_env_logger::formatted_timed_builder()
        //     .filter_level(tracing::LevelFilter::Info)
        //     .init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = "tmq+ws://localhost:6041?offset=10:20,11:40".to_string();
        tracing::info!("dsn: {}", dsn);
        let mut dsn = Dsn::from_str(&dsn)?;

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "ws_tmq_abc2";
        let db2 = "ws_tmq_abc2_dest";

        taos.exec(format!("drop topic if exists {db}")).await?;
        taos.exec(format!("drop database if exists {db}")).await?;

        std::thread::sleep(std::time::Duration::from_secs(3));

        taos.exec(format!(
            "create database if not exists {db} wal_retention_period 3600"
        ))
        .await?;

        std::thread::sleep(std::time::Duration::from_secs(3));

        taos.exec_many([
            format!("create topic {db} with meta as database {db}").as_str(),
            format!("use {db}").as_str(),
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

        taos.exec_many([
            format!("drop database if exists {db2}"),
            format!("create database if not exists {db2} wal_retention_period 3600"),
            format!("use {db2}"),
        ])
        .await?;

        dsn.params.insert("group.id".to_string(), "abc".to_string());

        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());

        let builder = TmqBuilder::from_dsn(&dsn)?;
        // dbg!(&builder);
        let mut consumer = builder.build().await?;
        consumer.subscribe([db]).await?;

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
                        let sql = json.iter().next().unwrap().to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
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
            format!("drop database {db2}"),
            format!("drop topic {db}"),
            format!("drop database {db}"),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ws_tmq_committed() -> taos_query::RawResult<()> {
        use taos_query::prelude::*;

        let dsn = "tmq+ws://localhost:6041?".to_string();
        tracing::info!("dsn: {}", dsn);
        let mut dsn = Dsn::from_str(&dsn)?;

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "ws_tmq_committed";
        let db2 = "ws_tmq_committed_dest";

        taos.exec(format!("drop topic if exists {db}")).await?;
        taos.exec(format!("drop database if exists {db}")).await?;

        std::thread::sleep(std::time::Duration::from_secs(1));

        taos.exec(format!(
            "create database if not exists {db} wal_retention_period 3600"
        ))
        .await?;

        std::thread::sleep(std::time::Duration::from_secs(1));

        taos.exec_many([
            format!("create topic {db} with meta as database {db}").as_str(),
            format!("use {db}").as_str(),
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
            "drop table `table`",
            // kind 10: drop child table
            "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            "drop table `stb2`",
            "drop table `stb1`",
        ])
        .await?;

        taos.exec_many([
            format!("drop database if exists {db2}"),
            format!("create database if not exists {db2} wal_retention_period 3600"),
            format!("use {db2}"),
        ])
        .await?;

        dsn.params.insert("group.id".to_string(), "abc".to_string());

        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());
        let builder = TmqBuilder::from_dsn(&dsn)?;
        // dbg!(&builder);
        let mut consumer = builder.build().await?;

        let topics = consumer.list_topics().await?;
        tracing::info!("topics: {:?}", topics);
        consumer.subscribe([db]).await?;
        let topics = consumer.list_topics().await?;
        tracing::info!("topics: {:?}", topics);

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
                        let sql = json.iter().next().unwrap().to_string();
                        if let Err(err) = taos.exec(sql).await {
                            tracing::trace!("maybe error: {}", err);
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
        tracing::info!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::info!(
                    "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                    topic,
                    vgroup_id,
                    current,
                    begin,
                    end
                );

                let committed = consumer.committed(topic, vgroup_id).await?;
                tracing::info!("committed: {:?}", committed);

                let position = consumer.position(topic, vgroup_id).await?;
                tracing::info!("position: {:?}", position);

                let res = consumer.offset_seek(topic, vgroup_id, end).await;
                if res.is_err() {
                    tracing::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    tracing::error!("assignments: {:?}", a);
                }

                let committed = consumer.committed(topic, vgroup_id).await?;
                tracing::info!("after seek committed: {:?}", committed);

                let position = consumer.position(topic, vgroup_id).await?;
                tracing::info!("after seek position: {:?}", position);

                let res = consumer.commit_offset(topic, vgroup_id, end).await;
                if res.is_err() {
                    tracing::error!("commit offset response: {:?}", res);
                }

                let committed = consumer.committed(topic, vgroup_id).await?;
                tracing::info!("after commit committed: {:?}", committed);

                let position = consumer.position(topic, vgroup_id).await?;
                tracing::info!("after commit position: {:?}", position);
            }

            let topic_assignment = consumer.topic_assignment(topic).await;
            tracing::info!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop database {db2}"),
            format!("drop topic {db}"),
            format!("drop database {db}"),
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_timezone_default() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1753758290",
            "drop database if exists test_1753758290",
            "create database test_1753758290",
            "create topic topic_1753758290 as database test_1753758290",
            "use test_1753758290",
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values ('2025-01-01 12:00:00', 1)",
            "insert into t0 values ('2025-01-02 15:30:00', 2)",
        ])
        .await?;

        let tmq =
            TmqBuilder::from_dsn("ws://localhost:6041?group.id=10&auto.offset.reset=earliest")?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1753758290"]).await?;

        #[derive(Debug, Deserialize)]
        struct Record {
            ts: String,
            c1: i32,
        }

        let mut cnt = 0;
        let mut records: Vec<Record> = Vec::new();

        let timeout = Timeout::Duration(Duration::from_secs(2));

        loop {
            if let Some((offset, mut message)) = consumer.recv_timeout(timeout).await? {
                if let Some(data) = message.data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        cnt += block.nrows();
                        records.append(&mut block.deserialize().try_collect()?);
                    }
                }
                consumer.commit(offset).await?;
            }

            if cnt == 2 {
                break;
            }
        }

        consumer.unsubscribe().await;

        assert_eq!(records.len(), 2);

        assert_eq!(records[0].ts, "2025-01-01T12:00:00+08:00");
        assert_eq!(records[1].ts, "2025-01-02T15:30:00+08:00");

        assert_eq!(records[0].c1, 1);
        assert_eq!(records[1].c1, 2);

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic topic_1753758290",
            "drop database test_1753758290",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_timezone_custom() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041?timezone=America/New_York")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1753759808",
            "drop database if exists test_1753759808",
            "create database test_1753759808",
            "create topic topic_1753759808 as database test_1753759808",
            "use test_1753759808",
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values ('2025-01-01 12:00:00', 1)",
            "insert into t0 values ('2025-01-02 15:30:00', 2)",
        ])
        .await?;

        let tmq = TmqBuilder::from_dsn(
            "ws://localhost:6041?group.id=10&auto.offset.reset=earliest&timezone=America/New_York",
        )?;

        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1753759808"]).await?;

        #[derive(Debug, Deserialize)]
        struct Record {
            ts: String,
            c1: i32,
        }

        let mut cnt = 0;
        let mut records: Vec<Record> = Vec::new();

        let timeout = Timeout::Duration(Duration::from_secs(2));

        loop {
            if let Some((offset, mut message)) = consumer.recv_timeout(timeout).await? {
                if let Some(data) = message.data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        cnt += block.nrows();
                        records.append(&mut block.deserialize().try_collect()?);
                    }
                }
                consumer.commit(offset).await?;
            }

            if cnt == 2 {
                break;
            }
        }

        consumer.unsubscribe().await;

        assert_eq!(records.len(), 2);

        assert_eq!(records[0].ts, "2025-01-01T12:00:00-05:00");
        assert_eq!(records[1].ts, "2025-01-02T15:30:00-05:00");

        assert_eq!(records[0].c1, 1);
        assert_eq!(records[1].c1, 2);

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic topic_1753759808",
            "drop database test_1753759808",
        ])
        .await?;

        Ok(())
    }
}
