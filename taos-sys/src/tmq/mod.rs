pub(crate) mod ffi;

use std::{
    ffi::{CStr, CString},
    fmt::Debug,
    str::FromStr,
    time::Duration,
};

pub(crate) use ffi::*;

use taos_query::{
    common::{raw_data_t, Precision, RawMeta},
    tmq::{
        AsAsyncConsumer, AsConsumer, AsyncOnSync, IsAsyncData, IsMeta, IsOffset, Timeout, VGroupId,
    },
    Dsn, DsnError, IntoDsn, RawBlock, TBuilder,
};

use crate::{conn::RawTaos, query::RawRes, Taos};

use taos_error::Error;

mod raw;

use raw::RawTmq;

use self::raw::{Conf, Topics};

impl RawRes {
    #[inline]
    pub fn tmq_topic_name(&self) -> Option<&str> {
        unsafe {
            let c = tmq_get_topic_name(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }
    #[inline]
    pub fn tmq_vgroup_id(&self) -> Option<VGroupId> {
        unsafe {
            let c = tmq_get_vgroup_id(self.as_ptr());
            if c == -1 {
                None
            } else {
                Some(c)
            }
        }
    }

    #[inline]
    pub fn tmq_table_name(&self) -> Option<&str> {
        unsafe {
            let c = tmq_get_table_name(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }
    #[inline]
    pub(crate) fn tmq_db_name(&self) -> Option<&str> {
        unsafe {
            let c = tmq_get_db_name(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }

    #[inline]
    pub(crate) fn tmq_message_type(&self) -> tmq_res_t {
        unsafe { tmq_get_res_type(self.as_ptr()) }
    }

    #[inline]
    pub(crate) fn tmq_get_json_meta(&self) -> CString {
        unsafe {
            let meta = tmq_get_json_meta(self.0);
            CString::from_raw(meta)
        }
    }

    #[inline]
    pub(crate) fn tmq_get_raw(&self) -> raw_data_t {
        let mut meta = raw_data_t {
            raw: std::ptr::null_mut(),
            raw_len: 0,
            raw_type: 0,
        };
        unsafe {
            let code = tmq_get_raw(self.0, &mut meta as _);
        }
        meta
    }
}

pub struct TmqBuilder {
    dsn: Dsn,
    conf: Conf,
    timeout: Timeout,
}

unsafe impl Send for TmqBuilder {}
unsafe impl Sync for TmqBuilder {}

// impl TmqBuilder {
//     fn from_dsn(dsn: impl IntoDsn) -> Result<Self, Error> {
//         let dsn = dsn
//             .into_dsn()
//             .map_err(|e| Error::from_string(format!("Parse dsn error: {}", e)))?;
//         let conf = Conf::from_dsn(&dsn)?;
//         Ok(Self {
//             dsn,
//             conf,
//             timeout: None,
//         })
//     }
//     fn with_timeout(mut self, timeout: Duration) -> Self {
//         self.timeout = Some(timeout);
//         self
//     }
//     fn build(&self) -> Result<Consumer, Error> {
//         self.conf.build().map(|tmq| Consumer {
//             tmq,
//             timeout: self.timeout.clone(),
//         })
//     }
// }

impl TBuilder for TmqBuilder {
    type Target = Consumer;

    type Error = Error;

    fn available_params() -> &'static [&'static str] {
        &["group.id", "client.id", "timeout", "enable.auto.commit"]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        let dsn = dsn
            .into_dsn()
            .map_err(|e| Error::from_string(format!("Parse dsn error: {}", e)))?;
        let conf = Conf::from_dsn(&dsn)?;
        let timeout = if let Some(timeout) = dsn.params.get("timeout") {
            Timeout::from_str(&timeout).map_err(Error::from_any)?
        } else {
            Timeout::from_millis(500)
        };
        Ok(Self { dsn, conf, timeout })
    }

    fn client_version() -> &'static str {
        RawTaos::version()
    }

    fn ping(&self, _: &mut Self::Target) -> Result<(), Self::Error> {
        self.build().map(|_| ())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> Result<Self::Target, Self::Error> {
        self.conf.build().map(|tmq| Consumer {
            tmq,
            timeout: self.timeout.clone(),
        })
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
}

impl Drop for Offset {
    fn drop(&mut self) {
        self.0.drop();
    }
}

#[derive(Debug)]
pub struct Consumer {
    tmq: RawTmq,
    timeout: Timeout,
}

unsafe impl Send for Consumer {}
unsafe impl Sync for Consumer {}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.tmq.unsubscribe();
        self.tmq.close();
    }
}

pub struct Messages {
    tmq: RawTmq,
    timeout: Option<Duration>,
}

impl Iterator for Messages {
    type Item = (Offset, MessageSet);

    fn next(&mut self) -> Option<Self::Item> {
        self.tmq
            .poll_timeout(self.timeout.map(|t| t.as_millis() as i64).unwrap_or(-1))
            .map(|raw| (Offset(raw), MessageSet::new(raw)))
    }
}

pub struct Meta {
    raw: RawRes,
}

impl AsyncOnSync for Meta {}

impl IsMeta for Meta {
    type Error = Error;

    fn as_raw_meta(&self) -> Result<RawMeta, Self::Error> {
        let raw = self.raw.tmq_get_raw();

        let mut data = Vec::new();

        data.extend(raw.raw_len.to_le_bytes());

        data.extend(raw.raw_type.to_le_bytes());

        data.extend(unsafe {
            std::slice::from_raw_parts(raw.raw as *const u8, raw.raw_len as usize)
        });
        Ok(RawMeta::new(data.into()))
    }

    fn as_json_meta(&self) -> Result<taos_query::common::JsonMeta, Self::Error> {
        let meta = serde_json::from_slice(self.raw.tmq_get_json_meta().as_bytes())
            .map_err(|err| Error::from_string(err.to_string()))?;
        Ok(meta)
    }
}
impl Meta {
    fn new(raw: RawRes) -> Self {
        Self { raw }
    }

    pub fn to_raw(&self) -> raw_data_t {
        self.raw.tmq_get_raw()
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::from_slice(self.raw.tmq_get_json_meta().as_bytes())
            .expect("meta json should always be valid json format")
    }

    pub fn to_sql(&self) -> String {
        todo!()
    }
}
pub struct Data {
    raw: RawRes,
    precision: Precision,
}

impl Data {
    fn new(raw: RawRes) -> Self {
        Self {
            precision: raw.precision(),
            raw,
        }
    }
}

#[async_trait::async_trait]
impl IsAsyncData for Data {
    type Error = Error;

    async fn as_raw_data(&self) -> Result<taos_query::common::RawData, Self::Error> {
        Ok(self.raw.tmq_get_raw().into())
    }

    async fn fetch_raw_block(&self) -> Result<Option<RawBlock>, Self::Error> {
        Ok(self.raw.fetch_raw_message(self.precision))
    }
}

pub enum MessageSet {
    Meta(Meta),
    Data(Data),
}

impl MessageSet {
    fn new(raw: RawRes) -> Self {
        match raw.tmq_message_type() {
            tmq_res_t::TMQ_RES_INVALID => unreachable!(),
            tmq_res_t::TMQ_RES_DATA => Self::Data(Data::new(raw)),
            tmq_res_t::TMQ_RES_TABLE_META => Self::Meta(Meta::new(raw)),
        }
    }
}

pub struct MessageSetIter {
    raw: RawRes,
    msg_type: tmq_res_t,
    precision: Precision,
}

impl Iterator for Data {
    type Item = Result<RawBlock, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.fetch_raw_message(self.precision).map(Ok)
    }
}

impl Iterator for MessageSet {
    type Item = RawBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MessageSet::Meta(data) => None,
            MessageSet::Data(data) => data.raw.fetch_raw_message(data.precision),
        }
    }
}

impl AsConsumer for Consumer {
    type Error = Error;

    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> Result<(), Self::Error> {
        let topics = Topics::from_topics(topics.into_iter().map(|s| s.into()))?;
        self.tmq.subscribe(&topics)
    }

    fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> Result<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
        Self::Error,
    > {
        Ok(self.tmq.poll_timeout(timeout.as_raw_timeout()).map(|raw| {
            (
                Offset(raw),
                match raw.tmq_message_type() {
                    tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                    tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
                    tmq_res_t::TMQ_RES_TABLE_META => {
                        taos_query::tmq::MessageSet::Meta(Meta::new(raw))
                    }
                },
            )
        }))
    }

    fn commit(&self, offset: Self::Offset) -> Result<(), Self::Error> {
        self.tmq.commit_sync(offset.0).map(|_| ())
    }
}

// impl AsyncOnSync for Consumer {}

#[async_trait::async_trait]
impl AsAsyncConsumer for Consumer {
    type Error = Error;

    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> Result<(), Self::Error> {
        let topics = Topics::from_topics(topics.into_iter().map(|s| s.into()))?;
        self.tmq.subscribe(&topics)
    }

    async fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> Result<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
        Self::Error,
    > {
        Ok(self.tmq.poll_timeout(timeout.as_raw_timeout()).map(|raw| {
            (
                Offset(raw),
                match raw.tmq_message_type() {
                    tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                    tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
                    tmq_res_t::TMQ_RES_TABLE_META => {
                        taos_query::tmq::MessageSet::Meta(Meta::new(raw))
                    }
                },
            )
        }))
    }

    async fn commit(&self, offset: Self::Offset) -> Result<(), Self::Error> {
        self.tmq.commit(offset.0).await.map(|_| ())
    }

    fn default_timeout(&self) -> Timeout {
        self.timeout
    }
}
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::RawTaos;

    use super::TmqBuilder;

    #[test]
    fn meta() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        use std::ptr::null;
        let host = null();
        let user = null();
        let pass = null();
        let db = null();
        let port = 0;
        let taos = RawTaos::connect(host, user, pass, db, port)?;
        let db = "tmq_meta";
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

        let builder = TmqBuilder::from_dsn("taos://localhost:6030/db?group.id=5")?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            println!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    dbg!(json);
                    taos.write_raw_meta(meta.to_raw())?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        let (nrows, ncols) = (raw.nrows(), raw.ncols());
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

        let query = taos.query("describe stb1")?;
        for row in query {
            let raw = row?;
            dbg!(raw);
        }

        Ok(())
    }

    #[test]
    fn test_ws_tmq_meta_sync() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        pretty_env_logger::formatted_builder()
            .filter_level(log::LevelFilter::Debug)
            .init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build()?;
        taos.exec_many([
            "drop database if exists ws_abc1",
            "create database ws_abc1",
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
            "drop table `table`",
            // kind 10: drop child table
            "drop table `tb2` `tb1`",
            // kind 11: drop super table
            "drop table `stb2`",
            "drop table `stb1`",
        ])?;

        taos.exec_many([
            "drop database if exists db2",
            "create database if not exists db2",
            "use db2",
        ])?;

        let builder = TmqBuilder::from_dsn("taos://localhost:6030?group.id=10&timeout=1000ms")?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["ws_abc1"])?;

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
                    taos.write_meta(raw)?;

                    let json = meta.as_json_meta()?;
                    match &json {
                        taos_query::common::JsonMeta::Create(m) => match m {
                            taos_query::common::MetaCreate::Super {
                                table_name,
                                columns,
                                tags,
                            } => {
                                let desc = taos.describe(table_name.as_str())?;
                                dbg!(desc);
                            }
                            taos_query::common::MetaCreate::Child {
                                table_name,
                                using,
                                tags,
                                tag_num,
                            } => {
                                let desc = taos.describe(table_name.as_str())?;
                                dbg!(desc);
                            }
                            taos_query::common::MetaCreate::Normal {
                                table_name,
                                columns,
                            } => {
                                let desc = taos.describe(table_name.as_str())?;
                                dbg!(desc);
                            }
                        },
                        taos_query::common::JsonMeta::Alter(_) => (),
                        taos_query::common::JsonMeta::Drop(_) => (),
                    }

                    // meta data can be write to an database seamlessly by raw or json (to sql).
                    let sql = dbg!(json.to_string());
                    if let Err(err) = taos.exec(sql) {
                        match err.errno() {
                            Code::TAG_ALREADY_EXIST => log::info!("tag already exists"),
                            Code::TAG_NOT_EXIST => log::debug!("tag not exist"),
                            Code::COLUMN_EXISTS => log::info!("column already exists"),
                            Code::COLUMN_NOT_EXIST => log::debug!("column not exists"),
                            Code::INVALID_COLUMN_NAME => log::info!("invalid column name"),
                            Code::MODIFIED_ALREADY => log::debug!("modified already done"),
                            Code::TABLE_NOT_EXIST => log::debug!("table does not exists"),
                            Code::STABLE_NOT_EXIST => log::debug!("stable does not exists"),
                            Code::INVALID_ROW_BYTES => log::debug!("invalid row bytes"),
                            Code::DUPLICATED_COLUMN_NAMES => log::debug!("duplicated column names"),
                            Code::NO_COLUMN_CAN_BE_DROPPED => {
                                log::debug!("no column can be dropped")
                            }
                            _ => {
                                log::error!("{:?}", err);
                                panic!("{}", err);
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
            }
            consumer.commit(offset)?;
        }

        consumer.unsubscribe();

        taos.exec_many(["drop database db2", "drop database ws_abc1"])?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_meta() -> anyhow::Result<()> {
        use futures::TryStreamExt;
        use taos_query::prelude::*;

        pretty_env_logger::formatted_builder()
            .filter_level(log::LevelFilter::Debug)
            .init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build()?;
        taos.exec_many([
            "drop database if exists ws_abc1",
            "create database ws_abc1",
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
            "drop table `table`",
            // kind 10: drop child table
            "drop table `tb2` `tb1`",
            // kind 11: drop super table
            "drop table `stb2`",
            "drop table `stb1`",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists db2",
            "create database if not exists db2",
            "use db2",
        ])
        .await?;

        let builder = TmqBuilder::from_dsn("taos:///?group.id=10&timeout=1000ms")?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["ws_abc1"]).await?;

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
                        taos.write_raw_meta(raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = dbg!(json.to_string());
                        if let Err(err) = taos.exec(sql).await {
                            match err.errno() {
                                Code::TAG_ALREADY_EXIST => log::info!("tag already exists"),
                                Code::TAG_NOT_EXIST => log::debug!("tag not exist"),
                                Code::COLUMN_EXISTS => log::info!("column already exists"),
                                Code::COLUMN_NOT_EXIST => log::debug!("column not exists"),
                                Code::INVALID_COLUMN_NAME => log::info!("invalid column name"),
                                Code::MODIFIED_ALREADY => log::debug!("modified already done"),
                                Code::TABLE_NOT_EXIST => log::debug!("table does not exists"),
                                Code::STABLE_NOT_EXIST => log::debug!("stable does not exists"),
                                Code::INVALID_ROW_BYTES => log::debug!("invalid row bytes"),
                                Code::DUPLICATED_COLUMN_NAMES => {
                                    log::debug!("duplicated column names")
                                }
                                Code::NO_COLUMN_CAN_BE_DROPPED => {
                                    log::debug!("no column can be dropped")
                                }
                                _ => {
                                    log::error!("{:?}", err);
                                    panic!("{}", err);
                                }
                            }
                        }
                    }
                    MessageSet::Data(mut data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.next().transpose()? {
                            dbg!(data.table_name());
                            dbg!(data);
                        }
                    }
                }
                consumer.commit(offset).await?;
                Ok(())
            })
            .await?;

        consumer.unsubscribe().await;

        taos.exec_many([
            "drop database db2",
            "drop topic ws_abc1",
            "drop database ws_abc1",
        ])
        .await?;
        Ok(())
    }
}
