pub(crate) mod ffi;

use core::time;
use std::{
    ffi::{CStr, CString},
    fmt::Debug,
    time::Duration,
};

pub use ffi::*;
use taos_query::{
    common::{Precision, RawMeta},
    tmq::{AsAsyncConsumer, AsConsumer, AsyncOnSync, IsMeta, IsOffset},
    Dsn, IntoDsn, RawData,
};

use crate::{
    ffi::{taos_free_result, TAOS_RES},
    query::RawRes,
    VGroupId,
};

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
    pub(crate) fn tmq_get_raw_meta(&self) -> tmq_raw_meta {
        let mut meta = tmq_raw_meta {
            raw_meta: std::ptr::null_mut(),
            raw_meta_len: 0,
            raw_meta_type: 0,
        };
        unsafe {
            let code = tmq_get_raw_meta(self.0, &mut meta as _);
            debug_assert!(
                code == 0,
                "tmq raw meta should always available for meta message"
            );
        }
        meta
    }
}

pub struct Builder {
    dsn: Dsn,
    conf: Conf,
    timeout: Option<Duration>,
}

impl Builder {
    pub fn from_dsn(dsn: impl IntoDsn) -> Result<Self, Error> {
        let dsn = dsn
            .into_dsn()
            .map_err(|e| Error::from_string(format!("Parse dsn error: {}", e)))?;
        let conf = Conf::from_dsn(&dsn)?;
        Ok(Self {
            dsn,
            conf,
            timeout: None,
        })
    }
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    pub fn build(&self) -> Result<Consumer, Error> {
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
    timeout: Option<Duration>,
}

unsafe impl Send for Consumer {}
unsafe impl Sync for Consumer {}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.tmq.close();
    }
}

// impl Consumer {
//     pub fn subscribe<T: AsRef<str>>(&mut self, topics: &[T]) -> Result<(), Error> {
//         let topics = Topics::from_topics(topics.into_iter().map(|s| s.as_ref()))?;
//         self.tmq.subscribe(&topics)
//     }

//     fn commit(&self, offset: Offset) -> Result<(), Offset> {
//         self.tmq.commit_sync(offset.0).map_err(|err| offset)
//     }

//     pub fn message_sets(&mut self) -> Messages {
//         Messages {
//             tmq: self.tmq,
//             timeout: self.timeout.clone(),
//         }
//     }
// }

// impl IntoIterator for &mut Consumer {
//     type Item = (Offset, MessageSet);

//     type IntoIter = Messages;

//     fn into_iter(self) -> Self::IntoIter {
//         self.message_sets()
//     }
// }

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
        let raw = self.raw.tmq_get_raw_meta();

        let mut data = Vec::new();

        data.extend(raw.raw_meta_len.to_le_bytes());

        data.extend(raw.raw_meta_type.to_le_bytes());

        data.extend(unsafe {
            std::slice::from_raw_parts(raw.raw_meta as *const u8, raw.raw_meta_len as usize)
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

    pub fn to_raw(&self) -> tmq_raw_meta {
        self.raw.tmq_get_raw_meta()
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
    type Item = RawData;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.fetch_raw_message(self.precision)
    }
}

impl Iterator for MessageSet {
    type Item = RawData;

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

impl AsyncOnSync for Consumer {}

// #[async_trait::async_trait]
// impl AsAsyncConsumer for Consumer {
//     type Error = Error;

//     type Offset = Offset;

//     type Meta = Meta;

//     type Data = Data;

//     async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
//         &mut self,
//         topics: I,
//     ) -> Result<(), Self::Error> {
//         let topics = Topics::from_topics(topics.into_iter().map(|s| s.into()))?;
//         self.tmq.subscribe(&topics)
//     }

//     async fn recv_timeout(
//         &self,
//         timeout: taos_query::tmq::Timeout,
//     ) -> Result<
//         Option<(
//             Self::Offset,
//             taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
//         )>,
//         Self::Error,
//     > {
//         Ok(self.tmq.poll_timeout(timeout.as_raw_timeout()).map(|raw| {
//             (
//                 Offset(raw),
//                 match raw.tmq_message_type() {
//                     tmq_res_t::TMQ_RES_INVALID => unreachable!(),
//                     tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
//                     tmq_res_t::TMQ_RES_TABLE_META => {
//                         taos_query::tmq::MessageSet::Meta(Meta::new(raw))
//                     }
//                 },
//             )
//         }))
//     }

//     async fn commit(&self, offset: Self::Offset) -> Result<(), Self::Error> {
//         self.tmq.commit(offset.0).await.map(|_| ())
//     }
// }
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::RawTaos;

    use super::Builder;

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

        let builder = Builder::from_dsn("taos://localhost:6030/db?group.id=5")?
            .with_timeout(Duration::from_millis(100));
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            println!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    dbg!(json);
                    taos.write_raw_meta(meta)?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
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
}
