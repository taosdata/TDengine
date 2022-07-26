use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use mdsn::{DsnError, IntoDsn};
use serde::Deserialize;

use std::{
    any::Any,
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    ffi::c_void,
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    str::FromStr,
    sync::{Arc, Weak},
    task::Poll,
};

use futures::{Sink, Stream, StreamExt, TryStreamExt};
use taos::{
    block::{itypes::IsValue, BlockStream, Field, Ty},
    prelude::AsyncFetchable,
    query::Dsn,
    tmq::{Consumer, TmqBuilder},
};

use crate::{
    plugins::sink::taos::TaosSinkBuilder,
    stream::{source::XSourceBuilder, stream::XSinkBuilder, transformer::Action},
    util::sync_table,
};

use crate::stream::stream::{SinkProtocol, TaosxSinkItem};

pub use taos::prelude::sync::*;

#[derive(Debug)]
pub enum TaosItem {
    Block(Arc<Taos>, RawData),
}

impl<'a> TaosxSinkItem for TaosItem {
    fn protocol(&self) -> SinkProtocol {
        match self {
            Self::Block(_, _) => SinkProtocol::Block,
        }
    }

    fn as_block(&self) -> (&Taos, &RawData) {
        match self {
            Self::Block(taos, block) => (taos.as_ref(), &block),
        }
    }

    fn as_raw_block(&self) -> Option<std::borrow::Cow<[u8]>> {
        None
    }

    fn as_record(&self) -> Option<std::borrow::Cow<[Value]>> {
        None
    }

    fn precision(&self) -> SchemalessPrecision {
        SchemalessPrecision::NonConfigured
    }

    fn as_schemaless_line(&self) -> Option<&str> {
        None
    }

    fn as_schemaless_telnet(&self) -> Option<&str> {
        None
    }

    fn as_schemaless_json(&self) -> Option<&str> {
        None
    }

    const PROTOCOL: SinkProtocol = SinkProtocol::__NoneExhaustive;
}

pub struct TaosSourceBuilder {
    from: Dsn,
    manager: Manager,
    tmq_builder: TmqBuilder,
    max_workers: usize,
    protocol: SinkProtocol,
}

#[derive(Debug)]
pub struct TaosSource {
    taos: Arc<Taos>,
    consumer: Consumer,
    protocol: SinkProtocol,
    rs: Option<ResultSet>,
    blocks: Option<BlockStream>,
}

impl Drop for TaosSource {
    fn drop(&mut self) {
        self.blocks.take();
        self.rs.take();
    }
}

use taos::prelude::Error;

impl XSourceBuilder for TaosSourceBuilder {
    type Error = Error;

    type Item = TaosItem;
    type XSource = TaosSource;

    const NAME: &'static str = "taos";

    fn from_dsn<T: IntoDsn>(dsn: T) -> Result<Self, Self::Error> {
        let mut dsn = dsn.into_dsn()?;
        if dsn.database.is_none() {
            Err(DsnError::RequireDatabase(
                "please input a database or topic/stream name".to_string(),
            ))?;
        }

        let mut max_workers = 0;
        let db = dsn.database.take().unwrap(); // unwrap is safe.

        let taos = Taos::from_dsn(&dsn)?;

        let topics: HashMap<_, _> = taos
            .topics()?
            .into_iter()
            .map(|topic| (topic.name().to_string(), topic))
            .collect();

        #[derive(Default, Deserialize)]
        struct VGroups {
            vgroups: usize,
        }
        // For `?topics=` options.
        if let Some(opt) = dsn.params.get("topics") {
            let topic_names = opt.split(",").map(|s| s.to_string()).collect_vec();
            if topic_names.is_empty() {
                return Err(Error::InvalidTopic("topics is empty".to_string()));
            }
            let mut db_name = None;
            {
                for topic_name in topic_names {
                    if let Some(topic) = topics.get(&topic_name) {
                        let db = topic.db_name();
                        let vgroups: VGroups = taos
                            .query_one(format!(
                            "select * from information_schema.user_databases where name = '{db}'"))?
                            .unwrap_or_default();
                        if max_workers < vgroups.vgroups {
                            max_workers = vgroups.vgroups;
                        }
                        if let Some(db1) = db_name.as_ref() {
                            if db1 != topic.db_name() {
                                // return Err(Error::InvalidTopic(
                                //     "topics should be in same database".to_string(),
                                // ));
                                log::warn!("subscribe topics in different database");
                            }
                        } else {
                            db_name = Some(topic.db_name().to_string());
                        }
                    } else {
                        return Err(Error::InvalidTopic(format!("topic {topic_name} not found")));
                    }
                }
            }
            dsn.database = db_name;
            // check group id
            if !dsn.params.contains_key("group.id") {
                let opt = opt.to_string();
                dsn.params.insert("group.id".to_string(), opt);
            }
        } else {
            // If no `topics` option in params, check if the database identity is topic(s) or not.
            dsn.params.insert("topics".to_string(), db.to_string());
            for db in db.split(",") {
                if let Some(topic) = topics.get(db) {
                    let db_name = topic.db_name();
                    dsn.database = Some(db_name.to_string());
                } else {
                    // todo: support subscribe stable/table.
                    let db_info: Option<String> = taos.query_one(format!(
                        "select * from information_schema.user_databases where name = \"{db}\""
                    ))?;
                    if db_info.is_none() {
                        return Err(Error::InvalidDatabase(db.to_string()));
                    }
                    taos.exec(format!("create topic if not exists {db} as database {db}"))?;
                    dsn.database = Some(db.to_string());
                }

                let vgroups: VGroups = taos
                    .query_one(format!(
                        "select * from information_schema.user_databases where name = '{db}'"
                    ))?
                    .unwrap_or_default();
                if max_workers < vgroups.vgroups {
                    max_workers = vgroups.vgroups;
                }
            }

            if !dsn.params.contains_key("group.id") {
                dsn.params.insert("group.id".to_string(), db.to_string());
            }
        }
        // check group id
        if !dsn.params.contains_key("wait") {
            dsn.params.insert("wait".to_string(), "10".to_string());
        }
        let manager = Manager::from_dsn(dsn.clone())?;

        Ok(Self {
            from: dsn.clone(),
            manager,
            tmq_builder: TmqBuilder::from_dsn(dsn)?,
            max_workers,
            protocol: SinkProtocol::Block,
        })
    }

    fn dsn(&self) -> Cow<Dsn> {
        Cow::Borrowed(&self.from)
    }

    fn build_source(&mut self) -> Result<Self::XSource, Self::Error> {
        Ok(TaosSource {
            taos: Arc::new(self.manager.connect()?),
            consumer: self.tmq_builder.build()?,
            protocol: SinkProtocol::Block,
            rs: None,
            blocks: None,
        })
    }

    fn protocol(&self) -> SinkProtocol {
        todo!()
    }

    fn max_workers(&self) -> usize {
        self.max_workers
    }

    fn database_options(&self) -> Vec<(String, String)> {
        unimplemented!()
    }

    fn schema_iter<I>(&self) -> I
    where
        I: Iterator<Item = crate::stream::source::XSchema>,
    {
        unimplemented!()
    }
}

impl Stream for TaosSource {
    type Item = Result<TaosItem, Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.protocol {
            SinkProtocol::ResultSet => todo!(),
            SinkProtocol::Block => {
                if let Some(blocks) = self.blocks.as_mut() {
                    match blocks.poll_next_unpin(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(None) => {
                            self.blocks = None;
                            self.rs = None;
                            match self.consumer.poll_next_unpin(cx) {
                                Poll::Pending => Poll::Pending,
                                Poll::Ready(None) => Poll::Ready(None),
                                Poll::Ready(Some(rs)) => match rs {
                                    Ok(mut rs) => {
                                        self.blocks = Some(rs.block_stream());
                                        self.rs = Some(rs);
                                        self.blocks.as_mut().unwrap().poll_next_unpin(cx).map(
                                            |block| {
                                                block.map(|block| {
                                                    Ok(TaosItem::Block(self.taos.clone(), block))
                                                })
                                            },
                                        )
                                    }
                                    Err(err) => Poll::Ready(Some(Err(err.into()))),
                                },
                            }
                        }
                        Poll::Ready(Some(block)) => {
                            Poll::Ready(Some(Ok(TaosItem::Block(self.taos.clone(), block))))
                        }
                    }
                } else {
                    match self.consumer.poll_next_unpin(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(None) => Poll::Ready(None),
                        Poll::Ready(Some(rs)) => match rs {
                            Ok(mut rs) => {
                                self.blocks = Some(rs.block_stream());
                                self.rs = Some(rs);
                                self.blocks
                                    .as_mut()
                                    .unwrap()
                                    .poll_next_unpin(cx)
                                    .map(|block| {
                                        block.map(|block| {
                                            Ok(TaosItem::Block(self.taos.clone(), block))
                                        })
                                    })
                            }
                            Err(_) => Poll::Ready(None),
                        },
                    }
                }
            }
            SinkProtocol::RawBlock => todo!(),
            SinkProtocol::Record => todo!(),
            SinkProtocol::SmlLine => todo!(),
            SinkProtocol::SmlTelnet => todo!(),
            SinkProtocol::SmlJson => todo!(),
            _ => todo!(),
        }
    }
}

#[taos::test(log_level = "trace", databases = 2)]
async fn test(taos: &Taos, databases: &[&str]) -> anyhow::Result<()> {
    taos.exec_many([
        "create table tb1 (ts timestamp, c_bool bool, c_int int, c_binary binary(10))",
        "create table tb2 (ts timestamp, c_bool bool, c_int int, c_binary binary(10))",
        "create table stb1 (ts timestamp, c_bool bool, c_int int, c_binary binary(10)) tags(c_i8 tinyint)",
        "insert into tb1 values(now, NULL, NULL, NULL) (now+1s, false, 0, 'abc')",
        "insert into tb2 values(now, NULL, NULL, NULL) (now+1s, false, 0, 'abc')",
        "insert into tb3 using stb1 tags(3) values(now, NULL, NULL, NULL) (now+1s, false, 0, 'abc')",
        "insert into tb4 using stb1 tags(4) values(now, NULL, NULL, NULL) (now+1s, false, 0, 'abc')",
    ])?;

    let db1 = databases[0];
    taos.exec(format!("create topic {db1} as database {db1}"))?;

    let mut source_builder = TaosSourceBuilder::from_dsn(format!("taos:///{db1}?wait=1000"))?;
    let mut source = source_builder.build_source()?;

    let mut rows = 0;
    let mut tables = std::collections::BTreeSet::new();
    while let Some(Ok(item)) = source.next().await {
        println!("{:?}", item);
        let block = item.as_block().1;
        rows += block.num_of_rows();
        let table = block.table_name().unwrap();
        tables.insert(table.to_string());
    }
    anyhow::ensure!(rows == 8);
    anyhow::ensure!(tables.into_iter().collect_vec() == vec!["tb1", "tb2", "tb3", "tb4"]);

    taos.exec_many([
        "insert into tb1 values(now, NULL, NULL, NULL) (now+1s, false, 0, 'abc')",
        "insert into tb2 values(now, NULL, NULL, NULL) (now+1s, false, 0, 'abc')",
        "insert into tb3 using stb1 tags(3) values(now, NULL, NULL, NULL) (now+1s, false, 0, 'abc')",
        "insert into tb4 using stb1 tags(4) values(now, NULL, NULL, NULL) (now+1s, false, 0, 'abc')",
    ])?;

    let db2 = databases[1];
    let transformers = vec![Action::from_str("add-tag:f1(10)=value1")?];
    let mut sink = TaosSinkBuilder::from_dsn(format!("taos:///{db2}"))?
        .with_transformer(transformers)
        .build_sink()?;

    source.forward(&mut sink).await?;

    Ok(())
}

#[taos::test(log_level = "debug")]
async fn test_sync() -> anyhow::Result<()> {
    return Ok(());
    let db1 = "abc1";
    // if taos.query_one::<String>("select name from information_schema.user_databases where name = abc1")?.is_none() {
    //     return Ok(());
    // }
    let mut source_builder =
        TaosSourceBuilder::from_dsn(format!("taos:///{db1}?group.id=1&wait=20000"))?;
    let source = source_builder.build_source()?;
    let db2 = "abc2";
    let mut sink = TaosSinkBuilder::from_dsn(format!("taos:///{db2}"))?.build_sink()?;

    source.forward(&mut sink).await?;

    Ok(())
}
