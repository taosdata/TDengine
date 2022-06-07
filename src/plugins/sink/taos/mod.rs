use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use mdsn::IntoDsn;
use std::{
    cell::Cell,
    fmt::Debug,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
};

use futures::{Sink, Stream, TryStreamExt};
use taos::{
    block::{itypes::IsValue, Ty},
    query::Dsn,
    tmq::{Consumer, TmqBuilder},
};

use crate::{stream::stream::*, util::sync_table};
use taos::prelude::sync::*;

pub struct TaosSinkBuilder {
    dsn: Dsn,
    builder: Manager,
    worker: AtomicUsize,
    metrics: Arc<Summary>,
}

#[derive(Debug)]
pub struct TaosSink {
    id: usize,
    taos: Taos,
    metrics: Arc<Summary>,
}

impl XSinkBuilder for TaosSinkBuilder {
    type Error = Error;

    fn from_dsn<T: IntoDsn>(dsn: T) -> Result<Self, Self::Error> {
        let mut dsn = dsn.into_dsn()?;
        if let Some(db) = dsn.database.take() {
            let taos = Taos::from_dsn(&dsn)?;
            let db_info: Option<String> = taos.query_one(format!(
                "select * from information_schema.user_databases where name = \"{db}\""
            ))?;
            if db_info.is_none() {
                log::warn!("create database {db} with default parameters since it's not exist");
                taos.exec(format!("create database if not exists {db}"))?;
            } else {
                taos.exec(format!("create topic if not exists {db} as database {db}"))?;
                dsn.params.insert("topics".to_string(), db.to_string());
            }
            dsn.database = Some(db);
        } else {
            return Err(Error::InvalidDatabase(
                "taos sink plugin requires a database name".to_string(),
            ));
        }

        let builder = Manager::from_dsn(&dsn)?;
        Ok(Self {
            dsn,
            builder,
            worker: AtomicUsize::new(0),
            metrics: Default::default(),
        })
    }

    fn build_sink(&self) -> Result<XSink<Self::Error>, Self::Error> {
        let id = self
            .worker
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(TaosSink {
            id,
            taos: self.builder.connect()?,
            metrics: self.metrics.clone(),
        }
        .into())
    }

    fn summary(&self) -> &Summary {
        &self.metrics
    }
}

impl Sink<(&Taos, SyncBlock)> for TaosSink {
    type Error = Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: (&Taos, SyncBlock),
    ) -> Result<(), Self::Error> {
        let taos = item.0;
        let block = item.1;
        let idx = self.id;
        let db = block.tmq_db_name().unwrap();
        taos.exec(format!("use {db}"))?;
        let table = block.tmq_table_name().unwrap();
        log::info!("[{idx}] table name is {table}");

        if self.taos.exec(format!("describe {table}")).is_err() {
            sync_table(taos, &self.taos, db, &table)?;
        }

        let bind: Vec<TaosMultiBind> = block.columns_iter().map(|col| col.into()).collect();
        let questions = std::iter::repeat("?").take(bind.len()).join(",");
        let mut stmt = self
            .taos
            .stmt(format!("insert into {table} values({questions})"))?;
        stmt.multi_bind(&bind)?;
        stmt.execute()?;
        let inserted = stmt.affected_rows();
        log::info!("[{idx}] inserted {inserted} rows into {table}");
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl TaosxSink for TaosSink {
    type Error = Error;

    fn batch_size(&self) -> usize {
        1
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn consume_block(&mut self, taos: &Taos, block: &SyncBlock) -> Result<(), Self::Error> {
        let idx = self.id;
        let db = block.tmq_db_name().unwrap();
        taos.exec(format!("use {db}"))?;
        let table = block.tmq_table_name().unwrap();
        log::debug!("[{idx}] db: {db}, table: {table}");

        if self.taos.exec(format!("describe {table}")).is_err() {
            log::info!("[{idx}] synchronize table schema {table}");
            sync_table(taos, &self.taos, &db, &table)?;
            log::info!("[{idx}] synchronize table {table} done");
        }

        let bind: Vec<TaosMultiBind> = block.columns_iter().map(|col| col.into()).collect();
        let questions = std::iter::repeat("?").take(bind.len()).join(",");
        let mut stmt = self
            .taos
            .stmt(format!("insert into {table} values({questions})"))?;
        stmt.multi_bind(&bind)?;
        stmt.execute()?;
        let inserted = stmt.affected_rows();
        self.metrics.blocks.fetch_add(1, Ordering::SeqCst);
        self.metrics.rows.fetch_add(inserted, Ordering::SeqCst);
        log::info!("[{idx}] inserted {inserted} rows into {table}");
        Ok(())
    }

    fn consume_raw_block(&mut self, _: &[u8]) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn consume_schemaless_line(
        &mut self,
        item: &str,
        precision: SchemalessPrecision,
    ) -> Result<(), Self::Error> {
        self.taos
            .schemaless_insert([item], SchemalessProtocol::Line, precision)?;
        Ok(())
    }

    fn consume_schemaless_telnet(
        &mut self,
        item: &str,
        precision: SchemalessPrecision,
    ) -> Result<(), Self::Error> {
        self.taos
            .schemaless_insert([item], SchemalessProtocol::Telnet, precision)?;
        Ok(())
    }

    fn consume_schemaless_json(
        &mut self,
        item: &str,
        precision: SchemalessPrecision,
    ) -> Result<(), Self::Error> {
        self.taos
            .schemaless_insert([item], SchemalessProtocol::Json, precision)?;
        Ok(())
    }
}

#[taos::test(log_level = "debug", databases = 2)]
async fn test(taos: &Taos, databases: &[&str]) -> Result<(), Error> {
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

    let mut tmq = TmqBuilder::from_dsn(format!(
        "taos:///{db1}?topics={db1}&group.id={db1}&wait=1000"
    ))?
    .build()?;

    let db2 = databases[1];

    let mut sink = TaosSinkBuilder::from_dsn(format!("taos:///{db2}"))?
        .build_sink_for_protocol(SinkProtocol::Block)?;

    use futures::sink::SinkExt;

    while let Some(rs) = tmq.poll() {
        let mut rs = rs?;
        for block in rs.blocks_iter() {
            log::info!("send block");
            sink.send((taos, block)).await?;
        }
    }
    drop(tmq);

    taos.exec(format!("drop topic {db1}"))?;

    let taos2 = Taos::from_dsn(format!("taos:///{db2}"))?;

    let mut rs = taos2.query("select * from stb1")?;
    let values = rs.to_rows_vec();
    dbg!(values);

    Ok(())
}

#[taos::test(log_level = "debug", precision = "ns")]
async fn sink_sml_line(taos: &Taos, database: &str) -> Result<(), Error> {
    let lines = vec!["st,t1=abc c1=3i64,c3=\"def\",c2=false 1626006833639000000"];
    let builder = TaosSinkBuilder::from_dsn(format!("taos:///{database}"))?;
    let mut sink = builder.build_sink_for_protocol(SinkProtocol::Block)?;

    use futures::sink::SinkExt;

    for line in lines {
        sink.send(XLine::new_line(line)).await?;
    }

    let taos = builder.builder.connect()?;
    let table = taos.describe("st")?;
    dbg!(table);

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Weather {
        _ts: String,
        c1: i64,
        c2: bool,
        c3: String,
    }
    let mut rs = taos.query("select * from st")?;
    for block in rs.blocks_iter() {
        dbg!(block.deserialize_into_vec::<Weather>());
    }
    Ok(())
}
