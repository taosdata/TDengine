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
    block::{itypes::IsValue, Describe, RawBlock, Ty},
    helpers::{ColumnMeta, Described},
    query::Dsn,
    tmq::{Consumer, TmqBuilder},
};

use crate::{
    stream::{
        stream::*,
        transformer::{Action, AddTag, AddTagOpts, Select},
    },
    util::sync_table,
};
use taos::prelude::sync::*;

pub struct TaosSinkBuilder {
    dsn: Dsn,
    builder: Manager,
    worker: AtomicUsize,
    transformer: Arc<Option<Vec<Action>>>,
    metrics: Arc<Summary>,
}

#[derive(Debug)]
pub struct TaosSink {
    id: usize,
    taos: Taos,
    transformer: Arc<Option<Vec<Action>>>,
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
            transformer: Arc::new(None),
            worker: AtomicUsize::new(0),
            metrics: Default::default(),
        })
    }

    fn with_transformer(mut self, transformer: Vec<Action>) -> Self {
        if transformer.len() > 0 {
            self.transformer = Arc::new(Some(transformer));
        }
        self
    }

    fn build_sink(&self) -> Result<XSink<Self::Error>, Self::Error> {
        let id = self
            .worker
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(TaosSink {
            id,
            taos: self.builder.connect()?,
            transformer: self.transformer.clone(),
            metrics: self.metrics.clone(),
        }
        .into())
    }

    fn summary(&self) -> &Summary {
        &self.metrics
    }
}

pub fn sync_table_with_transformer(
    from: &Taos,
    to: &Taos,
    db: &str,
    table: &str,
    transformer: &[Action],
) -> Result<(), Error> {
    use taos::prelude::sync::*;
    assert!(transformer.len() > 0);

    // let stable: Option<String> = from
    //     .query_one(format!(
    //         "select stable_name from information_schema.user_tables where db_name = '{db}' and table_name = \"{table}\""
    //     ))?
    //     .unwrap();

    // if let Some(stable) = stable {
    #[derive(Debug, serde::Deserialize)]
    struct Table {
        stable_name: Option<String>,
    }

    let stable: Table = from
        .query_one(format!(
            "select * from information_schema.user_tables where db_name = '{db}' and table_name = \"{table}\""
        ))?
        .unwrap();
    if let Some(stable) = stable.stable_name {
        let desc = from.describe(&format!("{db}.`{stable}`"))?;
        let mut stable2 = stable.to_string();

        let transformed_desc =
            transformer
                .iter()
                .fold(desc.clone(), |mut desc, action| match action {
                    Action::AddTag(add_tag) => {
                        desc.push(ColumnMeta::Tag(Described {
                            field: add_tag.name.clone(),
                            ty: Ty::VarChar,
                            length: add_tag.len,
                        }));
                        desc
                    }
                    Action::Select(select) => match select {
                        Select::Subset { subset } => desc
                            .into_iter()
                            .filter(|f| subset.contains(&f.field))
                            .collect(),
                        Select::Rename { rename } => desc
                            .into_iter()
                            .map(|mut f| {
                                if let Some(v) = rename.get(f.field()) {
                                    f.field = v.to_string();
                                    f
                                } else {
                                    f
                                }
                            })
                            .collect(),
                        Select::Exclude { exclude } => desc
                            .into_iter()
                            .filter(|f| !exclude.contains(&f.field))
                            .collect(),
                    },
                    Action::RenameChildTable(rename) | Action::RenameTable(rename) => {
                        match rename {
                            crate::stream::transformer::RenameOpts::Prefix { prefix } => {
                                stable2 = format!("{prefix}{stable}");
                            }
                            crate::stream::transformer::RenameOpts::Suffix { suffix } => {
                                stable2 = format!("{stable}{suffix}");
                            }
                            crate::stream::transformer::RenameOpts::Template { template } => {
                                stable2 = template.replace("{{ name }}", &stable);
                            }
                        }
                        desc
                    }
                    _ => desc,
                });

        log::trace!("create {stable2}");
        let sql = transformed_desc.to_create_table_sql(&stable2);
        to.exec(sql)?;

        let names = transformer.iter().fold(
            desc.tag_names().map(ToString::to_string).collect_vec(),
            |names, action| match action {
                Action::Select(select) => match select {
                    Select::Subset { subset } => names
                        .into_iter()
                        .filter(|name| subset.contains(name))
                        .collect_vec(),
                    Select::Rename { rename } => names
                        .into_iter()
                        .map(|f| {
                            if let Some(v) = rename.get(&f) {
                                format!("`{f}` as `{v}`")
                            } else {
                                f
                            }
                        })
                        .collect_vec(),
                    Select::Exclude { exclude } => names
                        .into_iter()
                        .filter(|name| !exclude.contains(name))
                        .collect_vec(),
                },
                _ => names,
            },
        );

        let names = names
            .iter()
            .map(|name| format!("last(`{name}`) as `{name}`"))
            .join(",");
        let children: Vec<Vec<Value>> = from
            .query(format!(
                "select tbname,{names} from {stable} group by tbname"
            ))?
            .deserialize()
            .try_collect()?;

        let children = transformer
            .iter()
            .fold(children, |children, action| match action {
                Action::AddTag(add_tag) => match &add_tag.opts {
                    AddTagOpts::Value { value } => children
                        .into_iter()
                        .map(|mut v| {
                            v.push(Value::VarChar(value.clone()));
                            v
                        })
                        .collect_vec(),
                    AddTagOpts::Template { template: _ } => todo!(),
                },
                Action::RenameChildTable(rename) | Action::RenameTable(rename) => match rename {
                    crate::stream::transformer::RenameOpts::Prefix { prefix } => children
                        .into_iter()
                        .map(|mut child| {
                            let name = child[0].strict_as_str();
                            child[0] = Value::VarChar(format!("{prefix}{name}"));
                            child
                        })
                        .collect_vec(),
                    crate::stream::transformer::RenameOpts::Suffix { suffix } => children
                        .into_iter()
                        .map(|mut child| {
                            let name = child[0].strict_as_str();
                            child[0] = Value::VarChar(format!("{name}{suffix}"));
                            child
                        })
                        .collect_vec(),
                    crate::stream::transformer::RenameOpts::Template { template } => children
                        .into_iter()
                        .map(|mut child| {
                            let name = child[0].strict_as_str();
                            child[0] = Value::VarChar(template.replace("{{ name }}", name));
                            child
                        })
                        .collect_vec(),
                },
                _ => children,
            });

        // todo: use par_iter to speed up tables creation.
        // todo: single table not work, blocked by https://jira.taosdata.com:18080/browse/TD-16117
        for child in children {
            let tbname = child[0].to_string().unwrap();
            let tags_values = child[1..].into_iter().map(|v| v.to_sql_value()).join(",");
            to.exec(format!(
                "create table if not exists {tbname} using {stable2} tags({tags_values})"
            ))
            .unwrap();
        }

        // let fields: Vec<Value> = from
        //     .query_one(format!(
        //         "select {names} from {stable} where tbname = '{table}'"
        //     ))?
        //     .unwrap();

        // let tags_values = fields.into_iter().map(|v| v.to_sql_value()).join(",");
        // to.exec(format!(
        //     "create table if not exists {table} using {stable} tags({tags_values})"
        // ))?;
    } else {
        let mut table = table.to_string();
        log::info!("describe table {table}");
        let desc = from.describe(&format!("{db}.`{table}`"))?;
        log::info!("table {table}: {desc:?}");

        let desc = transformer
            .iter()
            .fold(desc.clone(), |desc, action| match action {
                Action::Select(select) => match select {
                    Select::Subset { subset } => desc
                        .into_iter()
                        .filter(|f| subset.contains(&f.field))
                        .collect(),
                    Select::Rename { rename } => desc
                        .into_iter()
                        .map(|mut f| {
                            if let Some(v) = rename.get(f.field()) {
                                f.field = v.to_string();
                                f
                            } else {
                                f
                            }
                        })
                        .collect(),
                    Select::Exclude { exclude } => desc
                        .into_iter()
                        .filter(|f| !exclude.contains(&f.field))
                        .collect(),
                },
                Action::RenameTable(rename) => {
                    match rename {
                        crate::stream::transformer::RenameOpts::Prefix { prefix } => {
                            table = format!("{prefix}{table}");
                        }
                        crate::stream::transformer::RenameOpts::Suffix { suffix } => {
                            table = format!("{table}{suffix}");
                        }
                        crate::stream::transformer::RenameOpts::Template { template } => {
                            table = template.replace("{{ name }}", &table);
                        }
                    }
                    desc
                }
                _ => desc,
            });
        let sql = desc.to_create_table_sql(&table);
        log::info!("exec sql: {sql}");
        to.exec(sql).unwrap();
    }
    Ok(())
}

impl Sink<(&Taos, RawBlock)> for TaosSink {
    type Error = Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: (&Taos, RawBlock),
    ) -> Result<(), Self::Error> {
        self.consume_block(item.0, &item.1)
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

    fn consume_block(&mut self, taos: &Taos, block: &RawBlock) -> Result<(), Self::Error> {
        let idx = self.id;
        let db = block.tmq_db_name().unwrap();
        taos.exec(format!("use {db}"))?;
        let table = block.table_name().unwrap();
        log::debug!("[{idx}] db: {db}, table: {table}");
        debug_assert!(!table.is_empty());

        if let Some(transformer) = self.transformer.as_ref() {
            let mut table2 = table.to_string();
            let fields = block.fields();

            let mut bind: Vec<TaosMultiBind> = Vec::new();

            for action in transformer {
                match action {
                    Action::Select(Select::Subset { subset }) => {
                        bind = block
                            .columns_iter()
                            .zip(fields)
                            .filter_map(|(col, field)| {
                                if subset.contains(&field.name().to_string()) {
                                    Some(col.into())
                                } else {
                                    None
                                }
                            })
                            .collect()
                    }
                    Action::Select(Select::Exclude { exclude }) => {
                        bind = block
                            .columns_iter()
                            .zip(fields)
                            .filter_map(|(col, field)| {
                                if !exclude.contains(&field.name().to_string()) {
                                    Some(col.into())
                                } else {
                                    None
                                }
                            })
                            .collect()
                    }
                    Action::RenameChildTable(rename) | Action::RenameTable(rename) => {
                        match rename {
                            crate::stream::transformer::RenameOpts::Prefix { prefix } => {
                                table2 = format!("{prefix}{table}");
                            }
                            crate::stream::transformer::RenameOpts::Suffix { suffix } => {
                                table2 = format!("{table}{suffix}");
                            }
                            crate::stream::transformer::RenameOpts::Template { template } => {
                                table2 = template.replace("{{ name }}", &table);
                            }
                        }
                    }
                    _ => (),
                }
            }
            if bind.is_empty() {
                bind = block.columns_iter().map(Into::into).collect();
            }

            if self.taos.exec(format!("describe {table2}")).is_err() {
                sync_table_with_transformer(taos, &self.taos, db, &table, &transformer)?;
            }
            let questions = std::iter::repeat("?").take(bind.len()).join(",");
            let mut stmt = self
                .taos
                .stmt(format!("insert into {table2} values({questions})"))?;
            stmt.multi_bind(&bind)?;
            stmt.execute()?;
            let inserted = stmt.affected_rows();
            self.metrics.blocks.fetch_add(1, Ordering::SeqCst);
            self.metrics.rows.fetch_add(inserted, Ordering::SeqCst);
            log::info!("[{idx}] inserted {inserted} rows into {table2}");
            return Ok(());
        }

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

#[taos::test(log_level = "trace", databases = 2)]
async fn test_with_action_add_tag(taos: &Taos, databases: &[&str]) -> Result<(), Error> {
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

    let mut tmq =
        TmqBuilder::from_dsn(format!("taos:///{db1}?topics={db1}&group.id={db1}"))?.build()?;

    let db2 = databases[1];

    let mut sink = TaosSinkBuilder::from_dsn(format!("taos:///{db2}"))?
        .with_transformer(vec![Action::AddTag(AddTag {
            name: "f1".to_string(),
            len: 10,
            opts: AddTagOpts::value("v1"),
        })])
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
    taos2.exec("reset query cache")?;

    let mut rs = taos2.query("select tbname, f1 from stb1")?;
    let values = rs.to_rows_vec();
    dbg!(values);

    let mut rs = taos2.query("select * from tb1")?;
    let values = rs.to_rows_vec();
    dbg!(values);

    let mut rs = taos2.query("select * from tb2")?;
    let values = rs.to_rows_vec();
    dbg!(values);

    Ok(())
}
#[taos::test(log_level = "trace", databases = 2)]
async fn test_with_transformer(taos: &Taos, databases: &[&str]) -> Result<(), Error> {
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

    let mut tmq =
        TmqBuilder::from_dsn(format!("taos:///{db1}?topics={db1}&group.id={db1}"))?.build()?;

    let db2 = databases[1];

    let mut sink = TaosSinkBuilder::from_dsn(format!("taos:///{db2}"))?
        .with_transformer(vec![
            Action::AddTag(AddTag {
                name: "f1".to_string(),
                len: 10,
                opts: AddTagOpts::value("v1"),
            }),
            Action::Select(crate::stream::transformer::Select::exclude(vec![
                "c_bool".to_string()
            ])),
            Action::RenameTable(crate::stream::transformer::RenameOpts::prefix("p_")),
        ])
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

    let mut rs = taos2.query("select * from p_stb1")?;
    let values = rs.to_rows_vec();
    assert_eq!(values.len(), 4);
    dbg!(values);

    let mut rs = taos2.query("select * from p_tb1")?;
    let values = rs.to_rows_vec();
    dbg!(values);

    let mut rs = taos2.query("select * from p_tb2")?;
    let values = rs.to_rows_vec();
    dbg!(values);

    Ok(())
}

#[taos::test(log_level = "debug", precision = "ns")]
async fn sink_sml_line(taos: &Taos) -> Result<(), Error> {
    let database = "_rs_sml_";
    taos.exec(format!("drop database if exists {database}"))?;
    taos.exec(format!("create database {database} schemaless 1"))?;
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
