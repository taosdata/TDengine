use std::collections::HashMap;
use std::ops::Deref;
use std::task::Poll;
use std::thread;

use anyhow::Result;
use clap::Args;
use futures::prelude::*;
use serde::Deserialize;
use taos::prelude::*;
use taos::query::Fetchable;
use taos::{query::Dsn, tmq::TmqBuilder};
use taosx::TaosOpts;

#[derive(Debug, Args)]
/// Import external files to TDengine.
pub(crate) struct App {
    #[clap(short, long)]
    /// A DSN(database source name) format string for source TDengine: taos:///db1, for eg.
    from: Dsn,
    #[clap(short, long)]
    /// A DSN(database source name) format string for target TDengine: taos:///db2, for eg.
    to: Dsn,
    /// Number of workers for TMQ consumers.
    #[clap(short = 'j', long)]
    workers: Option<usize>,
}

async fn check_target_db<'a>(to: &mut Dsn, from: &'a Dsn) -> Result<&'a str> {
    let db1 = from.database.as_ref().unwrap();
    if to.database.is_some() && Taos::from_dsn(to).is_ok() {
        return Ok(db1);
    }

    let db2 = to.database.as_ref().unwrap_or(db1).to_string();

    to.database = None;
    let taos2 = Taos::from_dsn(to)?;
    let taos1 = Taos::from_dsn(from)?;
    let db_opts = taos1
        .databases()
        .await?
        .into_iter()
        .filter(|db| &db.name == db1)
        .next()
        .unwrap()
        .props
        .to_string();
    let db_new = format!("create database if not exists {db2} {db_opts}");
    taos2.exec(db_new).await?;

    to.database = Some(db2);

    Ok(db1.as_str())
}

async fn sync_schema(from: &Taos, to: &Taos) -> Result<()> {
    let stables: Vec<String> = from
        .query("show stables")
        .await?
        .deserialize_stream()
        .try_collect()
        .await?;
    let mut stable_fields = HashMap::new();
    for stable in stables {
        // todo: use "show create" sql?
        // from.query(format!("show create stable {stable}")).await?;
        let desc = from.describe(&stable).await?;
        let sql = desc.to_create_table_sql(&stable);
        stable_fields.insert(stable, desc);
        to.exec(sql).await?;
    }

    #[derive(Deserialize)]
    struct Table {
        table_name: String,
        db_name: String,
        stable_name: Option<String>,
    }
    let tables: Vec<Table> = from
        .query("show tables")
        .await?
        .deserialize_stream()
        .try_collect()
        .await?;
    use itertools::Itertools;
    for table in tables {
        let table_name = &table.table_name;
        if let Some(stable) = table.stable_name {
            let fields = &stable_fields[&stable];
            let tags = fields.tag_names().collect_vec();
            let names = fields.tag_names().join(",");
            let fields: Vec<Value> = from
                .query_one(format!(
                    "select {names} from {stable} where tbname = '{table_name}'"
                ))
                .await?
                .unwrap();

            let tags_values = fields.into_iter().map(|v| v.to_sql_value()).join(",");
            to.exec(format!(
                "create table if not exists {table_name} using {stable} tags({tags_values})"
            ))
            .await?;
            // tags.iter().zip(fields).map(|(name, value)| format!(""))
            // let tags_stmt = tags.map(|_| '?').join(",");
            // let mut stmt = to.stmt(format!("create table if not exists ? using ({tags_stmt})"))?;
            // stmt.set_tbname_tags(&table.table_name, &fields);
        } else {
            let desc = from.describe(table_name).await?;
            let sql = desc.to_create_table_sql(table_name);
            to.exec(sql).await?;
        }
    }

    // let tables: Vec<
    Ok(())
}

async fn sync_schema_for_db(from: &Taos, to: &Taos, database: &str, db2: &str) -> Result<()> {
    Ok(())
}

impl App {
    pub async fn run_with_taos_opts(mut self, _opts: &TaosOpts) -> Result<()> {
        log::info!("app: {self:?}");
        // simple_logger::init();
        assert!(self.from.driver == "taos");
        assert!(self.to.driver == "taos");
        assert!(self.from.database.is_some());
        let topic = check_target_db(&mut self.to, &self.from).await?;

        let from = Manager::from_dsn(&self.from)?.into_pool()?;
        let to = Manager::from_dsn(&self.to)?.into_pool()?;
        // tmq.forward(sink).await?;

        let taos1 = from.get()?;
        let taos2 = to.get()?;
        sync_schema(&taos1, &taos2).await?;

        taos1.create_topic(topic, topic).await?;
        self.from.params.insert("topics".to_string(), topic.into());

        let builder = TmqBuilder::from_dsn(&self.from)?;
        let workers = self.workers.unwrap_or(10);
        // let (tx, mut rx) = tokio::sync::mpsc::channel(workers);

        let mut handles = Vec::new();

        for idx in 0..workers {
            let tmq = builder.build()?;
            let taos1 = from.get()?;
            let taos2 = to.get()?;
            let handle = thread::spawn(move || {
                log::info!("[{idx}] task start");
                while let Some(rs) = tmq.poll() {
                    let mut rs = rs?;
                    let _: Vec<_> = rs
                        .blocks_iter()
                        .map(|block| -> Result<()> {
                            let table = block.tmq_table_name().unwrap();
                            log::info!("[{idx}] table name is {table}");
                            if taos2.exec_sync(format!("describe {table}")).is_err() {
                                futures::executor::block_on(sync_schema(&taos1, &taos2))?;
                            }

                            let bind: Vec<MultiBind> =
                                block.columns_iter().map(|col| col.into()).collect();
                            use itertools::Itertools;
                            let questions = std::iter::repeat("?").take(bind.len()).join(",");
                            let mut stmt =
                                taos2.stmt(format!("insert into {table} values({questions})"))?;
                            stmt.multi_bind(&bind)?;
                            stmt.execute()?;
                            let inserted = stmt.affected_rows();
                            log::info!("[{idx}] inserted {inserted} rows into {table}");
                            Ok(())
                        })
                        .collect();
                }
                tmq.commit(None, 0)?;
                log::info!("[{idx}] task done");
                Ok::<_, anyhow::Error>(())
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join();
        }
        

        // let tmq = builder.build()?;
        // while let Some(rs) = tmq.poll() {
        //     log::info!("polled");
        //     let mut rs = rs?;
        //     let _: Vec<_> = rs
        //         .block_stream()
        //         .map(|block| -> Result<()> {
        //             let table = block.tmq_table_name().unwrap();
        //             log::info!("table name is {table}");
        //             if taos2.exec_sync(format!("describe {table}")).is_err() {
        //                 futures::executor::block_on(sync_schema(&taos1, &taos2))?;
        //             }

        //             let bind: Vec<MultiBind> = block.columns_iter().map(|col| col.into()).collect();
        //             use itertools::Itertools;
        //             let questions = std::iter::repeat("?").take(bind.len()).join(",");
        //             let mut stmt =
        //                 taos2.stmt(format!("insert into {table} values({questions})"))?;
        //             stmt.multi_bind(&bind)?;
        //             stmt.execute()?;
        //             let inserted = stmt.affected_rows();
        //             log::info!("inserted {inserted} rows");
        //             Ok(())
        //         })
        //         .collect()
        //         .await;
        // }

        // let unfold = futures::sink::unfold(0, |mut sum, mut rs: ResultSet| async move {
        //     rs.block_stream()
        //         .for_each(|block| async {
        //             let bind: Vec<MultiBind> = block.columns_iter().map(|col| col.into()).collect();
        //             let table = block.tmq_table_name().unwrap();
        //             let mut stmt = taos2.stmt(format!("insert into {table} values(?,?)")).unwrap();
        //             stmt.multi_bind(&bind).unwrap();
        //             stmt.execute().unwrap();
        //             let inserted = stmt.affected_rows();
        //             log::info!("inserted {inserted} rows");
        //         });
        //     let (blocks, rows) = rs.summary();
        //     assert!(blocks == 1, "tmq response blocks always should be 1");
        //     sum += rows;
        //     eprintln!("sum: {sum}, rows in block = {rows}");
        //     Ok::<_, taos::Error>(sum)
        // });
        // futures::pin_mut!(unfold);
        // tmq.forward(unfold).await?;
        Ok(())
    }
}
