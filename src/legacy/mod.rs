use std::time::Duration;

use anyhow::{bail, Context};
use serde::Deserialize;
use taos::{Consumer, *};

use crate::{
    tmq::{check_tmq_dsn, group_id_hash},
    Action,
};

struct Opts {
    batch_size: usize,
}

async fn sync_single_table(
    from: &Taos,
    table: &str,
    to: &Taos,
    opts: &Opts,
    is_v3: bool,
) -> Result<(), Error> {
    if opts.batch_size == 0 {
        let mut res = from.query(format!("SELECT * FROM {table}")).await?;
        let fields = res.num_of_fields();
        let mut blocks = res.blocks();
        if is_v3 {
            while let Some(mut block) = blocks.try_next().await? {
                block.with_table_name(table);
                to.write_raw_block(&block).await?;
            }
        } else {
            let mut stmt = Stmt::init(to)?;
            let question_masks = std::iter::repeat('?').take(fields).join(",");
            stmt.prepare(format!("INSERT INTO {table} VALUES({question_masks})"))?;
            log::debug!("prepare stmt");
            while let Some(block) = blocks.try_next().await? {
                // let views = block.columns().collect_vec();
                log::debug!("bind block: {block:?}");
                stmt.bind(block.column_views())?;
                log::debug!("add to batch");
                stmt.add_batch()?;
                log::debug!("execute");
                stmt.execute()?;
                log::debug!("continue loop");
            }
        }
    } else {
        unreachable!("batch syncing is not supported currently");
    }
    Ok(())
}

async fn sync_super_table(from: &Taos, name: &str, to: &Taos, tables: usize) -> Result<(), Error> {
    // let version: String = from.query_one("SELECT server_version()").await?.unwrap();
    // if version.starts_with('2') {
    // create stable
    let (_, sql): ((), String) = from
        .query_one(format!("show create table {name}"))
        .await?
        .unwrap();
    to.exec(
        sql.replace("VARCHAR", "BINARY")
            .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
            .replace("CREATE STABLE", "CREATE STABLE IF NOT EXISTS"),
    )
    .await?;

    if tables == 0 {
        return Ok(());
    }

    let desc = from.describe(name).await?;
    let tag_names = desc.tag_names().map(|s| format!("`{s}`")).join(",");

    let mut res = from
        .query(format!("SELECT tbname, {tag_names} FROM {name}"))
        .await?;

    let mut blocks = res.blocks();
    while let Some(block) = blocks.try_next().await? {
        let mut sql = format!("CREATE TABLE");
        for mut row in block.rows() {
            let child = row.next().unwrap().1.to_string().unwrap();

            let tags = row.map(|(_, v)| v.to_value().to_sql_value()).join(",");
            sql.extend(format!("  IF NOT EXISTS `{child}` USING `{name}` TAGS({tags})").chars());
        }
        log::debug!("create child tables with sql length {}", sql.len());
        to.exec(&sql).await?;
        // }
    }
    Ok(())
}

async fn sync_normal_table(from: &Taos, name: &str, to: &Taos) -> Result<(), Error> {
    let (_, sql): ((), String) = from
        .query_one(format!("show create table {name}"))
        .await?
        .unwrap();
    to.exec(
        sql.replace("VARCHAR", "BINARY")
            .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS"),
    )
    .await?;
    Ok(())
}

#[derive(Deserialize)]
struct STableRecord {
    name: String,
    tables: usize,
}
#[derive(Deserialize)]
struct TableRecord {
    table_name: String,
    stable_name: Option<String>,
}

async fn sync_schema(from: &Taos, to: &Taos) -> Result<(), Error> {
    let v1: String = from.query_one("SELECT server_version()").await?.unwrap();
    let v2: String = to.query_one("SELECT server_version()").await?.unwrap();
    if v1.starts_with('2') {
        // get stable list.
        let mut res = from.query("SHOW STABLES").await?;
        res.deserialize()
            .try_for_each(|stable: STableRecord| async move {
                // let name = stable.name.to_string();
                sync_super_table(from, &stable.name, to, stable.tables).await
            })
            .await?;

        //  get normal tables.
        let mut res = from.query("SHOW TABLES").await?;
        res.deserialize()
            .try_for_each(|row: TableRecord| async move {
                if row.stable_name.is_none() {
                    sync_normal_table(from, row.table_name.as_str(), to).await
                } else {
                    Ok(())
                }
            })
            .await?;
    } else {
        let database: String = from.query_one("SELECT database()").await?.unwrap();
        // get stable list.
        let mut res = from.query("SHOW STABLES").await?;
        res.deserialize()
            .try_for_each(|name: String| async move { sync_super_table(from, &name, to, 1).await })
            .await?;
        //  get normal tables.
        let mut res = from.query(format!("select `table_name` from information_schema.ins_tables where db_name = '{database}' and stable_name is null;")).await?;
        res.deserialize()
            .try_for_each(
                |row: String| async move { sync_normal_table(from, row.as_str(), to).await },
            )
            .await?;
    }

    Ok(())
}

async fn sync_tables_only(from: &Taos, to: &Taos) -> Result<(), Error> {
    let v1: String = from.query_one("SELECT server_version()").await?.unwrap();
    let v2: String = to.query_one("SELECT server_version()").await?.unwrap();
    let to_is_v3 = v2.starts_with('3');
    let opts = &Opts { batch_size: 0 };
    if v1.starts_with('2') {
        let mut res = from.query("SHOW TABLES").await?;
        res.deserialize()
            .try_for_each(|row: TableRecord| async move {
                sync_single_table(from, &row.table_name, to, opts, to_is_v3).await
            })
            .await?;
    } else {
        //  get normal tables.
        let mut res = from.query("SHOW TABLES").await?;
        res.deserialize()
            .try_for_each(|row: String| async move {
                sync_single_table(from, row.as_str(), to, opts, to_is_v3).await
            })
            .await?;
    }
    Ok(())
}
async fn sync(id: usize, from: Taos, to: Taos) -> Result<(), Error> {
    sync_schema(&from, &to).await?;
    sync_tables_only(&from, &to).await?;
    Ok(())
}

pub async fn legacy_to_taos(
    from: Dsn,
    actions: Vec<Action>,
    to: Dsn,
    jobs: usize,
) -> anyhow::Result<()> {
    let from_builder = TaosBuilder::from_dsn(&from)?;
    let to_builder = TaosBuilder::from_dsn(&to)?;

    let from = from_builder.build()?;
    let to = to_builder.build()?;
    let from_is_v3 = from
        .query_one::<_, String>("SELECT server_version()")
        .await?
        .unwrap()
        .starts_with('3');
    let to_is_v3 = to
        .query_one::<_, String>("SELECT server_version()")
        .await?
        .unwrap()
        .starts_with('3');
    sync(0, from, to).await?;
    log::info!("syncing done, wait to release resources");
    if to_is_v3 {
        drop(to_builder);
    }
    drop(from_builder);

    log::info!("done");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    //
    #[tokio::test(flavor = "multi_thread")]
    async fn sync() -> anyhow::Result<()> {
        pretty_env_logger::formatted_timed_builder()
            .filter_level(log::LevelFilter::Trace)
            .init();
        let v3: Dsn = "taos:///db1".parse()?;
        let v2: Dsn = "taos://localhost:16030/db1?libraryPath=\
            /home/huolinhe/Projects/taosdata/TDengine2.0/debug/build/lib/libtaos.so.2.6.0.0\
            &configDir=\
            /home/huolinhe/Projects/taosdata/taos-connector-rust/taos-optin/tests/cfg/v2"
            .parse()?;
        legacy_to_taos(v3.clone(), vec![], v2.clone(), 1).await?;
        legacy_to_taos(v2, vec![], v3, 1).await?;
        Ok(())
    }
}
