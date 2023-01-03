use futures::TryStreamExt;
use serde::Deserialize;
use taos::{AsyncFetchable, AsyncQueryable, Dsn, TBuilder, Taos, TaosBuilder};

/// Check enterprise edition
pub async fn is_available_enterprise_edition(taos: &Taos) -> bool {
    #[derive(Deserialize, Debug)]
    struct Grant {
        version: String,
        expire_time: String,
        expired: String,
    }

    impl Grant {
        fn is_available_enterprise_edition(&self) -> bool {
            let _ = &self.expire_time;
            match (self.version.as_str(), self.expired.trim()) {
                ("official" | "trial", "false") => true,
                _ => false,
            }
        }
    }

    if let Ok(Some(grant)) = taos.query_one::<_, Grant>("show grants").await {
        grant.is_available_enterprise_edition()
    } else {
        false
    }
}

/// Clear database stables and tables.
pub async fn clear_database(dsn: &Dsn) -> anyhow::Result<()> {
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    let mut stables = taos.query("SHOW STABLES").await?;
    let mut rows = stables.rows();

    while let Some(mut row) = rows.try_next().await? {
        let name = format!("{}", row.next().unwrap().1);
        taos.exec(format!("DROP STABLE {name}")).await?;
    }

    let mut tables = taos.query("SHOW TABLES").await?;
    let mut rows = tables.rows();

    while let Some(mut row) = rows.try_next().await? {
        let name = format!("{}", row.next().unwrap().1);
        taos.exec(format!("DROP TABLE {name}")).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clear_database() -> anyhow::Result<()> {
    let dsn = "taos:///";

    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    let db = "test_clear_database";
    taos.exec_many([
        format!("drop database if exists {db}"),
        format!("create database {db}"),
        format!("use {db}"),
        format!("create stable stb1 (ts timestamp, v int) tags(t1 int)"),
        format!("create table ctb1 using stb1 tags(1)"),
        format!("create table ctb2 using stb1 tags(2)"),
        format!("create table ntb1 (ts timestamp, v int)"),
        format!("create table ntb2 (ts timestamp, v int)"),
    ])
    .await?;

    use std::str::FromStr;

    clear_database(&Dsn::from_str(&format!("taos:///{db}"))?).await?;

    assert!(taos.query_one::<_, String>("show stables").await?.is_none());
    assert!(taos.query_one::<_, String>("show tables").await?.is_none());

    taos.exec(format!("drop database {db}")).await?;

    Ok(())
}
