use std::str::FromStr;

use anyhow::Result;
use taos::*;

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    let dsn = Dsn::from_str("taos+ws:///test")?;
    dbg!(&dsn);
    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
    taos.exec("CREATE DATABASE `xtest` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 14400m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 20 SINGLE_STABLE 0").await?;
    Ok(())
}
