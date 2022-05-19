use anyhow::Result;
use taos::prelude::sync::*;

// Refer to [Jira TD-15233](https://jira.taosdata.com:18080/browse/TD-15233)
fn main() -> Result<()> {
    let taos = TaosOptions::new().build()?;
    taos.exec("create database abc")?;
    taos.exec("drop database abc")?;
    Ok(())
}
