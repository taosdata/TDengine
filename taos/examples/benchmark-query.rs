use anyhow::Result;
use taos::prelude::sync::*;

// Refer to [Jira TD-15233](https://jira.taosdata.com:18080/browse/TD-15233)
fn main() -> Result<()> {
    let taos = TaosOptions::new().database("abc1").build()?;
    let rs: (String, String) = taos
        .query_one("select tbname, location from meters limit 1")?
        .unwrap();
    dbg!(rs);
    Ok(())
}
