use anyhow::Result;
use taos::*;

#[tokio::main]
async fn main() -> Result<()> {
    let mut dsn = std::env::var("TDENGINE_CLOUD_DSN").parse()?;
    let builder = TaosBuilder::from_dsn(dsn)?;
    let conn = builder.build()?;
    //ANCHOR: insert
conn.exec("DROP DATABASE IF EXISTS power").await?;
conn.exec("CREATE DATABASE power").await?;
conn.exec("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)").await?;
conn.exec("INSERT INTO power.d1001 USING power.meters TAGS(California.SanFrancisco, 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
power.d1002 USING power.meters TAGS(California.SanFrancisco, 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
").await?;
    //ANCHOR_END: insert
    // ANCHOR: query
let result = conn.query("SELECT ts, current FROM power.meters LIMIT 2").await?;
    // ANCHOR_END: query
    // ANCHOR: meta
let fields = result.fields();
for column in fields {
    println!("name:{} bytes: {}", column.name(), column.bytes());
}
// name:ts bytes: 8
// name:current bytes: 4
    // ANCHOR_END: meta
    // ANCHOR: iter
    let mut rows = result.rows();
    while let Some(row) = rows.try_next().await? {
        for (name, value) in row {
            println!("got value of {}: {}", name, value);
        }
    }
// 2018-10-03 14:38:05.000 10.3
// 2018-10-03 14:38:15.000 12.6
    // ANCHOR_END: iter
    Ok(())
}
