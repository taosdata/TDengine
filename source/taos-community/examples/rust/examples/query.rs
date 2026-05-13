use std::time::Duration;

use chrono::{DateTime, Local};
use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://";

    let opts = PoolBuilder::new()
        .max_size(5000) // max connections
        .max_lifetime(Some(Duration::from_secs(60 * 60))) // lifetime of each connection
        .min_idle(Some(1000)) // minimal idle connections
        .connection_timeout(Duration::from_secs(2));

    let pool = TaosBuilder::from_dsn(dsn)?.with_pool_builder(opts)?;

    let taos = pool.get()?;

    let db = "query";

    // prepare database
    taos.exec_many([
        format!("DROP DATABASE IF EXISTS `{db}`"),
        format!("CREATE DATABASE `{db}`"),
        format!("USE `{db}`"),
    ])
    .await?;

    let inserted = taos.exec_many([
        // create super table
        "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))",
        // create child table
        "CREATE TABLE `d0` USING `meters` TAGS(0, 'Los Angles')",
        // insert into child table
        "INSERT INTO `d0` values(now - 10s, 10, 116, 0.32)",
        // insert with NULL values
        "INSERT INTO `d0` values(now - 8s, NULL, NULL, NULL)",
        // insert and automatically create table with tags if not exists
        "INSERT INTO `d1` USING `meters` TAGS(1, 'San Francisco') values(now - 9s, 10.1, 119, 0.33)",
        // insert many records in a single sql
        "INSERT INTO `d1` values (now-8s, 10, 120, 0.33) (now - 6s, 10, 119, 0.34) (now - 4s, 11.2, 118, 0.322)",
    ]).await?;

    assert_eq!(inserted, 6);
    loop {
        let count: usize = taos
            .query_one("select count(*) from `meters`")
            .await?
            .unwrap_or_default();

        if count >= 6 {
            break;
        } else {
            println!("waiting for data");
        }
    }

    let mut result = taos.query("select tbname, * from `meters`").await?;

    for field in result.fields() {
        println!("got field: {}", field.name());
    }

    // Query option 1, use rows stream.
    let mut rows = result.rows();
    let mut nrows = 0;
    while let Some(row) = rows.try_next().await? {
        for (col, (name, value)) in row.enumerate() {
            println!(
                "[{}] got value in col {} (named `{:>8}`): {}",
                nrows, col, name, value
            );
        }
        nrows += 1;
    }

    // Query options 2, use deserialization with serde.
    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        tbname: String,
        // deserialize timestamp to chrono::DateTime<Local>
        ts: DateTime<Local>,
        // float to f32
        current: Option<f32>,
        // int to i32
        voltage: Option<i32>,
        phase: Option<f32>,
        groupid: i32,
        // binary/varchar to String
        location: String,
    }

    let records: Vec<Record> = taos
        .query("select tbname, * from `meters`")
        .await?
        .deserialize()
        .try_collect()
        .await?;

    dbg!(result.summary());
    assert_eq!(records.len(), 6);
    dbg!(records);
    Ok(())
}
