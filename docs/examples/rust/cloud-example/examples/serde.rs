use anyhow::Result;
use chrono::{DateTime, Local};
use serde::Deserialize;
use taos::*;

#[derive(Debug, Deserialize)]
struct Record {
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

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("TDENGINE_CLOUD_DSN")?;
    let builder = TaosBuilder::from_dsn(dsn)?;
    let taos = builder.build().await?;

    let records: Vec<Record> = taos.query("select * from power.meters limit 5")
        .await?
        .deserialize()
        .try_collect()
        .await?;
    println!("length of records: {}", records.len());
    records.iter().for_each(|record| {
        println!("{:?}", record);
    });
    Ok(())
}


