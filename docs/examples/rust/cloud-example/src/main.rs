use anyhow::Result;
use libtaos::*;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("TDENGINE_CLOUD_DSN")?;
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;
    let _ = taos.query("show databases").await?;
    println!("Connected");
    Ok(())
}