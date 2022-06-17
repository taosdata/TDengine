use anyhow::Result;
use libtaos::*;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("TDENGINE_CLOUD_DSN").unwrap();
    let cfg = TaosCfg::from_dsn(dsn)?;
    let conn = cfg.connect()?;
    let _ = conn.query("show databases").await?;
    println!("Connected");
    Ok(())
}