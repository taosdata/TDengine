use anyhow::Result;
use taos::*;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("TDENGINE_CLOUD_DSN")?;
    let pool = TaosBuilder::from_dsn(dsn)?.pool()?;

    let conn = pool.get().await?;
    let mut res = conn.query("show databases").await?;
    res.rows().try_for_each(|row| async {
        println!("{}", row.into_value_iter().join(","));
        Ok(())
    }).await?;
    Ok(())
}


