use anyhow::Result;
use taos::*;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("TDENGINE_CLOUD_DSN")?;
    let builder = TaosBuilder::from_dsn(dsn)?;
    let taos = builder.build().await?;

    // ANCHOR: query
    let mut result = taos.query("SELECT * FROM power.meters limit 5").await?;
    // ANCHOR_END: query
    // ANCHOR: meta
    let fields = result.fields();
    for column in fields {
        println!("name: {}, type: {:?} , bytes: {}", column.name(), column.ty(), column.bytes());
    }
    // ANCHOR_END: meta
    // ANCHOR: iter
    let rows = result.rows();
    rows.try_for_each(|row| async {
        println!("{}", row.into_value_iter().join(","));
        Ok(())
    }).await?;
    // ANCHOR_END: iter

    // ANCHOR: iter_column
    let mut query_restult = taos.query("SELECT * FROM power.meters limit 5").await?;
    let mut ressult_rows = query_restult.rows();
    let mut nrows = 0;
    while let Some(row) = ressult_rows.try_next().await? {
        for (col, (name, value)) in row.enumerate() {
            println!(
                "[{}] got value in col {} (named `{:>8}`): {}",
                nrows, col, name, value
            );
        }
        nrows += 1;
    }
    // ANCHOR_END: iter_column
    Ok(())
}


