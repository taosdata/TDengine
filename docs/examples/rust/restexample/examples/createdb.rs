use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ANCHOR: create_db_and_table

    let dsn = "ws://localhost:6041";
    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

    // create database and use it
    match taos
        .exec_many(["CREATE DATABASE IF NOT EXISTS power"])
        .await
    {
        Ok(afffected_rows) => println!(
            "Create database power successfully, rowsAffected: {}",
            afffected_rows
        ),
        Err(err) => {
            eprintln!("Failed to create database power, ErrMessage: {}", err);
            return Err(err.into());
        }
    }

    // create super table
    match taos.exec_many([
        "CREATE STABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) \
        TAGS (`groupid` INT, `location` BINARY(24))",
    ]).await {
        Ok(afffected_rows) => println!("Create stable power.meters successfully, rowsAffected: {}", afffected_rows),
        Err(err) => {
            eprintln!("Failed to create stable power.meters, ErrMessage: {}", err);
            return Err(err.into());
        }
    }

    // ANCHOR_END: create_db_and_table

    Ok(())
}
