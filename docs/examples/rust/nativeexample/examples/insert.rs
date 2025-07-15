use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://localhost:6030";
    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
    taos.exec_many([
        "drop database if exists power",
        "create database power",
        "use power",
        "create table meters (ts timestamp, current float, voltage int, phase float) tags (groupid int, location varchar(64))",
    ]).await?;

    // ANCHOR: insert_data
    let insert_sql = r#"INSERT INTO 
                        power.d1001 USING power.meters TAGS(2,'California.SanFrancisco')  
                        VALUES 
                        (NOW + 1a, 10.30000, 219, 0.31000) 
                        (NOW + 2a, 12.60000, 218, 0.33000) 
                        (NOW + 3a, 12.30000, 221, 0.31000) 
                        power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') 
                        VALUES 
                        (NOW + 1a, 10.30000, 218, 0.25000) "#;

    match taos.exec(insert_sql).await {
        Ok(affected_rows) => println!(
            "Successfully inserted {} rows to power.meters.",
            affected_rows
        ),
        Err(err) => {
            eprintln!(
                "Failed to insert data to power.meters, sql: {}, ErrMessage: {}",
                insert_sql, err
            );
            return Err(err.into());
        }
    }
    // ANCHOR_END: insert_data

    Ok(())
}
