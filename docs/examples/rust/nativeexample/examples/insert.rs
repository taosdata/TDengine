use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://localhost:6030";
    let builder = TaosBuilder::from_dsn(dsn)?;

    let taos = builder.build().await?;


    // ANCHOR: insert_data
    match taos.exec(r#"INSERT INTO 
    power.d1001 USING power.meters TAGS(2,'California.SanFrancisco')  
    VALUES 
    (NOW + 1a, 10.30000, 219, 0.31000) 
    (NOW + 2a, 12.60000, 218, 0.33000) 
    (NOW + 3a, 12.30000, 221, 0.31000) 
    power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') 
    VALUES 
    (NOW + 1a, 10.30000, 218, 0.25000) "#).await{
        Ok(affected_rows) => println!("Successfully inserted {} rows to power.meters.", affected_rows),
        Err(err) => {
            eprintln!("Failed to insert data to power.meters, dsn: {}; ErrMessage: {}", dsn, err);
            return Err(err.into());
        }
    }

    // ANCHOR_END: insert_data

    Ok(())
}
