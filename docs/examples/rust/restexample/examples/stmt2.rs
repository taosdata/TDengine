use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "ws://";
    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

    taos.exec("DROP DATABASE IF EXISTS power").await?;
    taos.create_database("power").await?;
    taos.use_database("power").await?;
    taos.exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))").await?;

    let mut stmt2 = Stmt2::init(&taos).await?;
    stmt2.prepare("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)").await?;

    const NUM_TABLES: usize = 10;
    const NUM_ROWS: usize = 10;

    let mut params = Vec::with_capacity(NUM_TABLES);
    for i in 0..NUM_TABLES {
        let table_name = format!("d_bind_{}", i);
        let tags = vec![
            Value::Int(i as i32),
            Value::VarChar(format!("location_{}", i).into()),
        ];

        let mut timestamps = Vec::with_capacity(NUM_ROWS);
        let mut currents = Vec::with_capacity(NUM_ROWS);
        let mut voltages = Vec::with_capacity(NUM_ROWS);
        let mut phases = Vec::with_capacity(NUM_ROWS);

        for j in 0..NUM_ROWS {
            timestamps.push(1648432611249 + j as i64);
            currents.push(10.3 + j as f32);
            voltages.push(219 + j as i32);
            phases.push(0.31 + j as f32);
        }

        let columns = vec![
            ColumnView::from_millis_timestamp(timestamps),
            ColumnView::from_floats(currents),
            ColumnView::from_ints(voltages),
            ColumnView::from_floats(phases),
        ];

        let param = Stmt2BindParam::new(Some(table_name), Some(tags), Some(columns));
        params.push(param);
    }

    // Bind params to the prepared statement
    stmt2.bind(&params).await?;

    // Execute
    match stmt2.exec().await {
        Ok(affected_rows) => println!(
            "Successfully inserted {} rows to power.meters.",
            affected_rows
        ),
        Err(err) => {
            eprintln!(
                "Failed to insert to table meters using stmt2, ErrMessage: {}",
                err
            );
            return Err(err.into());
        }
    }

    Ok(())
}
