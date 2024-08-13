use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://localhost:6030";
    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

    taos.exec("DROP DATABASE IF EXISTS power").await?;
    taos.create_database("power").await?;
    taos.use_database("power").await?;
    taos.exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))").await?;

    let mut stmt = Stmt::init(&taos).await?;
    stmt.prepare("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)").await?;

    const NUM_TABLES: usize = 10;
    const NUM_ROWS: usize = 10;
    for i in 0..NUM_TABLES {
        let table_name = format!("d_bind_{}", i);
        let tags = vec![Value::Int(i as i32), Value::VarChar(format!("location_{}", i).into())];

        // set table name and tags for the prepared statement.
        stmt.set_tbname_tags(&table_name, &tags).await?;
        for j in 0..NUM_ROWS {
            let values = vec![
                ColumnView::from_millis_timestamp(vec![1648432611249 + j as i64]),
                ColumnView::from_floats(vec![10.3 + j as f32]),
                ColumnView::from_ints(vec![219 + j as i32]),
                ColumnView::from_floats(vec![0.31 + j as f32]),
            ];
            // bind values to the prepared statement.    
            stmt.bind(&values).await?;
        }

        stmt.add_batch().await?;
    }

    // execute.
    match stmt.execute().await{
        Ok(affected_rows) => println!("Successfully inserted {} rows to power.meters.", affected_rows),
        Err(err) => {
            eprintln!("Failed to insert to table meters using stmt, dsn: {}; ErrMessage: {}", dsn, err);
            return Err(err.into());
        }
    }

    Ok(())
}
