use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let taos = TaosBuilder::from_dsn("taos://")?.build()?;
    taos.create_database("power").await?;
    taos.use_database("power").await?;
    taos.exec("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)").await?;

    let mut stmt = Stmt::init(&taos)?;
    stmt.prepare("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")?;
    // bind table name and tags
    stmt.set_tbname_tags(
        "d1001",
        &[
            Value::VarChar("California.SanFransico".into()),
            Value::Int(2),
        ],
    )?;
    // bind values.
    let values = vec![
        ColumnView::from_millis_timestamp(vec![1648432611249]),
        ColumnView::from_floats(vec![10.3]),
        ColumnView::from_ints(vec![219]),
        ColumnView::from_floats(vec![0.31]),
    ];
    stmt.bind(&values)?;
    // bind one more row
    let values2 = vec![
        ColumnView::from_millis_timestamp(vec![1648432611749]),
        ColumnView::from_floats(vec![12.6]),
        ColumnView::from_ints(vec![218]),
        ColumnView::from_floats(vec![0.33]),
    ];
    stmt.bind(&values2)?;

    stmt.add_batch()?;

    // execute.
    let rows = stmt.execute()?;
    assert_eq!(rows, 2);
    Ok(())
}
