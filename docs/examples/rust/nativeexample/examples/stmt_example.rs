use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let taos = TaosBuilder::from_dsn("taos://")?.build().await?;
    taos.create_database("power").await?;
    taos.use_database("power").await?;
    taos.exec("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location VARCHAR(64), groupId INT)").await?;

    let mut stmt = Stmt::init(&taos).await?;
    stmt.prepare("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")
        .await?;

    // bind table name and tags
    stmt.set_tbname_tags(
        "d1001",
        &[
            Value::VarChar("California.SanFransico".into()),
            Value::Int(2),
        ],
    )
    .await?;

    // bind values.
    let values = vec![
        ColumnView::from_millis_timestamp(vec![1648432611249]),
        ColumnView::from_floats(vec![10.3]),
        ColumnView::from_ints(vec![219]),
        ColumnView::from_floats(vec![0.31]),
    ];
    stmt.bind(&values).await?;

    // bind one more row
    let values2 = vec![
        ColumnView::from_millis_timestamp(vec![1648432611749]),
        ColumnView::from_floats(vec![12.6]),
        ColumnView::from_ints(vec![218]),
        ColumnView::from_floats(vec![0.33]),
    ];
    stmt.bind(&values2).await?;

    stmt.add_batch().await?;

    // execute.
    let rows = stmt.execute().await?;
    assert_eq!(rows, 2);

    Ok(())
}
