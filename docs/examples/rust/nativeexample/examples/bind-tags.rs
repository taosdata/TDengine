use anyhow::Result;
use serde::Deserialize;
use taos::*;

#[tokio::main]
async fn main() -> Result<()> {
    let taos = TaosBuilder::from_dsn("taos://")?.build()?;
    taos.exec_many([
        "drop database if exists test",
        "create database test keep 36500",
        "use test",
        "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100)) tags(t1 varchar(100))",
    ])
    .await?;
    let mut stmt = Stmt::init(&taos)?;
    stmt.prepare(
        "insert into ? using tb1 tags(?) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;
    stmt.set_tbname("d0")?;
    stmt.set_tags(&[Value::VarChar("涛思".to_string())])?;

    let params = vec![
        ColumnView::from_millis_timestamp(vec![164000000000]),
        ColumnView::from_bools(vec![true]),
        ColumnView::from_tiny_ints(vec![i8::MAX]),
        ColumnView::from_small_ints(vec![i16::MAX]),
        ColumnView::from_ints(vec![i32::MAX]),
        ColumnView::from_big_ints(vec![i64::MAX]),
        ColumnView::from_unsigned_tiny_ints(vec![u8::MAX]),
        ColumnView::from_unsigned_small_ints(vec![u16::MAX]),
        ColumnView::from_unsigned_ints(vec![u32::MAX]),
        ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
        ColumnView::from_floats(vec![f32::MAX]),
        ColumnView::from_doubles(vec![f64::MAX]),
        ColumnView::from_varchar(vec!["ABC"]),
        ColumnView::from_nchar(vec!["涛思数据"]),
    ];
    let rows = stmt.bind(&params)?.add_batch()?.execute()?;
    assert_eq!(rows, 1);

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Row {
        ts: String,
        c1: bool,
        c2: i8,
        c3: i16,
        c4: i32,
        c5: i64,
        c6: u8,
        c7: u16,
        c8: u32,
        c9: u64,
        c10: Option<f32>,
        c11: f64,
        c12: String,
        c13: String,
        t1: serde_json::Value,
    }

    let rows: Vec<Row> = taos
        .query("select * from tb1")
        .await?
        .deserialize()
        .try_collect()
        .await?;
    let row = &rows[0];
    dbg!(&row);
    assert_eq!(row.c5, i64::MAX);
    assert_eq!(row.c8, u32::MAX);
    assert_eq!(row.c9, u64::MAX);
    assert_eq!(row.c10.unwrap(), f32::MAX);
    // assert_eq!(row.c11, f64::MAX);
    assert_eq!(row.c12, "ABC");
    assert_eq!(row.c13, "涛思数据");

    Ok(())
}
