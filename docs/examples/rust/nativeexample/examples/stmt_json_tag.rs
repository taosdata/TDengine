use serde_json::json;
use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://";
    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

    taos.exec("DROP DATABASE IF EXISTS example_all_type_stmt")
        .await?;
    taos.create_database("example_all_type_stmt").await?;
    taos.use_database("example_all_type_stmt").await?;
    taos.exec(
        r#"
CREATE STABLE IF NOT EXISTS stb_json (
    ts TIMESTAMP,
    int_col INT)
TAGS (
    json_tag JSON)
"#,
    )
    .await?;

    let mut stmt = Stmt::init(&taos).await?;
    stmt.prepare("INSERT INTO ? using stb_json tags(?) VALUES (?,?)")
        .await?;

    const NUM_TABLES: usize = 1;
    const NUM_ROWS: usize = 1;
    for i in 0..NUM_TABLES {
        let table_name = format!("d_bind_{}", i);
        let json_value: serde_json::Value = json!({
            "name": "value"
        });

        dbg!(json_value.to_string());

        let tags = vec![Value::Json(json_value)];

        // set table name and tags for the prepared statement.
        match stmt.set_tbname_tags(&table_name, &tags).await {
            Ok(_) => {}
            Err(err) => {
                eprintln!(
                    "Failed to set table name and tags, table_name:{}, tags:{:?}, ErrMessage: {}",
                    table_name, tags, err
                );
                return Err(err.into());
            }
        }
        for j in 0..NUM_ROWS {
            let values = vec![
                ColumnView::from_millis_timestamp(vec![1648432611249 + j as i64]),
                ColumnView::from_ints(vec![j as i32]),
            ];
            // bind values to the prepared statement.
            match stmt.bind(&values).await {
                Ok(_) => {}
                Err(err) => {
                    eprintln!(
                        "Failed to bind values, values:{:?}, ErrMessage: {}",
                        values, err
                    );
                    return Err(err.into());
                }
            }
        }

        match stmt.add_batch().await {
            Ok(_) => {}
            Err(err) => {
                eprintln!("Failed to add batch, ErrMessage: {}", err);
                return Err(err.into());
            }
        }
    }

    // execute.
    match stmt.execute().await {
        Ok(affected_rows) => println!(
            "Successfully inserted {} rows to example_all_type_stmt.stb_json.",
            affected_rows
        ),
        Err(err) => {
            eprintln!(
                "Failed to insert to table stb_json using stmt, ErrMessage: {}",
                err
            );
            return Err(err.into());
        }
    }

    Ok(())
}
