use taos::*;
use taos_query::util::hex::hex_string_to_bytes;

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
        CREATE STABLE IF NOT EXISTS stb (
            ts TIMESTAMP,
            int_col INT,
            double_col DOUBLE,
            bool_col BOOL,
            binary_col BINARY(100),
            nchar_col NCHAR(100),
            varbinary_col VARBINARY(100),
            geometry_col GEOMETRY(100))
        TAGS (
            int_tag INT,
            double_tag DOUBLE,
            bool_tag BOOL,
            binary_tag BINARY(100),
            nchar_tag NCHAR(100))
        "#,
    )
    .await?;

    let mut stmt = Stmt::init(&taos).await?;
    stmt.prepare("INSERT INTO ? using stb tags(?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?)")
        .await?;

    const NUM_TABLES: usize = 10;
    const NUM_ROWS: usize = 10;
    for i in 0..NUM_TABLES {
        let table_name = format!("d_bind_{}", i);
        let tags = vec![
            Value::Int(i as i32),
            Value::Double(1.1),
            Value::Bool(true),
            Value::VarChar("binary_value".into()),
            Value::NChar("nchar_value".into()),
            // Value::VarBinary(vec![0x98, 0xf4, 0x6e].into()),
            // Value::Geometry(
            //     vec![
            //         0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            //         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            //     ]
            //     .into(),
            // ),
        ];

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
                ColumnView::from_doubles(vec![1.1]),
                ColumnView::from_bools(vec![true]),
                ColumnView::from_varchar(vec!["ABC"]),
                ColumnView::from_nchar(vec!["涛思数据"]),
                ColumnView::from_bytes(vec![hex_string_to_bytes("123456").to_vec()]),
                ColumnView::from_geobytes(vec![hex_string_to_bytes(
                    "0101000000000000000000F03F0000000000000040",
                )
                .to_vec()]),
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
            "Successfully inserted {} rows to example_all_type_stmt.stb.",
            affected_rows
        ),
        Err(err) => {
            eprintln!(
                "Failed to insert to table stb using stmt, ErrMessage: {}",
                err
            );
            return Err(err.into());
        }
    }

    Ok(())
}
