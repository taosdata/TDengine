use libtaos::*;

fn taos_connect() -> Result<Taos, Error> {
    TaosCfgBuilder::default()
        .ip("localhost")
        .user("root")
        .pass("taosdata")
        .db("power")
        .port(6030u16)
        .build()
        .expect("TaosCfg builder error")
        .connect()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let taos = taos_connect().expect("connect error");
    let result = taos.query("SELECT ts, current FROM meters LIMIT 2").await?;
    // print column names
    let meta: Vec<ColumnMeta> = result.column_meta;
    for column in meta {
        print!("{}\t", column.name)
    }
    println!();
    // print rows
    let rows: Vec<Vec<Field>> = result.rows;
    for row in rows {
        for field in row {
            print!("{}\t", field);
        }
        println!();
    }
    Ok(())
}

// output:
// ts      current
// 2022-03-28 09:56:51.249 10.3
// 2022-03-28 09:56:51.749 12.6
