use anyhow::Result;
use taos::*;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("TDENGINE_CLOUD_DSN")?;
    let builder = TaosBuilder::from_dsn(dsn)?;
    let taos = builder.build().await?;
    //ANCHOR: option
    //ANCHOR: insert
    taos.exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)").await?;
    taos.exec("INSERT INTO
        power.d1001 USING power.meters
            TAGS('California.SanFrancisco', 2)
            VALUES (NOW, 12.3, 219, 0.31000) (NOW - 1s, 12.60000, 218, 0.33000) (NOW - 3s, 12.30000, 221, 0.31000)
        power.d1002 USING power.meters
            TAGS('California.SanFrancisco', 3)
            VALUES ('2018-10-03 14:39:16.650', 23.4, 218, 0.25000)
        ").await?;
    //ANCHOR_END: insert

    //ANCHOR: exec_many
    taos.use_database("power").await?;
    taos.exec_many([
        "CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)",
        "CREATE TABLE IF NOT EXISTS d0 USING power.meters TAGS('California.LosAngles',0)",
        "INSERT INTO d0 values(now - 10s, 10, 116, 0.32)",
        "INSERT INTO d1 USING meters TAGS('California.SanFrancisco', 4) values (now-8s, 10, 120, 0.33) (now - 6s, 10, 119, 0.34)",
    ]).await?;
    //ANCHOR_END: exec_many


    // ANCHOR: query
    let mut result = taos.query("SELECT * FROM power.meters limit 5").await?;
    // ANCHOR_END: query
    // ANCHOR: meta
    let fields = result.fields();
    for column in fields {
        println!("name: {}, type: {:?} , bytes: {}", column.name(), column.ty(), column.bytes());
    }
    // output
    // name: ts, type: Timestamp , bytes: 8
    // name: current, type: Float , bytes: 4
    // name: voltage, type: Int , bytes: 4
    // name: phase, type: Float , bytes: 4
    // name: location, type: VarChar , bytes: 64
    // name: groupid, type: Int , bytes: 4

    // ANCHOR_END: meta
    // ANCHOR: iter
    let rows = result.rows();
    rows.try_for_each(|row| async {
        println!("{}", row.into_value_iter().join(","));
        Ok(())
    }).await?;
    // output
    // 2018-10-03T14:39:05+08:00,12.3,219,0.31,California.SanFrancisco,2
    // 2018-10-03T14:39:15+08:00,12.6,218,0.33,California.SanFrancisco,2
    // 2018-10-03T14:39:16.800+08:00,12.3,221,0.31,California.SanFrancisco,2
    // 2018-10-03T14:39:16.650+08:00,23.4,218,0.25,California.SanFrancisco,3
    // ANCHOR_END: iter
    // ANCHOR_END: option

    Ok(())
}
