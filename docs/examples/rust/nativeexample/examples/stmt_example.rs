use bstr::BString;
use libtaos::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let taos = TaosCfg::default().connect().expect("fail to connect");
    taos.create_database("power").await?;
    taos.use_database("power").await?;
    taos.exec("CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)").await?;
    let mut stmt = taos.stmt("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")?;
    // bind table name and tags
    stmt.set_tbname_tags(
        "d1001",
        [
            Field::Binary(BString::from("California.SanFrancisco")),
            Field::Int(2),
        ],
    )?;
    // bind values.
    let values = vec![
        Field::Timestamp(Timestamp::new(1648432611249, TimestampPrecision::Milli)),
        Field::Float(10.3),
        Field::Int(219),
        Field::Float(0.31),
    ];
    stmt.bind(&values)?;
    // bind one more row
    let values2 = vec![
        Field::Timestamp(Timestamp::new(1648432611749, TimestampPrecision::Milli)),
        Field::Float(12.6),
        Field::Int(218),
        Field::Float(0.33),
    ];
    stmt.bind(&values2)?;
    // execute
    stmt.execute()?;
    Ok(())
}
