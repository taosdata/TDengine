use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "ws://";
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    taos.exec_many([
        "DROP DATABASE IF EXISTS power",
        "CREATE DATABASE power",
        "USE power",
        "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
    ]).await?;

    let inserted = taos.exec("INSERT INTO 
    power.d1001 USING power.meters TAGS('California.SanFrancisco', 2)
     VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) 
     ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3)
     VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS('California.LosAngeles', 2) 
     VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS('California.LosAngeles', 3) 
     VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)").await?;

    assert_eq!(inserted, 8);
    Ok(())
}
