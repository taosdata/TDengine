#[cfg(all(feature = "optin", feature = "deadpool"))]
#[tokio::test]
async fn test_deadpool_optin() -> anyhow::Result<()> {
    use taos::*;

    let pool = TaosBuilder::from_dsn("taos://localhost:6030")?.pool()?;
    let taos = pool.get().await?;
    taos.exec_many([
        "drop database if exists test_1752561201",
        "create database test_1752561201",
        "use test_1752561201",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752561201",
    ])
    .await?;

    Ok(())
}

#[cfg(all(feature = "optin", feature = "ws", feature = "deadpool"))]
#[tokio::test]
async fn test_deadpool_optin2() -> anyhow::Result<()> {
    use taos::*;

    let pool = TaosBuilder::from_dsn("taos://localhost:6030")?.pool()?;
    let taos = pool.get().await?;
    taos.exec_many([
        "drop database if exists test_1752561305",
        "create database test_1752561305",
        "use test_1752561305",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752561305",
    ])
    .await?;

    Ok(())
}

#[cfg(all(feature = "ws", feature = "deadpool"))]
#[tokio::test]
async fn test_deadpool_ws() -> anyhow::Result<()> {
    use taos::*;

    let pool = TaosBuilder::from_dsn("ws://localhost:6041")?.pool()?;
    let taos = pool.get().await?;
    taos.exec_many([
        "drop database if exists test_1752561357",
        "create database test_1752561357",
        "use test_1752561357",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752561357",
    ])
    .await?;

    Ok(())
}

#[cfg(all(feature = "ws", feature = "optin", feature = "deadpool"))]
#[tokio::test]
async fn test_deadpool_ws2() -> anyhow::Result<()> {
    use taos::*;

    let pool = TaosBuilder::from_dsn("ws://localhost:6041")?.pool()?;
    let taos = pool.get().await?;
    taos.exec_many([
        "drop database if exists test_1752561405",
        "create database test_1752561405",
        "use test_1752561405",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752561405",
    ])
    .await?;

    Ok(())
}
