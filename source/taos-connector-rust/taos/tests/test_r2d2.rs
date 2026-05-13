#[cfg(all(feature = "optin", feature = "r2d2"))]
#[test]
fn test_r2d2_optin() -> anyhow::Result<()> {
    use std::time::Duration;

    use taos::sync::*;

    let builder = PoolBuilder::new()
        .max_size(5000)
        .max_lifetime(Some(Duration::from_secs(60 * 60)))
        .min_idle(Some(1000))
        .connection_timeout(Duration::from_secs(5));

    let pool = TaosBuilder::from_dsn("taos://localhost:6030")?.with_pool_builder(builder)?;
    let taos = pool.get()?;

    taos.exec_many([
        "drop database if exists test_1752492646",
        "create database test_1752492646",
        "use test_1752492646",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752492646",
    ])?;

    Ok(())
}

#[cfg(all(feature = "optin", feature = "ws", feature = "r2d2"))]
#[test]
fn test_r2d2_optin2() -> anyhow::Result<()> {
    use std::time::Duration;

    use taos::sync::*;

    let builder = PoolBuilder::new()
        .max_size(5000)
        .max_lifetime(Some(Duration::from_secs(60 * 60)))
        .min_idle(Some(1000))
        .connection_timeout(Duration::from_secs(5));

    let pool = TaosBuilder::from_dsn("taos://localhost:6030")?.with_pool_builder(builder)?;
    let taos = pool.get()?;

    taos.exec_many([
        "drop database if exists test_1752560231",
        "create database test_1752560231",
        "use test_1752560231",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752560231",
    ])?;

    Ok(())
}

#[cfg(all(feature = "ws", feature = "r2d2"))]
#[test]
fn test_r2d2_ws() -> anyhow::Result<()> {
    use std::time::Duration;

    use taos::sync::*;

    let builder = PoolBuilder::new()
        .max_size(5000)
        .max_lifetime(Some(Duration::from_secs(60 * 60)))
        .min_idle(Some(1000))
        .connection_timeout(Duration::from_secs(5));

    let pool = TaosBuilder::from_dsn("ws://localhost:6041")?.with_pool_builder(builder)?;
    let taos = pool.get()?;

    taos.exec_many([
        "drop database if exists test_1752492388",
        "create database test_1752492388",
        "use test_1752492388",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752492388",
    ])?;

    Ok(())
}

#[cfg(all(feature = "ws", feature = "optin", feature = "r2d2"))]
#[test]
fn test_r2d2_ws2() -> anyhow::Result<()> {
    use std::time::Duration;

    use taos::sync::*;

    let builder = PoolBuilder::new()
        .max_size(5000)
        .max_lifetime(Some(Duration::from_secs(60 * 60)))
        .min_idle(Some(1000))
        .connection_timeout(Duration::from_secs(5));

    let pool = TaosBuilder::from_dsn("ws://localhost:6041")?.with_pool_builder(builder)?;
    let taos = pool.get()?;

    taos.exec_many([
        "drop database if exists test_1752560246",
        "create database test_1752560246",
        "use test_1752560246",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752560246",
    ])?;

    Ok(())
}

#[cfg(feature = "optin")]
#[tokio::test]
async fn test_optin() -> anyhow::Result<()> {
    use taos::*;

    let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
        .build()
        .await?;

    taos.exec_many([
        "drop database if exists test_1752560727",
        "create database test_1752560727",
        "use test_1752560727",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752560727",
    ])
    .await?;

    Ok(())
}

#[cfg(all(feature = "optin", feature = "ws"))]
#[tokio::test]
async fn test_optin2() -> anyhow::Result<()> {
    use taos::*;

    let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
        .build()
        .await?;

    taos.exec_many([
        "drop database if exists test_1752560848",
        "create database test_1752560848",
        "use test_1752560848",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752560848",
    ])
    .await?;

    Ok(())
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn test_ws() -> anyhow::Result<()> {
    use taos::*;

    let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
        .build()
        .await?;

    taos.exec_many([
        "drop database if exists test_1752560711",
        "create database test_1752560711",
        "use test_1752560711",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752560711",
    ])
    .await?;

    Ok(())
}

#[cfg(all(feature = "ws", feature = "optin"))]
#[tokio::test]
async fn test_ws2() -> anyhow::Result<()> {
    use taos::*;

    let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
        .build()
        .await?;

    taos.exec_many([
        "drop database if exists test_1752560890",
        "create database test_1752560890",
        "use test_1752560890",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752560890",
    ])
    .await?;

    Ok(())
}
