#[tokio::test]
async fn test_query_error() -> anyhow::Result<()> {
    use taos::*;
    let dsn = "taos://";

    let pool = TaosBuilder::from_dsn(dsn)?.pool()?;
    tracing::trace!("start");

    let taos = pool.get().await?;
    tracing::trace!("got connection");

    let result = taos.query("describe information_schema._tb1").await;
    assert!(result.is_err());

    Ok(())
}
