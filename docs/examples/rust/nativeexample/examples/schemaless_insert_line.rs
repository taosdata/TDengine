use taos_query::common::SchemalessPrecision;
use taos_query::common::SchemalessProtocol;
use taos_query::common::SmlDataBuilder;

use crate::AsyncQueryable;
use crate::AsyncTBuilder;
use crate::TaosBuilder;

async fn put_line() -> anyhow::Result<()> {
    // std::env::set_var("RUST_LOG", "taos=trace");
    std::env::set_var("RUST_LOG", "taos=debug");
    pretty_env_logger::init();

    let dsn =
        std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
    log::debug!("dsn: {:?}", &dsn);

    let client = TaosBuilder::from_dsn(dsn)?.build().await?;

    let db = "demo_schemaless_ws";

    client.exec(format!("drop database if exists {db}")).await?;

    client
        .exec(format!("create database if not exists {db}"))
        .await?;

    // should specify database before insert
    client.exec(format!("use {db}")).await?;

    let data = [
        "measurement,host=host1 field1=2i,field2=2.0 1577837300000",
        "measurement,host=host1 field1=2i,field2=2.0 1577837400000",
        "measurement,host=host1 field1=2i,field2=2.0 1577837500000",
        "measurement,host=host1 field1=2i,field2=2.0 1577837600000",
    ]
    .map(String::from)
    .to_vec();

    // demo with all fields
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .ttl(1000)
        .req_id(100u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default ttl
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .req_id(101u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default ttl and req_id 
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default precision
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .data(data)
        .req_id(103u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    client.exec(format!("drop database if exists {db}")).await?;

    Ok(())
}
