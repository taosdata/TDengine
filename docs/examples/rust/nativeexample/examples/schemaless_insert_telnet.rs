use taos_query::common::SchemalessPrecision;
use taos_query::common::SchemalessProtocol;
use taos_query::common::SmlDataBuilder;

use crate::AsyncQueryable;
use crate::AsyncTBuilder;
use crate::TaosBuilder;

async fn put_telnet() -> anyhow::Result<()> {
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
        "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2",
        "meters.current 1648432611250 12.6 location=California.SanFrancisco group=2",
        "meters.current 1648432611249 10.8 location=California.LosAngeles group=3",
        "meters.current 1648432611250 11.3 location=California.LosAngeles group=3",
        "meters.voltage 1648432611249 219 location=California.SanFrancisco group=2",
        "meters.voltage 1648432611250 218 location=California.SanFrancisco group=2",
        "meters.voltage 1648432611249 221 location=California.LosAngeles group=3",
        "meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
    ]
    .map(String::from)
    .to_vec();

    // demo with all fields
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Telnet)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .ttl(1000)
        .req_id(200u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default precision
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Telnet)
        .data(data.clone())
        .ttl(1000)
        .req_id(201u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default ttl
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Telnet)
        .data(data.clone())
        .req_id(202u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default req_id
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Telnet)
        .data(data.clone())
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    client.exec(format!("drop database if exists {db}")).await?;

    Ok(())
}
