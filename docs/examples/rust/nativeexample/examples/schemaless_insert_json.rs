use taos_query::common::SchemalessPrecision;
use taos_query::common::SchemalessProtocol;
use taos_query::common::SmlDataBuilder;

use crate::AsyncQueryable;
use crate::AsyncTBuilder;
use crate::TaosBuilder;

async fn put_json() -> anyhow::Result<()> {
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

    // SchemalessProtocol::Json
    let data = [
        r#"[{"metric": "meters.current", "timestamp": 1681345954000, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "California.LosAngeles", "groupid": 1}}, {"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}}]"#
    ]
    .map(String::from)
    .to_vec();

    // demo with all fields
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Json)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .ttl(1000)
        .req_id(300u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default precision
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Json)
        .data(data.clone())
        .ttl(1000)
        .req_id(301u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default ttl
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Json)
        .data(data.clone())
        .req_id(302u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default req_id
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Json)
        .data(data.clone())
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    client.exec(format!("drop database if exists {db}")).await?;

    Ok(())
}
