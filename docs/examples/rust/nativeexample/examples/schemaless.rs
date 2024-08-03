use taos_query::common::SchemalessPrecision;
use taos_query::common::SchemalessProtocol;
use taos_query::common::SmlDataBuilder;

use crate::AsyncQueryable;
use crate::AsyncTBuilder;
use crate::TaosBuilder;

async fn put() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "taos=debug");
    pretty_env_logger::init();
    let dsn =
        std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("taos://localhost:6030".to_string());
    log::debug!("dsn: {:?}", &dsn);

    let client = TaosBuilder::from_dsn(dsn)?.build().await?;

    let db = "power";

    client
        .exec(format!("create database if not exists {db}"))
        .await?;

    // should specify database before insert
    client.exec(format!("use {db}")).await?;

    // SchemalessProtocol::Line
    let data = [
        "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639000000",
    ]
    .map(String::from)
    .to_vec();

    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .ttl(1000)
        .req_id(100u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // SchemalessProtocol::Telnet
    let data = [
        "metric_telnet 1648432611249 10.3 location=California.SanFrancisco group=2",
    ]
    .map(String::from)
    .to_vec();

    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Telnet)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .ttl(1000)
        .req_id(200u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // SchemalessProtocol::Json
    let data = [
        r#"[{"metric": "metric_json", "timestamp": 1681345954000, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "California.LosAngeles", "groupid": 1}}, {"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}}]"#
    ]
    .map(String::from)
    .to_vec();

    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Json)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .ttl(1000)
        .req_id(300u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    Ok(())
}
