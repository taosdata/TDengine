use taos_query::common::SchemalessPrecision;
use taos_query::common::SchemalessProtocol;
use taos_query::common::SmlDataBuilder;

use taos::AsyncQueryable;
use taos::AsyncTBuilder;
use taos::TaosBuilder;
use taos::taos_query;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "taos=debug");
    pretty_env_logger::init();
    let host = "localhost";
    let dsn = format!("ws://{}:6041/power", host);
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
        "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639",
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
    match client.put(&sml_data).await{
        Ok(_) => {},
        Err(err) => {
            eprintln!("Failed to insert data with schemaless, data:{:?}, ErrMessage: {}", data, err);
            return Err(err.into());
        }
    }

    // SchemalessProtocol::Telnet
    let data = [
        "metric_telnet 1707095283260 4 host=host0 interface=eth0",
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
    match client.put(&sml_data).await{
        Ok(_) => {},
        Err(err) => {
            eprintln!("Failed to insert data with schemaless, data:{:?}, ErrMessage: {}", data, err);
            return Err(err.into());
        }
    }

    // SchemalessProtocol::Json
    let data = [
        r#"[{
            "metric": "metric_json",
            "timestamp": 1626846400,
            "value": 10.3,
            "tags": {
                "groupid": 2,
                "location": "California.SanFrancisco",
                "id": "d1001"
            }
            }]"#
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
    match client.put(&sml_data).await{
        Ok(_) => {},
        Err(err) => {
            eprintln!("Failed to insert data with schemaless, data:{:?}, ErrMessage: {}", data, err);
            return Err(err.into());
        }
    }

    println!("Inserted data with schemaless successfully.");
    Ok(())
}
