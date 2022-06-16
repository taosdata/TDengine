use libtaos::schemaless::*;
use libtaos::*;

fn main() {
    let taos = TaosCfg::default().connect().expect("fail to connect");
    taos.raw_query("CREATE DATABASE test").unwrap();
    taos.raw_query("USE test").unwrap();
    let lines = [
        r#"[{"metric": "meters.current", "timestamp": 1648432611249, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}},
        {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "California.LosAngeles", "groupid": 1}},
        {"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "California.SanFrancisco", "groupid": 2}},
        {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}}]"#,
    ];

    let affected_rows = taos
        .schemaless_insert(
            &lines,
            TSDB_SML_JSON_PROTOCOL,
            TSDB_SML_TIMESTAMP_NOT_CONFIGURED,
        )
        .unwrap();
    println!("affected_rows={}", affected_rows); // affected_rows=4
}

// run with:  cargo run --example opentsdb_json_example
