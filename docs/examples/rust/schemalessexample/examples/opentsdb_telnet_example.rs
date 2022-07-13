use libtaos::schemaless::*;
use libtaos::*;

fn main() {
    let taos = TaosCfg::default().connect().expect("fail to connect");
    taos.raw_query("CREATE DATABASE test").unwrap();
    taos.raw_query("USE test").unwrap();
    let lines = [
        "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
        "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
        "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
        "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
        "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
        "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
        "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
        "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
    ];
    let affected_rows = taos
        .schemaless_insert(
            &lines,
            TSDB_SML_TELNET_PROTOCOL,
            TSDB_SML_TIMESTAMP_NOT_CONFIGURED,
        )
        .unwrap();
    println!("affected_rows={}", affected_rows); // affected_rows=8
}

// run with:  cargo run --example opentsdb_telnet_example
