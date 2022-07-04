use libtaos::schemaless::*;
use libtaos::*;

fn main() {
    let taos = TaosCfg::default().connect().expect("fail to connect");
    taos.raw_query("CREATE DATABASE test").unwrap();
    taos.raw_query("USE test").unwrap();
    let lines = ["meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
    "meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
    "meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
    "meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250"];
    let affected_rows = taos
        .schemaless_insert(
            &lines,
            TSDB_SML_LINE_PROTOCOL,
            TSDB_SML_TIMESTAMP_MILLISECONDS,
        )
        .unwrap();
    println!("affected_rows={}", affected_rows);
}

// run with:  cargo run --example influxdb_line_example
