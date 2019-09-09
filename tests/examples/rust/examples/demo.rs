use std::process;
use tdengine::Tdengine;

fn main() {
    let tde = Tdengine::new("127.0.0.1", "root", "taosdata", "demo", 0)
                .unwrap_or_else(|err| {
        eprintln!("Can't create Tdengine: {}", err);
        process::exit(1)
    });

    tde.query("drop database demo");
    tde.query("create database demo");
    tde.query("use demo");
    tde.query("create table m1 (ts timestamp, speed int)");

    for i in 0..10 {
        tde.query(format!("insert into m1 values (now+{}s, {})", i, i).as_str());
    }
}
