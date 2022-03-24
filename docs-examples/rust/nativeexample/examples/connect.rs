use libtaos::*;

fn taos_connect() -> Result<Taos, Error> {
    TaosCfgBuilder::default()
        .ip("localhost")
        .user("root")
        .pass("taosdata")
        // .db("log") // remove comment if you want to connect to database log by default.
        .port(6030u16)
        .build()
        .expect("TaosCfg builder error")
        .connect()
}

fn main() {
    #[allow(unused_variables)]
    let taos = taos_connect().unwrap();
    println!("Connected")
}
