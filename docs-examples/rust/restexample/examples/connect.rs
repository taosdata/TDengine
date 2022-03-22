use libtaos::*;

fn taos_connect() -> Result<Taos, Error> {
    TaosCfgBuilder::default()
        .ip("127.0.0.1")
        .user("root")
        .pass("taosdata")
        .db("log")
        .port(6030u16)
        .build()
        .expect("ToasCfg builder error")
        .connect()
}

fn main() {
    #[allow(unused_variables)]
    let taos = taos_connect().unwrap();
    println!("Connected")
}
