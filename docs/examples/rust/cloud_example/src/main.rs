use libtaos::*;

fn main() {
    let token =  std::env::var("TDENGINE_CLOUD_TOKEN").unwrap();
    let url = std::env::var("TDENGINE_CLOUD_URL").unwrap();
    let dsn = url + "?token=" + &token;
    let taos = TaosCfg::from_dsn(dsn)?;
    println!("connected");
}