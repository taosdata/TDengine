use taos::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    #[allow(unused_variables)]
    let taos = TaosBuilder::from_dsn("taos://")?.build()?;
    println!("Connected");
    Ok(())
}
