use taos::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    #[allow(unused_variables)]
    let taos = TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;
    println!("Connected to localhost with native connection successfully.");
    Ok(())
}
