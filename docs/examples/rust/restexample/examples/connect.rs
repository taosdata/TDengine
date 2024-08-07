use taos::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    #[allow(unused_variables)]
    let taos = TaosBuilder::from_dsn("taos+ws://localhost:6041")?.build()?;
    println!("Connected to localhost with websocket connection successfully.");
    Ok(())
}
