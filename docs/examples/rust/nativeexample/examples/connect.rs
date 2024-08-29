use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://localhost:6030".to_string();
    
    match TaosBuilder::from_dsn(&dsn)?.build().await {
        Ok(_taos) => {
            println!("Connected to {} successfully.", dsn);
            Ok(())
        }
        Err(err) => {
            eprintln!("Failed to connect to {}, ErrMessage: {}", dsn, err);
            return Err(err.into());
        }
    }
}
