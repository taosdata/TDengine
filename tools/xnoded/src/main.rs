#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    loop {
        println!("xxxzgc *** Hello, world!");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
