use std::time::{Duration, Instant};

use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init();

    // subscribe
    let group = chrono::Local::now().timestamp_nanos_opt().unwrap();
    let tmq = TmqBuilder::from_dsn(format!(
        "taos+ws://vm98:6041/?group.id={group}&experimental.snapshot.enable=true&timeout=5s&auto.offset.reset=earliest",
    ))?;
    println!("group id: {}", group);

    let mut consumer = tmq.build().await?;
    consumer.subscribe(["topic_ws_map"]).await?;
    let assignment = consumer.assignments().await;
    println!("assignments: {:?}", assignment);

    let mut blocks_cost = Duration::ZERO;
    {
        let mut stream = consumer.stream();
        println!("start consuming");

        let begin = Instant::now();

        let mut count = 0;
        while let Some((offset, message)) = stream.try_next().await? {
            // get information from offset

            if let Some(data) = message.into_data() {
                let _ = data.as_raw_data().await?;
                let now = Instant::now();
                while let Some(block) = data.fetch_raw_block().await? {
                    // one block for one table, get table name if needed
                    let row = block.nrows();
                    count += row;
                }
                blocks_cost += now.elapsed();
                if count >= 5000000 {
                    break;
                }
            }
            consumer.commit(offset).await?;
        }
        println!("total cost: {:?}", begin.elapsed());
    }
    println!("blocks cost: {:?}", blocks_cost);

    consumer.unsubscribe().await;

    Ok(())
}
