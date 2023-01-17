use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // subscribe
    let tmq_str = std::env::var("TDENGINE_CLOUD_TMQ")?;
    let tmq_uri = format!( "{}&group.id=test_group_rs&client.id=test_consumer_ws", tmq_str);
    println!("request tmq URI is {tmq_uri}\n");
    let tmq = TmqBuilder::from_dsn(tmq_uri,)?;
    let mut consumer = tmq.build()?;
    consumer.subscribe(["test"]).await?;

    // consume loop
    consumer
        .stream()
        .try_for_each_concurrent(10, |(offset, message)| async {
            let topic = offset.topic();
            // the vgroup id, like partition id in kafka.
            let vgroup_id = offset.vgroup_id();
            println!("* in vgroup id {vgroup_id} of topic {topic}\n");

            if let Some(data) = message.into_data() {
                while let Some(block) = data.fetch_raw_block().await? {
                    // A two-dimension matrix while each cell is a [taos::Value] object.
                    let values = block.to_values();
                    // Number of rows.
                    assert_eq!(values.len(), block.nrows());
                    // Number of columns
                    assert_eq!(values[0].len(), block.ncols());
                    println!("first row: {}", values[0].iter().join(", "));
                }
            }
            consumer.commit(offset).await?;
            Ok(())
        })
        .await?;

    consumer.unsubscribe().await;

    Ok(())
}