use taos::tmq::Consumer;
use taos::prelude::sync::*;

fn main() -> anyhow::Result<()> {
    let mut consumer = taos::tmq::TmqBuilder::from_dsn("taos:///")?.build()?;

    loop {
        if let Some(offset, message) = consumer.next().transpose()? {
            let topic = offset.topic();
            let db = offset.database();
            let table = offset.table();

            // consume message.
            let data = message.to_owned_message();


            // you can save the offset and commit it after all message are consumed.
            consumer.commit_sync(offset);

        }
    }
    Ok(())
}
