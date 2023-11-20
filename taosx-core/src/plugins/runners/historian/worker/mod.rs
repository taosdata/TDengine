use itertools::Itertools;

use crate::runners::historian::config::TaskConfig;
use crate::runners::historian::query::HistorianQuery;
use crate::runners::historian::worker::consumer::Consumer;
use crate::runners::historian::worker::producer::Producer;
use crate::utils::port_pool::PortPool;

mod consumer;
mod producer;

pub async fn migrate_history(config: TaskConfig, port_pool: &PortPool) -> anyhow::Result<()> {
    let (tx, rx) = flume::bounded(config.concurrency);

    let mut consumers = Vec::new();
    for _ in 0..config.concurrency {
        let receiver = rx.clone();
        let connect_config = config.connect.clone();
        let port_pool = port_pool.clone();

        let c = tokio::spawn(async move {
            let mut consumer = Consumer::new(
                HistorianQuery::new(connect_config).await?,
                port_pool,
            );
            consumer.consume(receiver).await
        });
        consumers.push(c);
    }

    let producer = Producer::new(&config);
    producer.produce(tx).await?;

    for c in consumers {
        c.await??;
    }

    Ok(())
}

pub async fn sync_history(task_config: TaskConfig, port_pool: &PortPool) -> anyhow::Result<()> {
    println!("sync history data, from: {}", task_config.begin_datetime);

    Ok(())
}

pub async fn sync_live(task_config: TaskConfig, port_pool: &PortPool) -> anyhow::Result<()> {
    println!("sync live data");

    Ok(())
}

