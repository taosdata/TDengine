use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use arrow_schema::Schema;
use taos::Dsn;
use tokio::{task::JoinSet, time::sleep_until};
use tokio_util::sync::CancellationToken;

use source_mqtt::client::{GenericMessagePoller, MessagePoller};

use taosx_core::{
    Pipeline,
    plugins::transform::{
        parse::ParserImpl,
        sample::multi_schema::{MultiSchemaSamples, SampleWithSchema},
    },
    utils::futs_helper::{Select2, Select3, select2, select3},
};

use super::{batch::BatchPayload, pb::rebirth_payload, process_message};

pub async fn get_sample(
    dsn: &Dsn,
    limit: usize,
    timeout: Duration,
) -> anyhow::Result<MultiSchemaSamples> {
    let deadline = tokio::time::Instant::now() + timeout;
    let cancel = CancellationToken::new();

    let spb_config: super::config::Config = dsn.try_into().context("parse connect config error")?;
    let subscriptions = spb_config.subscribe.subscriptions();

    let mut tasks = JoinSet::new();
    let (payload_tx, payload_rx) = flume::bounded(limit);
    for mut config in spb_config
        .mqtt
        .mqtt_config()
        .context("parse mqtt config error")?
    {
        let client_id = format!(
            "_taosx_sample_spb_{}_{}",
            config.client_id,
            uuid::Uuid::new_v4().simple()
        );
        config.client_id = client_id;
        let poller = GenericMessagePoller::from_config(&config, subscriptions.clone())
            .await
            .context("build poller error")?;
        let client = poller.client();
        let send_rebirth_cmd = spb_config.subscribe.send_rebirth_cmd();
        tasks.spawn(process_message(
            poller,
            payload_tx.clone(),
            send_rebirth_cmd,
            None,
            cancel.clone(),
        ));
        if send_rebirth_cmd {
            if let Some(topics) = spb_config.subscribe.rebirth_topics() {
                for topic in topics {
                    let res = select3(
                        client.publish(&topic, 1, rebirth_payload()),
                        sleep_until(deadline),
                        cancel.cancelled(),
                    )
                    .await;
                    if !matches!(res, Select3::T1(_)) {
                        break;
                    }
                }
            }
        }
    }
    drop(payload_tx);

    let mut ret = HashMap::<Arc<Schema>, Vec<BatchPayload>>::new();
    let mut count = 0;
    loop {
        let Select2::T1(Ok((schema, payload))) =
            select2(payload_rx.recv_async(), sleep_until(deadline)).await
        else {
            break;
        };
        let payloads = ret.entry(schema).or_default();
        payloads.push(payload);
        count += 1;
        if count >= limit {
            break;
        }
    }

    cancel.cancel();
    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                anyhow::bail!("run sample task error: {e:#}");
            }
            Err(e) => {
                anyhow::bail!("sample task paniced: {e}")
            }
        }
    }
    let samples = ret
        .into_iter()
        .map(|(schema, input)| SampleWithSchema::new(&schema, input).context("build sample error"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(MultiSchemaSamples::new(
        Pipeline::default().with_parse(ParserImpl::default()),
        samples,
    ))
}
