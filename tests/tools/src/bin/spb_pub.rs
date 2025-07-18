use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use clap::Parser;
use rumqttc::v5::{
    mqttbytes::{
        v5::{ConnectReturnCode, LastWill, SubscribeReasonCode},
        QoS,
    },
    AsyncClient, Event, EventLoop, Incoming, MqttOptions,
};
use taosx_tools::{
    fake_spb::{message_type::MessageType, topic::TopicComponents, NodeDeviceFaker, Schema},
    generate_random_string, select3,
    signal::wait_signal,
    Select3,
};
use tokio::{sync::Notify, task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(long)]
    schema: PathBuf,
    #[arg(long = "host", default_value = "localhost")]
    broker_host: String,
    #[arg(long = "port", default_value_t = 1883)]
    broker_port: u16,
    #[arg(long = "report-interval", default_value = "5s", value_parser = fundu::parse_duration)]
    report_interval: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let schema = Schema::from_file(args.schema)
        .await
        .context("parse schema from file error")?;

    let cancel = CancellationToken::new();
    let mut tasks = JoinSet::new();

    let group_id = schema.group_id.clone();
    for (node_id, (node_faker, devices)) in schema.node_devices() {
        let Some(node_faker) = node_faker else {
            anyhow::bail!("node {node_id} metrics is empty");
        };
        // 创建 client
        let client_id = format!(
            "fake_spb_tool_{group_id}_{node_id}_{}",
            generate_random_string(10)
        );
        println!("client_id: {}", client_id);
        let mut opts = MqttOptions::new(client_id, &args.broker_host, args.broker_port);
        opts.set_keep_alive(Duration::from_secs(10));
        opts.set_last_will(LastWill::new(
            format!("spBv1.0/{group_id}/NDEATH/{node_id}"),
            node_faker
                .death_payload()
                .context("build death payload error")?,
            QoS::AtLeastOnce,
            false,
            None,
        ));
        let (client, poller) = AsyncClient::new(opts, 100);

        let birth_notify = Arc::new(Notify::new());

        // 订阅 rebirth 命令
        tasks.spawn(poll(
            client.clone(),
            poller,
            node_faker.clone(),
            birth_notify.clone(),
            cancel.child_token(),
        ));

        // 上报 node 节点数据
        println!("start report {} data", node_faker.display_id());
        tasks.spawn(report(
            client.clone(),
            node_faker,
            args.report_interval,
            birth_notify.clone(),
            cancel.child_token(),
        ));

        // 上报 device 数据
        for device_faker in devices {
            println!("start report {} data", device_faker.display_id());
            tasks.spawn(report(
                client.clone(),
                device_faker,
                args.report_interval,
                birth_notify.clone(),
                cancel.child_token(),
            ));
        }
    }

    wait_signal().await.context("wait signal error")?;
    cancel.cancel();

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                println!("task exit with error: {e:#}");
            }
            Err(e) => {
                println!("task panic: {e}")
            }
        }
    }
    Ok(())
}

async fn poll(
    client: AsyncClient,
    mut poller: EventLoop,
    faker: NodeDeviceFaker,
    birth_notify: Arc<Notify>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let ncmd_topic = faker.ncmd_topic();
    let Some(event) = cancel
        .run_until_cancelled(poller.poll())
        .await
        .transpose()
        .context("fetch mqtt event error")?
    else {
        return Ok(());
    };
    let Event::Incoming(Incoming::ConnAck(ack)) = event else {
        anyhow::bail!("expected a connack packet")
    };
    if !matches!(ack.code, ConnectReturnCode::Success) {
        anyhow::bail!("connect error: {:?}", ack.code);
    }
    println!("successfully connected");
    // 通知 node 和 device 发送 birth 命令
    birth_notify.notify_waiters();
    // 订阅 NCMD 命令
    client
        .subscribe(&ncmd_topic, QoS::AtMostOnce)
        .await
        .context("subscribe NCMD error")?;
    loop {
        let Some(event) = cancel.run_until_cancelled(poller.poll()).await else {
            break;
        };
        match event {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                if !matches!(ack.code, ConnectReturnCode::Success) {
                    anyhow::bail!("connection error: {:?}", ack.code);
                }
                // 通知 node 和 device 发送 birth 命令
                birth_notify.notify_waiters();
                // 订阅 NCMD 命令
                client
                    .subscribe(&ncmd_topic, QoS::AtMostOnce)
                    .await
                    .context("subscribe NCMD error")?;
            }
            Ok(Event::Incoming(Incoming::SubAck(ack))) => {
                let Some(code) = ack.return_codes.first() else {
                    break;
                };
                match code {
                    SubscribeReasonCode::Success(qos) => {
                        println!("subscribe success: {ncmd_topic}, qos: {}", *qos as u8);
                    }
                    code => {
                        println!("subscribe failed: {code:?}")
                    }
                }
            }
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                let topic =
                    String::from_utf8(publish.topic.to_vec()).context("topic not valid utf8")?;
                let components: TopicComponents = topic.parse()?;
                if matches!(components.message_type, MessageType::NCmd)
                    && components.edge_node_id == faker.node_id
                {
                    println!("received rebirth cmd: {topic}");
                    birth_notify.notify_waiters();
                }
            }
            Ok(_) => {}
            Err(e) => {
                println!("poll message error: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    anyhow::Ok(())
}

async fn report(
    client: AsyncClient,
    mut faker: NodeDeviceFaker,
    interval: Duration,
    birth_notify: Arc<Notify>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let birth_topic = faker.birth_topic();
    let data_topic = faker.data_topic();
    loop {
        match select3(sleep(interval), birth_notify.notified(), cancel.cancelled()).await {
            Select3::T1(_) => {
                client
                    .publish(
                        data_topic.as_str(),
                        QoS::AtMostOnce,
                        false,
                        faker.data_payload().context("build data payload error")?,
                    )
                    .await
                    .context("send DDATA payload error")?;
            }
            Select3::T2(_) => {
                client
                    .publish(
                        birth_topic.as_str(),
                        QoS::AtMostOnce,
                        false,
                        faker.birth_payload().context("build birth payload error")?,
                    )
                    .await
                    .context("send DBIRTH payload error")?;
            }
            Select3::T3(_) => break,
        }
    }
    anyhow::Ok(())
}
