use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::Parser;
use rand::distributions::{Alphanumeric, DistString};
use rumqttc::{
    v5::{
        mqttbytes::{
            qos,
            v5::{ConnectReturnCode, PubAckReason, PubRecReason},
        },
        Event, Incoming, MqttOptions,
    },
    Outgoing,
};
use tokio::{signal::ctrl_c, sync::oneshot, task::JoinSet};
use tokio_util::sync::CancellationToken;

use taosx_tools::faker::DataFaker;

#[derive(Debug, clap::Parser)]
struct Args {
    #[clap(long = "schema", short = 's')]
    schema: PathBuf,
    #[clap(long = "host", default_value = "localhost")]
    broker_host: String,
    #[clap(long = "port", default_value_t = 1883)]
    broker_port: u16,
    #[clap(long = "username", short = 'u')]
    username: Option<String>,
    #[clap(long = "password", short = 'p')]
    password: Option<String>,
    #[clap(long = "keep_alive", short = 'k', default_value = "5s", value_parser = fundu::parse_duration)]
    keep_alive: Duration,
    #[clap(long = "client_id", short = 'c', default_value_t = format!("mqtt_pub_tool_{}", generate_random_string(10)))]
    client_id: String,
    #[clap(long = "topic", short = 't')]
    topic: String,
    #[clap(long = "qos", short = 'q', default_value_t = 0)]
    qos: u8,
    #[clap(long = "perallel", short = 'z', default_value_t = std::thread::available_parallelism().unwrap().get())]
    perallel: usize,
    #[clap(long = "interval", short = 'i', default_value = "100ms", value_parser = fundu::parse_duration)]
    interval: Duration,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("client_id: {}", args.client_id);
    publish(args).await;
}

/// TODO: qos=0 大数据量 publish 时，偶发性报错 I/O Connection reset by peer，订阅端会收到不合法的 json 字符串
async fn publish(args: Args) {
    let faker = Arc::new(DataFaker::from_toml(args.schema).await.unwrap());
    enum WatchEvent {
        ConnAck,
    }

    let client_id = args.client_id;
    let mut opts = MqttOptions::new(client_id, args.broker_host, args.broker_port);
    if let (Some(username), Some(password)) = (args.username, args.password) {
        opts.set_credentials(username, password);
    }
    opts.set_keep_alive(args.keep_alive);

    let perallel = args.perallel;

    let (client, mut event_loop) = rumqttc::v5::AsyncClient::new(opts, 1000);

    let token = CancellationToken::new();
    let mut join_set = JoinSet::new();
    let (tx, rx) = flume::bounded(perallel);
    join_set.spawn({
        let token = token.clone();
        async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut published = 0;
            let start = Instant::now();
            loop {
                tokio::select! {
                    res = event_loop.poll() => match res {
                        Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                            if matches!(ack.code, ConnectReturnCode::Success) {
                                tx.try_send(WatchEvent::ConnAck).ok();
                            } else {
                                println!("connect error: {:?}", ack.code);
                                break
                            }
                        },
                        Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                            if matches!(ack.reason, PubAckReason::Success) {
                                published += 1;
                            } else {
                                println!("publish error: {:?}", ack.reason);
                            }
                        },
                        Ok(Event::Incoming(Incoming::PubRec(ack))) => {
                            if matches!(ack.reason, PubRecReason::Success) {
                                published += 1;
                            } else {
                                println!("publish error: {:?}", ack.reason);
                            }
                        },
                        Ok(Event::Outgoing(Outgoing::Publish(_))) => {
                            if matches!(args.qos, 0) {
                               published += 1;
                            }
                        }
                        Ok(_) => {},
                        Err(e) => {
                            println!("polling error: {e}");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        },
                    },
                    _ = ticker.tick() => {
                        let duration = start.elapsed().as_secs();
                        if duration == 0 {
                            continue;
                        }
                        println!("published {published}, speed: {}/s", published / duration);
                    }
                    _ = token.cancelled() => break
                }
            }
        }
    });

    let Ok(WatchEvent::ConnAck) = rx.recv_async().await else {
        println!("connect error, exit");
        return;
    };

    for _ in 0..perallel {
        join_set.spawn({
            let token = token.clone();
            let client = client.clone();
            let topic = args.topic.clone();
            let faker = faker.clone();
            async move {
                let qos = qos(args.qos).unwrap();
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(args.interval) => {
                            let (tx, rx) = oneshot::channel();
                            rayon::spawn({
                                let faker = faker.clone();
                                move || {
                                    let value = faker.rand_json().inspect_err(|e| println!("gen fake data error: {e}")).unwrap();
                                    tx.send(serde_json::to_vec(&value).expect("json serialize error")).ok();
                                }
                            });
                            let Ok(value) = rx.await else {
                                continue
                            };
                            tokio::select! {
                                _ = client.publish(&topic, qos, false, value) => {},
                                _ = token.cancelled() => break
                            }
                        },
                        _ = token.cancelled() => break
                    }
                }
            }
        });
    }

    ctrl_c().await.ok();
    println!("received ctrl_c signal");
    match tokio::time::timeout(Duration::from_secs(5), client.disconnect()).await {
        Ok(_) => println!("mqtt client disconnected"),
        Err(_) => println!("mqtt client disconnected timeout"),
    }
    token.cancel();
    join_set.abort_all();
    while join_set.join_next().await.is_some() {}
}

fn generate_random_string(length: usize) -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), length)
}
