use chrono::prelude::*;
use std::time::Instant;
fn main() {
    use rumqttc::{Client, MqttOptions, QoS};
    use std::thread;
    use std::time::Duration;

    let mut mqttoptions = MqttOptions::new("rumqtt-sync", "127.0.0.1", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (mut client, mut connection) = Client::new(mqttoptions, 10);
    let topic = "camelCase";
    client.subscribe(topic, QoS::AtMostOnce).unwrap();
    thread::spawn(move || {
        for i in 0.. {
            let now = chrono::Utc::now();
            let json = serde_json::json!({
                "ts": now.to_rfc3339(),
                "id": i % 100,
                "current": 0.5,
                "voltage": i,
                "phase": 1.1,
                "groupId": "100",
                "locationName": "shanghai",
            });
            println!("i = {}", json);
            client
                .publish(
                    topic,
                    QoS::AtLeastOnce,
                    false,
                    serde_json::to_vec(&json).unwrap(),
                )
                .unwrap();
            //thread::sleep(Duration::from_millis(1));
        }
    });

    // Iterate to poll the eventloop for connection progress
    for (i, notification) in connection.iter().enumerate() {
        //println!("Notification {i} = {:?}", notification);
    }
}
