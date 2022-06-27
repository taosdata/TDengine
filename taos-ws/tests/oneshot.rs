use tokio::sync::oneshot;
use tokio::time::{interval, sleep, Duration};

#[tokio::test(flavor = "multi_thread")]
async fn main() {
    let (send, mut recv) = oneshot::channel();
    let mut interval = interval(Duration::from_millis(100));

    tokio::spawn(async move {
        sleep(Duration::from_secs(1)).await;
        send.send("shut down").unwrap();
    });

    loop {
        tokio::select! {
            _ = interval.tick() => println!("Another 100ms"),
            msg = &mut recv => {
                println!("Got message: {}", msg.unwrap());
                break;
            }
        }
    }
}
