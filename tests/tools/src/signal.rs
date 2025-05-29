use futures::FutureExt;
use tokio::signal::unix::{signal, SignalKind};

use crate::select4;

pub async fn wait_signal() -> std::io::Result<()> {
    let _ = select4(
        signal(SignalKind::interrupt())?.recv().inspect(|_| {
            println!("receive SIGINT signal");
        }),
        signal(SignalKind::terminate())?.recv().inspect(|_| {
            println!("receive SIGTERM signal");
        }),
        signal(SignalKind::hangup())?.recv().inspect(|_| {
            println!("receive SIGUP signal");
        }),
        signal(SignalKind::quit())?.recv().inspect(|_| {
            println!("receive SIGQUIT signal");
        }),
    )
    .await;
    Ok(())
}
