use crate::lib::{run_example, stop_example};
use std::error::Error;
use tokio_cron_scheduler::JobScheduler;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod lib;

fn main() {
    let handle = std::thread::Builder::new()
        .name("schedule thread".to_string())
        .spawn(move || {
            // tokio::runtime::Builder::new_current_thread()    <- This hangs
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build runtime failed")
                .block_on(start())
                .expect("TODO: panic message");
        })
        .expect("spawn thread failed");
    handle.join().expect("join failed");
}

async fn start() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
    info!("Creating scheduler");
    let mut sched = JobScheduler::new().await?;
    info!("Run example");
    let jobs = run_example(&mut sched)
        .await
        .expect("Could not run example");
    stop_example(&mut sched, jobs)
        .await
        .expect("Could not stop example");
    Ok(())
}
