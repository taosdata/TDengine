use crate::lib::{run_example, stop_example};
use tokio_cron_scheduler::JobScheduler;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod lib;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
    let sched = JobScheduler::new_with_channel_size(1000).await;
    let mut sched = sched.unwrap();
    let jobs = run_example(&mut sched)
        .await
        .expect("Could not run example");
    stop_example(&mut sched, jobs)
        .await
        .expect("Could not stop example");
}
