use crate::lib::{run_example, stop_example};
use tokio_cron_scheduler::{
    JobScheduler, NatsMetadataStore, NatsNotificationStore, SimpleJobCode, SimpleNotificationCode,
};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod lib;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    info!("Remember to have a running Nats instance to connect to. For example:\n");
    info!("docker run --rm -it -p 4222:4222 -p 6222:6222 -p 7222:7222 -p 8222:8222 nats -js -DV");
    let metadata_storage = Box::new(NatsMetadataStore::default());
    let notification_storage = Box::new(NatsNotificationStore::default());

    let simple_job_code = Box::new(SimpleJobCode::default());
    let simple_notification_code = Box::new(SimpleNotificationCode::default());

    let mut sched = JobScheduler::new_with_storage_and_code(
        metadata_storage,
        notification_storage,
        simple_job_code,
        simple_notification_code,
        200,
    )
    .await
    .unwrap();

    let jobs = run_example(&mut sched)
        .await
        .expect("Could not run example");
    stop_example(&mut sched, jobs)
        .await
        .expect("Could not stop example");
}
