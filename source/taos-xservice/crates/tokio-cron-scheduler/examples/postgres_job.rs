mod lib;
use crate::lib::{run_example, stop_example};
use tokio_cron_scheduler::{
    JobScheduler, PostgresMetadataStore, PostgresNotificationStore, SimpleJobCode,
    SimpleNotificationCode,
};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    info!("Remember to have a running Postgres instance to connect to. For example:\n");
    info!("docker run --rm -it -p 5432:5432 -e POSTGRES_USER=\"postgres\" -e POSTGRES_PASSWORD=\"\" -e POSTGRES_HOST_AUTH_METHOD=\"trust\" postgres:14.1");

    let metadata_storage = Box::new(PostgresMetadataStore::default());
    let notification_storage = Box::new(PostgresNotificationStore::default());
    if std::env::var("POSTGRES_INIT_METADATA").is_err() {
        info!("Set to not initialize the job metadata tables. POSTGRES_INIT_METADATA=false");
    }
    if std::env::var("POSTGRES_INIT_NOTIFICATIONS").is_err() {
        info!(
            "Set to not initialization of notification tables. POSTGRES_INIT_NOTIFICATIONS=false"
        );
    }

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
