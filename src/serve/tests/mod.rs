use std::sync::Arc;

use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use taosx_core::set_env_data_dir;

use crate::serve::{
    controller::Activity,
    scheduler::{
        SchedulerNotify,
        agent::{AgentNotify, AgentWorker},
        runner::AgentIntegrationChannel,
    },
};

use super::{
    controller::TaskController,
    scheduler::{NotifyChannel, TaskScheduler, agent::AgentNotifySender},
    *,
};

pub(crate) async fn generate_scheduler_for_test()
-> anyhow::Result<(TaskController, TaskScheduler, AgentNotifySender)> {
    let (agent_activity_sender, agent_activity_receiver) = tokio::sync::broadcast::channel(1024);
    let (agent_notify_sender, agent_notify_receiver) = tokio::sync::broadcast::channel(1024);
    let (scheduler_notify_sender, _) = tokio::sync::broadcast::channel::<SchedulerNotify>(1024);
    let scheduler_notify_sender = Arc::new(scheduler_notify_sender);

    let weak_notify_sender = Arc::downgrade(&scheduler_notify_sender);
    let agent_notify_sender_cloned = agent_notify_sender.clone();
    tokio::spawn(async move {
        tokio::pin!(agent_activity_receiver);
        loop {
            match agent_activity_receiver.recv().await {
                Ok((agent, action)) => {
                    tracing::info!(agent, "agent action: {:?}", action);
                    match action {
                        crate::serve::controller::AgentAction::Run(id, _, _) => {
                            tracing::info!("task run: {id}");
                        }
                        crate::serve::controller::AgentAction::Stop(id) => {
                            tracing::info!("tasks stop: {}", id);
                            let agent_notify_sender_cloned = agent_notify_sender_cloned.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                tracing::info!("agent cancel");
                                let _ = agent_notify_sender_cloned
                                    .send(AgentNotify::TaskActivity(agent, Activity::stopped(id)));
                            });
                        }
                        crate::serve::controller::AgentAction::Cancel(id) => {
                            tracing::info!("task suspend: {}", id);
                            let agent_notify_sender_cloned = agent_notify_sender_cloned.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                tracing::info!("agent cancel");
                                let _ = agent_notify_sender_cloned.send(AgentNotify::TaskActivity(
                                    agent,
                                    Activity::suspended(id, Uuid::nil()),
                                ));
                            });
                        }
                        crate::serve::controller::AgentAction::ListDataSets(_, _) => {
                            // TODO
                        }
                        crate::serve::controller::AgentAction::RetrieveDataSets(_, _) => {
                            // TODO
                        }
                        crate::serve::controller::AgentAction::Interrupt(_) => {
                            // TODO
                        }
                        crate::serve::controller::AgentAction::Check(_, _) => {
                            // TODO
                        }
                        crate::serve::controller::AgentAction::GetSample(_, _) => {
                            // TODO
                        }
                        crate::serve::controller::AgentAction::PutFile(_, _) => {
                            // TODO
                        }
                        crate::serve::controller::AgentAction::QueryDataSource(_, _) => {
                            // TODO
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged)) => {
                    tracing::warn!(
                        "agent activity channel lagged: {lagged}, resubscribe it from current offset"
                    );
                    continue;
                }
            }
        }
    });

    let (_agent_spawn_sender, agent_spawn_receiver) = flume::bounded(0);

    let agent_worker = AgentWorker::new(
        agent_activity_sender,
        agent_notify_receiver,
        weak_notify_sender,
        agent_spawn_receiver,
    )
    .await;
    let agent_integration_channel = AgentIntegrationChannel::Server(agent_worker);

    let scheduler = TaskScheduler::new(scheduler_notify_sender, agent_integration_channel)
        .await
        .unwrap();
    tracing::info!("scheduler created: {:?}", scheduler);
    let controller = TaskController::from_sqlite("sqlite::memory:", scheduler.clone(), 100).await?;
    tracing::info!("task controller created: {:?}", scheduler);
    Ok((controller, scheduler, agent_notify_sender))
}

pub async fn wait_notify_channel(notify_channel: NotifyChannel) {
    tracing::info!("notify_channel length: {}", notify_channel.len());
    tokio::pin!(notify_channel);

    loop {
        match notify_channel.recv().await {
            Ok(act) => {
                dbg!(act);
            }
            Err(err) => {
                dbg!(&err);
                match err {
                    tokio::sync::broadcast::error::RecvError::Closed => {
                        tracing::info!("notify channel closed");
                        break;
                    }
                    tokio::sync::broadcast::error::RecvError::Lagged(lagged) => {
                        tracing::warn!("notify channel lagged: {lagged}, continue");
                        continue;
                    }
                }
            }
        }
    }
}
pub fn tracing_subscriber_init() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse()?))
        .with_file(true)
        .pretty()
        .try_init();
    set_env_data_dir("");
    Ok(())
}

#[test]
fn ssl() {
    let cli = Cli::parse_from([
        "",
        "--ssl-cert",
        "cert.pem",
        "--ssl-key",
        "key.pem",
        "--ssl-ca",
        "ca.pem",
    ]);
    assert_eq!(cli.ssl_cert, Some("cert.pem".to_string()));
    assert_eq!(cli.ssl_key, Some("key.pem".to_string()));
}

#[test]
fn merge_cli_options() {
    let mut cli = Cli::parse_from([
        "",
        "--ssl-cert",
        "cert.pem",
        "--ssl-key",
        "key.pem",
        "--ssl-ca",
        "ca.pem",
    ]);
    assert_eq!(cli.ssl_cert.as_deref(), Some("cert.pem"));
    assert_eq!(cli.ssl_key.as_deref(), Some("key.pem"));
    assert_eq!(cli.ssl_ca.as_deref(), Some("ca.pem"));

    let rhs = Cli::default();
    assert_eq!(rhs.ssl_cert, None);
    assert_eq!(rhs.ssl_key, None);

    cli.merge_from(rhs);

    assert_eq!(cli.ssl_cert.as_deref(), Some("cert.pem"));
    assert_eq!(cli.ssl_key.as_deref(), Some("key.pem"));
    assert_eq!(cli.ssl_ca.as_deref(), Some("ca.pem"));

    let mut lhs = Cli::parse_from(["", "--listen", "0.0.0.0:6050"]);

    assert_eq!(lhs.listen.as_deref(), Some("0.0.0.0:6050"));
    assert!(lhs.ssl_cert.is_none());

    lhs.merge_from(cli);
    assert_eq!(lhs.listen.as_deref(), Some("0.0.0.0:6050"));
    assert_eq!(lhs.ssl_cert.as_deref(), Some("cert.pem"));
    assert_eq!(lhs.ssl_key.as_deref(), Some("key.pem"));
    assert_eq!(lhs.ssl_ca.as_deref(), Some("ca.pem"));
}
