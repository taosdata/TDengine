use std::sync::Arc;

use anyhow::Context;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};

use crate::{
    Args,
    api::start_http,
    controller::Controller,
    tasks::{monitor::start_monitor, rebalancer::start_rebalancer, updater::start_updater},
    utils::signal::{Signal, wait_signal},
};

#[tokio::main]
#[instrument(skip_all)]
pub async fn run(args: Args) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let dsn = match &args.taos_dsn {
        Some(v) => v.clone(),
        None => match (args.token.as_ref(), args.user_pass.as_ref()) {
            (Some(token), _) => {
                format!("taos:///?bearer_token={}&cfgDir={}", token, args.cfg_dir)
            }
            (_, Some(user_pass)) => {
                format!("taos://{}@/?cfgDir={}", user_pass, args.cfg_dir)
            }
            _ => {
                anyhow::bail!("either taos_dsn or token/user_pass must be provided");
            }
        },
    };

    let (rebalance_tx, rebalance_rx) = flume::bounded(100);
    let controller = Arc::new(
        Controller::create(&args, &dsn, rebalance_tx, cancel.clone())
            .await
            .context("build controller error")?,
    );
    let mut tasks = JoinSet::new();

    // http server
    tasks.spawn({
        let cancel = cancel.clone();
        let controller = controller.clone();
        async move {
            start_http(args.listen.clone(), controller, cancel)
                .in_current_span()
                .await
                .context("run http server error")
        }
    });

    // monitor
    tasks.spawn({
        let cancel = cancel.clone();
        let leader_ep = args.leader_ep.clone();
        let dsn = dsn.clone();
        start_monitor(dsn, leader_ep, cancel).in_current_span()
    });

    // updater
    tasks.spawn({
        let cancel = cancel.clone();
        start_updater(dsn, controller.xnodes(), controller.tasks(), cancel).in_current_span()
    });

    // rebalancer
    tasks.spawn({
        let controller = controller.clone();
        let cancel = cancel.clone();
        start_rebalancer(controller, rebalance_rx, cancel).in_current_span()
    });

    let mut exit_result = Ok(());
    match cancel.run_until_cancelled(wait_signal()).await {
        Some(Ok(signal)) => {
            if !matches!(signal, Signal::Interrupt | Signal::Terminate) {
                exit_result = Err(anyhow::anyhow!("signal received: {signal}"));
            }
            tracing::info!("signal received: {signal}");
        }
        Some(Err(e)) => {
            tracing::error!("wait signal error: {e:#}");
        }
        None => {}
    }

    cancel.cancel();

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("task error: {e:#}");
            }
            Err(e) => {
                tracing::error!("task panic: {e:#}");
            }
        }
    }

    exit_result
}
