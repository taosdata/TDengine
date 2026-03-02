use std::sync::Arc;

use anyhow::Context;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};

use taosx_utils::signal::{Signal, wait_signal};

use crate::{
    Args,
    api::start_http,
    controller::Controller,
    tasks::{monitor::start_monitor, rebalancer::start_rebalancer, updater::start_updater},
};

fn build_dsn(args: &Args) -> anyhow::Result<String> {
    if let Some(dsn) = &args.taos_dsn {
        return Ok(dsn.clone());
    }

    match (args.token.as_ref(), args.user_pass.as_ref()) {
        (Some(token), _) => Ok(format!(
            "taos:///?bearer_token={token}&cfgDir={}",
            args.cfg_dir
        )),
        (_, Some(user_pass)) => Ok(format!("taos://{user_pass}@/?cfgDir={}", args.cfg_dir)),
        _ => anyhow::bail!("either taos_dsn or token/user_pass must be provided"),
    }
}

#[tokio::main]
#[instrument(skip_all)]
pub async fn run(args: Args) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let dsn = build_dsn(&args)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    fn base_args() -> Args {
        Args {
            cfg_dir: "/etc/taos".to_string(),
            leader_ep: "127.0.0.1:7030".to_string(),
            cluster_id: "cluster".to_string(),
            log: crate::LogOpts {
                path: PathBuf::from("/tmp"),
                level: None,
                compress: None,
                rotation_count: None,
                keep_days: None,
                rotation_size: None,
                reserved_disk_size: None,
            },
            listen: None,
            taos_dsn: None,
            user_pass: None,
            token: None,
        }
    }

    #[test]
    fn build_dsn_prefers_explicit_taos_dsn() {
        let mut args = base_args();
        args.taos_dsn = Some("taos://explicit".to_string());
        args.token = Some("token".to_string());
        args.user_pass = Some("user:pass".to_string());

        let dsn = build_dsn(&args).expect("dsn");
        assert_eq!(dsn, "taos://explicit");
    }

    #[test]
    fn build_dsn_from_token() {
        let mut args = base_args();
        args.token = Some("abc".to_string());

        let dsn = build_dsn(&args).expect("dsn");
        assert_eq!(dsn, "taos:///?bearer_token=abc&cfgDir=/etc/taos");
    }

    #[test]
    fn build_dsn_from_user_pass() {
        let mut args = base_args();
        args.user_pass = Some("user:pass".to_string());

        let dsn = build_dsn(&args).expect("dsn");
        assert_eq!(dsn, "taos://user:pass@/?cfgDir=/etc/taos");
    }

    #[test]
    fn build_dsn_error_when_missing_all_credentials() {
        let args = base_args();
        let err = build_dsn(&args).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("either taos_dsn or token/user_pass must be provided"));
    }
}
