use std::{collections::HashMap, path::PathBuf, time::Duration};

use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use ha_core::{batch::BatchIter, consts::*, types::XnodedId};
use tokio::{task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

#[derive(Debug, Clone, clap::Parser)]
struct Args {
    #[arg(short, long, default_value = "http://localhost:6055")]
    addr: String,
    #[arg(short = 'f', long)]
    payload_file: PathBuf,
    #[arg(short, long)]
    cluster_id: String,
    #[arg(short, long)]
    leader_ep: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let payloads = tokio::fs::read_to_string(&args.payload_file)
        .await
        .context("read payload file error")?;
    let payloads = serde_json::from_str::<Vec<HashMap<String, serde_json::Value>>>(&payloads)
        .context("payload invalid json")?;

    let cluster_id = args.cluster_id;
    let leader_ep = args.leader_ep;

    let uri = args.addr.parse().context("invalid address")?;
    let channel = Channel::builder(uri)
        .connect()
        .await
        .context("connect grpc server error")?;

    let cancel = CancellationToken::new();
    let parallel = std::thread::available_parallelism()
        .context("get available parallelism error")?
        .get();

    let (event_tx, event_rx) = flume::bounded(1000);
    let xnoded_id = XnodedId {
        cluster_id,
        leader_ep,
    };
    let client = ha_rpc_client::create_client(
        channel,
        &xnoded_id,
        event_tx,
        cancel.child_token(),
        parallel,
    )
    .await
    .context("build ha rpc client error")?;

    let mut tasks = JoinSet::new();

    tasks.spawn({
        let cancel = cancel.clone();
        async move {
            let _guard = cancel.drop_guard_ref();

            for mut payload in payloads {
                let enabled = payload
                    .remove("enabled")
                    .is_some_and(|v| v.as_bool().is_some_and(|v| v));
                if !enabled {
                    continue;
                }
                for (action, context) in payload {
                    macro_rules! send_recv {
                        ($action: expr, $method: ident) => {
                            let param = serde_json::from_value(context)
                                .with_context(|| format!("deserialize {} param", $action))?;
                            let res = client
                                .$method(&param)
                                .await
                                .with_context(|| format!("client {} error", $action))?;
                            let res = serde_json::to_string(&res)
                                .with_context(|| format!("serialize {} response", $action))?;
                            println!("{} resp: {res}", $action);
                        };
                        (no_param, $action: expr, $method: ident) => {
                            let res = client
                                .$method()
                                .await
                                .with_context(|| format!("client {} error", $action))?;
                            let res = serde_json::to_string(&res)
                                .with_context(|| format!("serialize {} response", $action))?;
                            println!("{} resp: {res}", $action);
                        };
                        (string, $action: expr, $method: ident) => {
                            let param =
                                context.as_str().context("param not valid: expect string")?;
                            let res = client
                                .$method(param)
                                .await
                                .with_context(|| format!("client {} error", $action))?;
                            let res = serde_json::to_string(&res)
                                .with_context(|| format!("serialize {} response", $action))?;
                            println!("{} resp: {res}", $action);
                        };
                    }
                    match action.as_str() {
                        PLAN_TASK_REQ => {
                            send_recv!(PLAN_TASK_REQ, plan_task);
                        }
                        START_TASK_JOB_REQ => {
                            send_recv!(START_TASK_JOB_REQ, start_task_job);
                        }
                        STOP_TASK_JOB_REQ => {
                            send_recv!(STOP_TASK_JOB_REQ, stop_task_job);
                        }
                        LIST_TASK_JOB_STATES_REQ => {
                            send_recv!(no_param, LIST_TASK_JOB_STATES_REQ, list_task_job_states);
                        }
                        ADD_AGENTS_REQ => {
                            send_recv!(ADD_AGENTS_REQ, add_agents);
                        }
                        DEL_AGENTS_REQ => {
                            send_recv!(DEL_AGENTS_REQ, del_agents);
                        }
                        LIST_AGENTS_REQ => {
                            send_recv!(no_param, LIST_AGENTS_REQ, list_agents);
                        }
                        CHECK_VALID_REQ => {
                            send_recv!(CHECK_VALID_REQ, check_valid);
                        }
                        GET_SAMPLES_REQ => {
                            send_recv!(string, GET_SAMPLES_REQ, get_samples);
                        }
                        TASK_PREVIEW_REQ => {
                            send_recv!(TASK_PREVIEW_REQ, task_preview);
                        }
                        TASK_JOB_DRAIN_REQ => {
                            send_recv!(no_param, TASK_JOB_DRAIN_REQ, drain_task_job);
                        }
                        s => {
                            println!("Unknown action: {s}");
                        }
                    }
                }
            }
            let interval = Duration::from_secs(1);
            let mut ticker = tokio::time::interval_at(Instant::now() + interval, interval);
            while cancel.run_until_cancelled(ticker.tick()).await.is_some() {
                match client
                    .heartbeat(&xnoded_id)
                    .await
                    .context("heartbeat error")
                {
                    Ok(hb_metrics) => {
                        let hb_metrics = serde_json::to_string(&hb_metrics)
                            .context("serialize hb metrics error")?;
                        println!("heartbeat metrics: {}", hb_metrics);
                    }
                    Err(e) => {
                        println!("{e:#}");
                    }
                }
            }
            anyhow::Ok(())
        }
    });

    tasks.spawn({
        let cancel = cancel.clone();
        async move {
            let _guard = cancel.drop_guard_ref();
            let mut stream = event_rx.into_stream();
            while let Some(batch) = cancel
                .run_until_cancelled(stream.next())
                .await
                .flatten()
                .transpose()?
            {
                let Some(record) = BatchIter::new(&batch)
                    .context("build batch iter error")?
                    .next()
                else {
                    continue;
                };
                if record.action == DROP_CONNECTION {
                    break;
                }
                arrow::util::pretty::print_batches(&[batch])
                    .context("print response batch error")?;
            }
            Ok(())
        }
    });

    if cancel
        .run_until_cancelled(tokio::signal::ctrl_c())
        .await
        .is_some()
    {
        cancel.cancel();
    }

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                println!("Task error: {e:#}");
            }
            Err(e) => {
                println!("Task panic: {e}");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use jsonwebtoken::{EncodingKey, Header};

    #[test]
    fn gen_agent_token() {
        static SECRET: &[u8] = &[
            126, 222, 130, 137, 43, 122, 41, 173, 144, 146, 116, 138, 153, 244, 251, 99, 50, 55,
            140, 238, 218, 232, 15, 161, 226, 54, 130, 40, 211, 234, 111, 171,
        ];
        for agent_id in 0..3 {
            let token = jsonwebtoken::encode(
                &Header::default(),
                &serde_json::json!({
                    "sub": agent_id,
                    "iat": Utc::now().timestamp_millis()
                }),
                &EncodingKey::from_secret(SECRET),
            )
            .unwrap();
            println!("{token}");
        }
    }
}
