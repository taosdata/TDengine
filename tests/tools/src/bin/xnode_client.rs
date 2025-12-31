use std::{collections::HashMap, path::PathBuf};

use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use ha_core::{batch::BatchIter, consts::*};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

#[derive(Debug, Clone, clap::Parser)]
struct Args {
    #[arg(short, long, default_value = "http://localhost:6055")]
    addr: String,
    #[arg(short = 'f', long)]
    payload_file: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let payloads = tokio::fs::read_to_string(&args.payload_file)
        .await
        .context("read payload file error")?;
    let payloads = serde_json::from_str::<Vec<HashMap<String, serde_json::Value>>>(&payloads)
        .context("payload invalid json")?;

    let uri = args.addr.parse().context("invalid address")?;
    let channel = Channel::builder(uri)
        .connect()
        .await
        .context("connect grpc server error")?;

    let cancel = CancellationToken::new();

    let (event_tx, event_rx) = flume::bounded(1000);
    let client = ha_rpc_client::create_guest(channel, event_tx, cancel.child_token())
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
                        LIST_TASK_JOB_STATES_REQ => {
                            send_recv!(no_param, LIST_TASK_JOB_STATES_REQ, list_task_job_states);
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
                        s => {
                            println!("Unknown action: {s}");
                        }
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
