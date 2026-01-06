use std::{collections::HashMap, str::FromStr, sync::LazyLock};

use anyhow::Context;
use arrow_flight::error::FlightError;
use ha_core::types::*;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use taos::Dsn;
use taosx_core::{
    global::XNODE_HTTP_PORTS,
    plugins::transform::{modeler::ModeledJsonOutput, sample::DsSamples},
};

use crate::serve::{
    controller::{TaskControllerRef, agent::AgentToken},
    rpc::{
        FlightResult, XnodedId,
        utils::{check_taos_connectivity, decode_err, internal_err},
    },
};

type ApiResult<T> = std::result::Result<T, FlightError>;

pub async fn plan_task(context: &str) -> ApiResult<SplitJobResult> {
    let task = serde_json::from_str::<HaTask>(context)
        .context("deserialize xnode_plan_task body error")
        .map_err(decode_err)?;
    let split_task: SplitJobTask = task
        .try_into()
        .context("build split task req error")
        .map_err(decode_err)?;

    let from_driver = split_task.from.driver.as_str();
    let to_driver = split_task.to.driver.as_str();
    match (from_driver, to_driver) {
        ("tmq" | "sync", "taos")
        | ("tmq" | "sync", "local")
        | ("local", "taos" | "tmq")
        | ("taos", "taos")
        | ("taos", "csv")
        | ("taos", "parquet")
        | ("pi" | "pibackfill", "taos")
        | ("opc" | "opcda" | "opcua", "taos")
        | ("tmq", "mqtt")
        | ("sparkplugb", "taos")
        | ("influxdb", "taos")
        | ("opentsdb", "taos")
        | ("csv", "taos")
        | ("tmq", "kafka")
        | ("avevaHistorian", "taos")
        | ("orc", "taos")
        | ("mongodb", _)
        | ("mysql", _)
        | ("postgres", _)
        | ("oracle", _)
        | ("mssql", _) => {
            return Ok(SplitJobResult {
                from: serde_json::Value::String(split_task.from.to_string()),
                to: split_task.to.to_string(),
                parser: split_task.parser,
            });
        }
        _ => {}
    }
    let task_value = match (from_driver, to_driver) {
        ("kafka", _) => source_kafka::split_job::split_job(split_task)
            .await
            .context("kafka split job error")
            .map_err(internal_err)?,
        ("mqtt", _) => source_mqtt::split_job::split_job(split_task)
            .await
            .context("mqtt split job error")
            .map_err(internal_err)?,
        _ => {
            return Err(FlightError::DecodeError(format!(
                "unsupported split job from `{from_driver}` to `{to_driver}`"
            )));
        }
    };
    Ok(task_value)
}

pub async fn check_valid(context: &str) -> ApiResult<()> {
    let param: CheckValidParam = serde_json::from_str(context)
        .context("deserialize check_valid payload error")
        .map_err(decode_err)?;
    let from = Dsn::from_str(&param.from)
        .with_context(|| format!("check valid param `from` invalid dsn: {}", param.from))
        .map_err(decode_err)?;
    let to = Dsn::from_str(&param.to)
        .with_context(|| format!("param `to` invalid dsn: {}", param.to))
        .map_err(decode_err)?;

    taosx_task::validate::validate_dsn(&from)
        .await
        .ok()
        .map_err(internal_err)?;
    if to.driver.as_str() == "taos" {
        check_taos_connectivity(&to).await.map_err(internal_err)?;
    }

    Ok(())
}

pub async fn get_samples(context: &str) -> ApiResult<serde_json::Value> {
    let samples = taosx_task::sample::get_sample(context)
        .await
        .map_err(internal_err)?;
    serde_json::to_value(samples)
        .context("failed to serialize samples to json value")
        .map_err(internal_err)
}

pub fn get_x_http_port() -> ApiResult<Option<Vec<u16>>> {
    Ok(XNODE_HTTP_PORTS.get().cloned())
}

pub async fn task_preview(context: &str) -> ApiResult<Vec<ModeledJsonOutput>> {
    let param: TaskPreviewParam = serde_json::from_str(context)
        .context("deserialize task_preview payload error")
        .map_err(internal_err)?;
    let has_input = match &param.input {
        ha_core::types::Samples::Input(input) => !input.is_empty(),
        ha_core::types::Samples::Samples(samples) => !samples.is_empty(),
    };

    let samples = if !has_input {
        taosx_task::sample::get_sample(&param.from)
            .await
            .map_err(internal_err)?
    } else {
        serde_json::from_str(context)
            .context("preview input not valid ds samples")
            .map_err(decode_err)?
    };

    match samples {
        DsSamples::Simple(mut samples) => {
            samples.parser = serde_json::from_value(param.parser)
                .context("preview parser not valid")
                .map_err(decode_err)?;
            samples.transform(None)
        }
        DsSamples::MultiSchema(mut samples) => {
            samples.parser = serde_json::from_value(param.parser)
                .context("preview parser not valid")
                .map_err(decode_err)?;
            samples.transform(None)
        }
    }
    .context("transform samples error")
    .map_err(internal_err)
}

pub async fn start_task_job(
    controller: &TaskControllerRef,
    context: &str,
    xnoded_tx: flume::Sender<FlightResult>,
) -> ApiResult<()> {
    let task: StartTaskJobParam = serde_json::from_str(context)
        .context("deserialize xnode_start_task_job context error")
        .map_err(decode_err)?;

    controller
        .start_task(task.into(), xnoded_tx)
        .await
        .map_err(internal_err)?;

    Ok(())
}

pub async fn stop_task_job(controller: &TaskControllerRef, context: &str) -> ApiResult<()> {
    let task: StopTaskJobParam = serde_json::from_str(context)
        .context("deserialize xnode_stop_task_job context error")
        .map_err(decode_err)?;
    controller
        .stop_task(task.task_id, task.job_id)
        .await
        .map_err(internal_err)?;
    Ok(())
}

pub async fn drain(controller: &TaskControllerRef) -> ApiResult<()> {
    controller.stop_all_task().await.map_err(internal_err)?;
    Ok(())
}

pub fn heartbeat(xnoded_id: &XnodedId, context: &str) -> ApiResult<HeartbeatMetrics> {
    let hb_xnoded_id: XnodedId = serde_json::from_str(context)
        .context("Invalid heartbeat payload")
        .map_err(decode_err)?;
    if xnoded_id != &hb_xnoded_id {
        return Err(FlightError::ProtocolError(format!(
            "xnoded id miss match in heartbeat, expected {xnoded_id}, received: {hb_xnoded_id}"
        )));
    }
    Ok(system_metrics())
}

pub fn system_metrics() -> HeartbeatMetrics {
    static SYSINFO_KIND: LazyLock<RefreshKind> = LazyLock::new(|| {
        RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
            .with_memory(MemoryRefreshKind::nothing().with_ram())
    });
    let system = System::new_with_specifics(*SYSINFO_KIND);
    HeartbeatMetrics {
        cpu_cores: system.cpus().len(),
        cpu_usage: system.global_cpu_usage(),
        memory: system.total_memory(),
        used_memory: system.used_memory(),
        free_memory: system.free_memory(),
    }
}

pub async fn add_agents(
    controller: &TaskControllerRef,
    context: &str,
) -> ApiResult<AddAgentsResult> {
    let tokens: Vec<String> = serde_json::from_str(context)
        .context("deserialize add agents payload error")
        .map_err(internal_err)?;
    let mut res = HashMap::with_capacity(tokens.len());
    for token in tokens {
        let token_s = token.clone();
        let token = AgentToken::from(token);
        let agent_id = match token.jwt_decode() {
            Ok(claims) => claims.sub,
            Err(e) => {
                res.insert(token_s, e.to_string());
                continue;
            }
        };

        if controller.is_agent_exists(agent_id) {
            if controller.agent_alive(agent_id).await {
                res.insert(token_s, format!("agent {agent_id} is alive"));
                continue;
            }
            res.insert(token_s, format!("agent {agent_id} already exists"));
            continue;
        }

        controller.add_valid_agents(agent_id);
    }

    Ok(res)
}

pub async fn del_agents(
    controller: &TaskControllerRef,
    context: &str,
) -> ApiResult<DelAgentsResult> {
    let agent_ids: Vec<i64> = serde_json::from_str(context)
        .context("deserialize del agents payload error")
        .map_err(internal_err)?;
    let mut res = Vec::with_capacity(agent_ids.len());
    for agent_id in agent_ids {
        if !controller.is_agent_exists(agent_id) {
            res.push(DelAgentErrorStatus {
                id: agent_id,
                error: "agent id not found".to_string(),
            });
            continue;
        }
        if controller.agent_alive(agent_id).await {
            res.push(DelAgentErrorStatus {
                id: agent_id,
                error: "agent is alive".to_string(),
            });
            continue;
        }
        controller.del_valid_agent(agent_id);
    }

    Ok(res)
}

pub async fn list_agents(controller: &TaskControllerRef) -> ApiResult<ListAgentsResult> {
    let states = controller.list_agent_states().await;
    let mut context = Vec::with_capacity(states.len());
    for (id, state) in states {
        context.push(ListAgentStatesParam {
            id,
            state: state.into(),
        });
    }

    Ok(context)
}

pub async fn list_task_states(
    controller: &TaskControllerRef,
) -> ApiResult<ListTaskJobStatesResult> {
    let states = controller.list_task_states().await;
    let mut context = Vec::with_capacity(states.len());
    for ((task_id, job_id), state) in states {
        context.push(ListTaskJobStatesParam {
            task_id,
            job_id,
            state: state.into(),
        });
    }
    Ok(context)
}
