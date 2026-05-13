use std::{str::FromStr, sync::LazyLock};

use anyhow::Context;
use arrow_flight::error::FlightError;
use ha_core::{jwt::agent::AgentToken, types::*};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use taos::Dsn;
use taosx_core::{
    dsv::DataSourceValidation,
    global::XNODE_HTTP_PORTS,
    plugins::transform::{modeler::ModeledJsonOutput, sample::DsSamples},
};

use crate::serve::{
    controller::{AgentAction, TaskControllerRef},
    rpc::{
        FlightResult, XnodedId,
        utils::{check_taos_connectivity, decode_err, internal_err},
    },
};

type ApiResult<T> = std::result::Result<T, FlightError>;

pub async fn plan_task(controller: &TaskControllerRef, context: &str) -> ApiResult<SplitJobResult> {
    let task = serde_json::from_str::<HaTask>(context)
        .context("deserialize xnode_plan_task body error")
        .map_err(decode_err)?;
    match task.via {
        Some(via) => controller
            .scheduler
            .split_task_via_agent(via, task)
            .await
            .map_err(internal_err),
        None => taosx_task::split_job::plan_task(task)
            .await
            .map_err(internal_err),
    }
}

pub async fn check_valid(
    controller: &TaskControllerRef,
    context: &str,
) -> ApiResult<DataSourceValidation> {
    let param: CheckValidParam = serde_json::from_str(context)
        .context("deserialize check_valid payload error")
        .map_err(decode_err)?;
    let from = Dsn::from_str(&param.from)
        .with_context(|| format!("check valid param `from` invalid dsn: {}", param.from))
        .map_err(decode_err)?;
    let to = Dsn::from_str(&param.to)
        .with_context(|| format!("param `to` invalid dsn: {}", param.to))
        .map_err(decode_err)?;

    let res = match param.via {
        Some(via) => controller.validate_dsn_via_agent(via, &from).await,
        None => taosx_task::validate::validate_dsn(&from).await,
    };

    res.ok().map_err(internal_err)?;

    if matches!(
        to.driver.as_str(),
        "taos" | "ws" | "wss" | "http" | "https" | "taosws" | "taoswss"
    ) {
        check_taos_connectivity(&to).await.map_err(internal_err)?;
    }

    Ok(res)
}

pub async fn get_samples(
    controller: &TaskControllerRef,
    context: &str,
) -> ApiResult<serde_json::Value> {
    let param: GetSamplesParam = serde_json::from_str(context)
        .context("deserialize get_samples payload error")
        .map_err(decode_err)?;
    let res = match param.via {
        Some(via) => controller.get_sample_via_agent(via, param.from).await,
        None => taosx_task::sample::get_sample(param.from).await,
    };
    let samples = res.map_err(internal_err)?;
    serde_json::to_value(samples)
        .context("samples not valid json value")
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

    let task = task.try_into().map_err(internal_err)?;
    controller
        .start_task(task, xnoded_tx)
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

pub async fn add_agents(controller: &TaskControllerRef, context: &str) -> ApiResult<()> {
    let tokens: Vec<String> = serde_json::from_str(context)
        .context("deserialize add agents payload error")
        .map_err(internal_err)?;
    for token in tokens {
        let token = AgentToken::from(token);
        let agent_id = match token.jwt_decode() {
            Ok(claims) => claims.sub,
            Err(e) => {
                tracing::error!("add agent error: {e:#}");
                continue;
            }
        };

        if controller.is_agent_exists(agent_id) {
            if controller.agent_alive(agent_id).await {
                tracing::error!(agent_id, "agent is already alive");
                continue;
            }
            tracing::error!(agent_id, "agent already exists");
            continue;
        }

        controller.add_valid_agents(agent_id);
    }

    Ok(())
}

pub async fn del_agents(controller: &TaskControllerRef, context: &str) -> ApiResult<()> {
    let agent_ids: Vec<i64> = serde_json::from_str(context)
        .context("deserialize del agents payload error")
        .map_err(internal_err)?;
    for agent_id in agent_ids {
        if !controller.is_agent_exists(agent_id) {
            tracing::error!(agent_id, "agent id not found");
            continue;
        }
        if controller.agent_alive(agent_id).await {
            tracing::error!(agent_id, "agent is alive");
        }
        controller.del_valid_agent(agent_id);
        if let Err(e) = controller.stop_task_by_agent(agent_id).await {
            tracing::error!(agent_id, "stop task after agent delete error: {e:#}");
        }
        controller
            .push_agent_action(agent_id, AgentAction::Exit)
            .await;
    }

    Ok(())
}

pub async fn list_agents(controller: &TaskControllerRef) -> ApiResult<ListAgentsResult> {
    let states = controller.list_agent_states().await;
    let mut context = Vec::with_capacity(states.len());
    for (id, state) in states {
        context.push(ListAgentStatusResult {
            id,
            status: state.into(),
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
        context.push(ListTaskJobStates {
            task_id,
            job_id,
            state: state.into(),
        });
    }
    Ok(context)
}
