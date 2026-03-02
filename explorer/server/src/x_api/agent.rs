use actix_web::{
    HttpRequest,
    web::{self, Json, Path},
};
use anyhow::Context;
use ha_core::{activity::AgentStatus, consts::AGENT_ACTIVITIES_STABLE};
use http::StatusCode;

use crate::{
    Args,
    sql::{exec, query, query_one},
    x_api::{
        JsonResult, JsonStatusResult, get_dsn,
        types::{ActivityLog, AgentRecord, JobRecord, TaskRecord},
        x_addrs,
    },
};

pub async fn get_agents(args: web::Data<Args>, req: HttpRequest) -> JsonResult<Vec<AgentRecord>> {
    let dsn = get_dsn(&args, &req).await?;
    let mut agents = query::<AgentRecord>(&dsn, "SHOW XNODE AGENTS").await?;
    for agent in agents.iter_mut() {
        if agent.status.is_none() {
            agent.status = Some(AgentStatus::Idle)
        }
    }
    Ok(Json(agents))
}

pub async fn get_agent(
    args: web::Data<Args>,
    req: HttpRequest,
    agent_id: Path<i64>,
) -> JsonStatusResult<Option<AgentRecord>> {
    let agent_id = agent_id.into_inner();
    let dsn = get_dsn(&args, &req).await?;
    let sql = format!("SHOW XNODE AGENTS WHERE ID = {agent_id}");
    let agent = query_one::<AgentRecord>(&dsn, &sql).await?;
    let agent = agent.map(|mut v| {
        if v.status.is_none() {
            v.status = Some(AgentStatus::Idle)
        }
        v
    });
    match agent {
        Some(agent) => Ok((Json(Some(agent)), StatusCode::OK)),
        None => Ok((Json(None), StatusCode::NOT_FOUND)),
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct AgentParam {
    name: String,
}

#[derive(Debug, serde::Serialize)]
pub struct AddAgentResult {
    pub x_addrs: String,
    #[serde(flatten)]
    pub agent: AgentRecord,
}

pub async fn add_agent(
    args: web::Data<Args>,
    req: HttpRequest,
    Json(agent): Json<AgentParam>,
) -> JsonResult<AddAgentResult> {
    let name = agent.name;
    let dsn = get_dsn(&args, &req).await?;
    exec(
        &dsn,
        &format!(
            "CREATE XNODE AGENT '{name}' WITH STATUS '{}'",
            AgentStatus::Idle
        ),
    )
    .await
    .with_context(|| format!("create agent `{name}` error"))?;

    let sql = format!("SHOW XNODE AGENTS WHERE NAME = '{name}'");
    let agent = query_one::<AgentRecord>(&dsn, &sql)
        .await?
        .with_context(|| format!("agent `{name}` not found"))?;
    let x_addrs = x_addrs(&args, &req).await?.join(",");
    Ok(Json(AddAgentResult { x_addrs, agent }))
}

pub async fn del_agent(
    args: web::Data<Args>,
    req: HttpRequest,
    agent_id: Path<i64>,
) -> JsonResult<()> {
    let agent_id = agent_id.into_inner();

    let dsn = get_dsn(&args, &req).await?;
    // 查询是否有任务使用 agent
    let sql = format!("SHOW XNODE TASKS WHERE VIA = {agent_id}");
    let task_using = query::<TaskRecord>(&dsn, &sql)
        .await
        .context("show xnode tasks error")?
        .into_iter()
        .any(|v| v.via.is_some_and(|v| v == agent_id) && v.status.is_some_and(|v| v.is_running()));
    if task_using {
        return Err(anyhow::anyhow!("should delete associated tasks before delete agent").into());
    }
    let sql = format!("SHOW XNODE JOBS WHERE VIA = {agent_id}");
    let job_using = query::<JobRecord>(&dsn, &sql)
        .await
        .context("show xnode jobs error")?
        .into_iter()
        .any(|v| v.via.is_some_and(|v| v == agent_id) && v.status.is_some_and(|v| v.is_running()));
    if job_using {
        return Err(anyhow::anyhow!("should delete associated jobs before delete agent").into());
    }

    // 删除数据库中的 agent
    exec(&dsn, &format!("DROP XNODE AGENT {agent_id}"))
        .await
        .with_context(|| format!("drop agent {agent_id}"))?;

    Ok(Json(()))
}

pub async fn edit_agent(
    args: web::Data<Args>,
    req: HttpRequest,
    agent_id: Path<i64>,
    Json(agent): Json<AgentParam>,
) -> JsonResult<Option<AgentRecord>> {
    let agent_id = agent_id.into_inner();
    let name = agent.name;
    let dsn = get_dsn(&args, &req).await?;
    exec(
        &dsn,
        &format!("ALTER XNODE AGENT {agent_id} WITH NAME '{name}'"),
    )
    .await
    .with_context(|| format!("alter agent {agent_id}"))?;

    let sql = format!("SHOW XNODE AGENTS WHERE ID = {agent_id}");
    let agents = query_one::<AgentRecord>(&dsn, &sql).await?;
    let agent = agents.map(|mut v| {
        if v.status.is_none() {
            v.status = Some(AgentStatus::Idle)
        }
        v
    });
    Ok(Json(agent))
}

pub async fn agent_activities(
    args: web::Data<Args>,
    req: HttpRequest,
    agent_id: Path<i64>,
) -> JsonResult<Vec<ActivityLog>> {
    let mut dsn = get_dsn(&args, &req).await?;
    dsn.subject = Some("log".to_string());

    let agent_id = agent_id.into_inner();
    let sql = format!(
        "SELECT \
        `agent_id as `id`, `ts` as `at`, `level`, `status`, `activity` \
        FROM `{AGENT_ACTIVITIES_STABLE}_agent_{agent_id}` \
        ORDER BY ts DESC limit 10"
    );
    let activities = match query::<ActivityLog>(&dsn, &sql).await {
        Ok(activities) => activities,
        Err(e) => {
            tracing::error!("query agent activities error: {}", e);
            vec![]
        }
    };

    Ok(Json(activities))
}
