use actix_web::{
    delete, get, patch, post,
    web::{Data, Json, Path, Query},
    HttpResponse, Responder,
};

use crate::serve::{
    controller::{
        agent::{AgentActivityFilter, AgentProps, AgentUpdates},
        AgentFilter, TaskControllerRef,
    },
    task::Failed,
};

/// Create new agent with cluster id/ user id and privileges
#[utoipa::path(
    tag = "agents",
    request_body = AgentProps,
    responses(
        (status = 200, description = "Tasks count (deleted tasks will not be included by default)", body = AgentWithToken)
    )
)]
#[post("/agents")]
pub(super) async fn create_agent(
    task_store: Data<TaskControllerRef>,
    agent: Json<AgentProps>,
) -> impl Responder {
    match task_store.create_agent(agent.into_inner()).await {
        Ok(agent) => Ok(HttpResponse::Ok().json(&agent)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Create new agent with cluster id / user id and privileges
#[utoipa::path(
    tag = "agents",
    responses(
        (status = 200, description = "Deleted", body = ()),
        (status = 500, description = "Error", body = Failed)
    )
)]
#[delete("/agents/{agent_id}")]
pub(super) async fn delete_agent(
    task_store: Data<TaskControllerRef>,
    agent_id: Path<i64>,
) -> impl Responder {
    match task_store.delete_agent(agent_id.into_inner()).await {
        Ok(_) => Ok(HttpResponse::Ok().json(serde_json::Value::Null)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// List agents with specified `cluster_id` and `user_id`
///
#[utoipa::path(
    tag = "agents",
    responses(
        (status = 200, description = "List current agents items", body = [Agent])
    ),
    params(
        AgentFilter,
    )
)]
#[get("/agents")]
pub(super) async fn get_agents(
    task_store: Data<TaskControllerRef>,
    filter: Query<AgentFilter>,
) -> impl Responder {
    match task_store.get_agents(filter.into_inner()).await {
        Ok(agents) => Ok(HttpResponse::Ok()
            .append_header(("Count", agents.len()))
            .json(&agents)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Get agent by id.
///
#[utoipa::path(
    tag = "agents",
    responses(
        (status = 200, description = "List current agents items", body = Agent)
    )
)]
#[get("/agents/{agent_id}")]
pub(super) async fn get_agent_by_id(
    task_store: Data<TaskControllerRef>,
    agent_id: Path<i64>,
) -> impl Responder {
    match task_store.get_agent_by_id(agent_id.into_inner()).await {
        Ok(agents) => Ok(HttpResponse::Ok().json(&agents)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// List agents with specified `cluster_id` and `user_id`
///
#[utoipa::path(
    tag = "agents",
    responses(
        (status = 200, description = "List current agents items", body = [TaskDetail])
    ),
    params(
        AgentFilter,
    )
)]
#[get("/agents/{agent_id}/tasks")]
pub(super) async fn get_agent_tasks(
    task_store: Data<TaskControllerRef>,
    agent_id: Path<i64>,
) -> impl Responder {
    match task_store.get_tasks_of_agent(agent_id.into_inner()).await {
        Ok(agents) => Ok(HttpResponse::Ok()
            .append_header(("Count", agents.len()))
            .json(&agents)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Get agent activities by id
///
#[utoipa::path(
    tag = "agents",
    responses(
        (status = 200, description = "List current agents items", body = [Activity])
    ),
    params(AgentActivityFilter)
)]
#[get("/agents/{agent_id}/activities")]
pub(super) async fn get_agent_activities(
    task_store: Data<TaskControllerRef>,
    agent_id: Path<i64>,
    filter: Query<AgentActivityFilter>,
) -> impl Responder {
    match task_store
        .agent_activities(agent_id.into_inner(), &filter)
        .await
    {
        Ok(agents) => Ok(HttpResponse::Ok()
            .append_header(("Count", agents.len()))
            .json(&agents)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Update an agent by id but and get new token.
///
#[utoipa::path(
    tag = "agents",
    request_body = AgentUpdates,
    responses(
        (status = 200, description = "List current agents items", body = [AgentWithToken])
    )
)]
#[patch("/agents/{agent_id}")]
pub(super) async fn update_agent(
    task_store: Data<TaskControllerRef>,
    agent_id: Path<i64>,
    body: Json<AgentUpdates>,
) -> impl Responder {
    match task_store
        .update_agent(agent_id.into_inner(), body.into_inner())
        .await
    {
        Ok(agents) => Ok(HttpResponse::Ok().json(&agents)),
        Err(err) => Err(Failed::from_error(err)),
    }
}
