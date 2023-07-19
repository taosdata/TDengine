use actix_web::{
    delete, get, patch, post,
    web::{Data, Json, Path, Query},
    HttpResponse, Responder,
};
use taos::Code;

use crate::serve::{
    controller::{
        agent::{AgentProps, AgentUpdates},
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
        Ok(agent) => HttpResponse::Ok().json(&agent),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::FAILED,
            message: err.to_string(),
        }),
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
        Ok(_) => HttpResponse::Ok().json(serde_json::Value::Null),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::FAILED,
            message: err.to_string(),
        }),
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
        Ok(agents) => HttpResponse::Ok()
            .append_header(("Count", agents.len()))
            .json(&agents),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::FAILED,
            message: err.to_string(),
        }),
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
#[get("/agents/{agent_id}/tasks")]
pub(super) async fn get_agent_tasks(
    task_store: Data<TaskControllerRef>,
    agent_id: Path<i64>,
) -> impl Responder {
    match task_store.get_tasks_of_agent(agent_id.into_inner()).await {
        Ok(agents) => HttpResponse::Ok()
            .append_header(("Count", agents.len()))
            .json(&agents),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::FAILED,
            message: err.to_string(),
        }),
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
        Ok(agents) => HttpResponse::Ok().json(&agents),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::FAILED,
            message: err.to_string(),
        }),
    }
}
