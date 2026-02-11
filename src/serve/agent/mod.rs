use std::collections::HashSet;

use actix_web::{
    Error, HttpRequest, HttpResponse, Responder, delete, get, patch, post, rt,
    web::{Data, Json, Path, Payload, Query},
};
use actix_ws::{CloseCode, CloseReason, Session};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::serve::{
    controller::{
        AgentFilter, TaskControllerRef,
        agent::{AgentActivityFilter, AgentProps, AgentUpdates},
    },
    task::Failed,
};

use super::metrics::ws::echo_heartbeat_ws;

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

#[instrument(skip_all)]
pub(crate) async fn send_all_agents_activities(
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse, Error> {
    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;
    // spawn websocket handler (and don't await it) so that the response is returned immediately
    let cancel = CancellationToken::new();
    rt::spawn(send_all_agents_activities_ws(
        req,
        session.clone(),
        cancel.clone(),
    ));
    rt::spawn(echo_heartbeat_ws(session, msg_stream, cancel));
    Ok(res)
}

async fn send_all_agents_activities_ws(
    req: HttpRequest,
    mut session: Session,
    cancel: CancellationToken,
) {
    let task_store = match req.app_data::<Data<TaskControllerRef>>() {
        Some(store) => store,
        None => {
            let reason = Some(CloseReason {
                code: CloseCode::Abnormal,
                description: Some("Failed to get task store".to_string()),
            });
            let _ = session.close(reason).await;
            return;
        }
    };

    let filter = AgentFilter::default();
    let agents = match task_store.get_agents(filter).await {
        Ok(agents) => agents,
        Err(err) => {
            tracing::error!("agent-ws failed to get agents: {:#}", err);
            return;
        }
    };
    let agent_ids: HashSet<_> = agents.into_iter().map(|t| t.id).collect();

    // send the latest 5 activities for each agent
    match task_store.all_agents_activities().await {
        Ok(activities) => {
            for activity in activities.into_iter() {
                // only send activities for tasks in the current cluster
                if agent_ids.contains(&activity.id)
                    && let Err(err) = session
                        .text(serde_json::to_string(&activity).unwrap())
                        .await
                {
                    tracing::info!("agent-ws session closed: {:#}", err);
                    break;
                }
            }
        }
        Err(err) => {
            tracing::error!("agent-ws failed to send latest agent activities: {:#}", err);
        }
    }

    let scheduler = task_store.scheduler.clone();
    // get notify channel
    let notify_channel = scheduler.notify_channel();
    let mut rx = notify_channel;
    'loop_send: loop {
        let Some(res) = cancel.run_until_cancelled(rx.recv()).await else {
            break;
        };
        match res {
            Ok(notify) => match notify {
                crate::serve::scheduler::SchedulerNotify::TaskActivity(_) => {}
                crate::serve::scheduler::SchedulerNotify::AgentActivity(activity) => {
                    if let Err(err) = session
                        .text(serde_json::to_string(&activity).unwrap())
                        .await
                    {
                        tracing::info!("agent-ws session closed: {:#}", err);
                        break 'loop_send;
                    }
                }
            },
            Err(err) => match err {
                tokio::sync::broadcast::error::RecvError::Closed => break,
                tokio::sync::broadcast::error::RecvError::Lagged(_) => {
                    continue;
                }
            },
        }
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
