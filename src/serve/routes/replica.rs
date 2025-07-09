use actix_web::{
    HttpResponse, Responder, delete, post,
    web::{Data, Json, Path},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::serve::{
    controller::{TaskControllerRef, replica::ReplicaOpts},
    task::Failed,
};

/// Crete or update a replica monitor task
///
#[utoipa::path(
    tag = "replica",
    request_body = ReplicaOpts,
    responses(
			(status = 200, description = "Create replica monitor task", body = ReplicaOpts)
    ),
)]
#[post("/replicas")]
pub(crate) async fn start_replica_monitor(
    task_store: Data<TaskControllerRef>,
    body: Json<ReplicaOpts>,
) -> impl Responder {
    match task_store.start_replica_monitor(body.into_inner()).await {
        Ok(opts) => Ok(HttpResponse::Ok().json(&opts)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Stop a replica monitor task
///
#[utoipa::path(
    tag = "replica",
    responses(
			(status = 200, description = "Stop replica monitor task by id", body = ())
    ),
    params(
        ("id", description = "Replica id")
    ),
)]
#[post("/replicas/{id}")]
pub(crate) async fn stop_replica_monitor(
    id: Path<String>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    match task_store.stop_replica_monitor(&id.into_inner()).await {
        Ok(opts) => Ok(HttpResponse::Ok().json(opts)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

/// Replica action options
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
struct ReplicaActionOptions {
    new_databases_checking_interval: Option<u32>,
}
/// Replica action definition.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[schema(
    example = json!({
        "action": "stop",
    })
)]
struct ReplicaAction {
    /// start/restart/stop/delete
    action: String,
    options: ReplicaActionOptions,
}

/// Delete a replica monitor task
///
#[utoipa::path(
    tag = "replica",
    params(
        ("id", description = "Replica id")
    ),
    request_body = ReplicaAction,
    responses(
		(status = 200, description = "Stop replica monitor task by id", body = Option<ReplicaOpts>),
        (status = 404, description = "Replica monitor not found by id", body = Failed),
    ),
)]
#[delete("/replicas/{id}")]
pub(crate) async fn delete_replica_monitor(
    id: Path<String>,
    req: Json<ReplicaAction>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let req = req.into_inner();
    match req.action.as_str() {
        "stop" => {
            match task_store.remove_replica_monitor(&id.into_inner()).await {
                Ok(Some(opts)) => Ok(HttpResponse::Ok().json(&opts)),
                Ok(None) => Ok(HttpResponse::NotFound()
                    .json(Failed::from_error("No replica monitor found: {id}"))),
                Err(err) => Err(Failed::from_error(err)),
            }
        }
        "start" | "restart" | "update" => {
            match task_store
                .start_replica_monitor_by_id(
                    &id.into_inner(),
                    req.options.new_databases_checking_interval,
                )
                .await
            {
                Ok(Some(opts)) => Ok(HttpResponse::Ok().json(&opts)),
                Ok(None) => Ok(HttpResponse::NotFound()
                    .json(Failed::from_error("No replica monitor found: {id}"))),
                Err(err) => Err(Failed::from_error(err)),
            }
        }
        "delete" | "remove" => {
            match task_store.remove_replica_monitor(&id.into_inner()).await {
                Ok(Some(opts)) => Ok(HttpResponse::Ok().json(&opts)),
                Ok(None) => Ok(HttpResponse::NotFound()
                    .json(Failed::from_error("No replica monitor found: {id}"))),
                Err(err) => Err(Failed::from_error(err)),
            }
        }
        _ => Err(Failed::from_error(format!(
            "Action not supported: {}",
            req.action
        ))),
    }
}
