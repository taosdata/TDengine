use actix_web::{
    get,
    web::{Data, Path},
    HttpResponse, Responder,
};
use taos::Code;

use crate::serve::{controller::TaskControllerRef, task::Failed};

/// Get connector transferred metrics of a cluster by `id`.
///
#[utoipa::path(
    tag = "cluster",
    responses(
			(status = 200, description = "List connector transferred metrics", body = ConnectorTransferred)
    ),
    params(
        ("id", description = "Unique cluster id")
    ),
)]
#[get("/cluster/{id}/transferred")]
pub(super) async fn get_cluster_connector_transferred(
    id: Path<i64>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let id = id.into_inner();

    match task_store.cluster_transferred(id).await {
        Ok(offsets) => HttpResponse::Ok().json(&offsets),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: Code::Failed,
            message: err.to_string(),
        }),
    }
}
