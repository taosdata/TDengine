use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use axum_extra::extract::WithRejection;

use crate::{
    api::{Data, Error, JsonResult, RawResult, call},
    controller::Controller,
};

#[derive(Debug, serde::Deserialize)]
pub struct CreateXnode {
    id: i32,
    url: String,
}

pub async fn create_xnode(
    State(controller): State<Arc<Controller>>,
    WithRejection(Json(CreateXnode { id, url }), _): WithRejection<Json<CreateXnode>, Error>,
) -> RawResult<()> {
    call!(spawn, controller, create_xnode(id, &url))
}

#[derive(Debug, serde::Deserialize)]
pub struct DeleteXnodeParams {
    #[serde(default)]
    force: bool,
}
pub async fn delete_xnode(
    State(controller): State<Arc<Controller>>,
    WithRejection(Path(id), _): WithRejection<Path<i32>, Error>,
    WithRejection(Query(params), _): WithRejection<Query<DeleteXnodeParams>, Error>,
) -> RawResult<()> {
    call!(spawn, controller, delete_xnode(id, params.force))
}

pub async fn drain_xnode(
    State(controller): State<Arc<Controller>>,
    WithRejection(Path(id), _): WithRejection<Path<i32>, Error>,
) -> RawResult<()> {
    call!(spawn, controller, drain_xnode(id))
}

pub async fn xnode_status(
    State(controller): State<Arc<Controller>>,
    WithRejection(Path(id), _): WithRejection<Path<i32>, Error>,
) -> JsonResult<serde_json::Value> {
    Ok(Data(serde_json::json!({
        "status": controller.xnode_status(id)?
    })))
}
