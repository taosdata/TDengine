use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use ha_core::types::ListTaskJobStatesResult;

use crate::{
    api::{CancelledSnafu, Data, Error, JsonResult, RawResult, call},
    controller::Controller,
};

#[derive(Debug, serde::Deserialize)]
pub struct TaskConfigParam {
    pub xnode_id: Option<i32>,
    pub from: String,
    pub to: String,
    pub parser: Option<String>,
}

pub async fn check_task(
    State(controller): State<Arc<Controller>>,
    WithRejection(Json(param), _): WithRejection<Json<TaskConfigParam>, Error>,
) -> RawResult<()> {
    call!(controller, check_task(param.xnode_id, param))
}

pub async fn start_task(
    State(controller): State<Arc<Controller>>,
    WithRejection(Path(task_id), _): WithRejection<Path<i64>, Error>,
    WithRejection(Json(param), _): WithRejection<Json<TaskConfigParam>, Error>,
) -> RawResult<()> {
    call!(spawn, controller, plan_start_task(task_id, &param))
}

pub async fn stop_task(
    State(controller): State<Arc<Controller>>,
    WithRejection(Path(task_id), _): WithRejection<Path<i64>, Error>,
) -> RawResult<()> {
    call!(spawn, controller, stop_by_task(task_id, false))
}

pub async fn drop_task(
    State(controller): State<Arc<Controller>>,
    WithRejection(Path(task_id), _): WithRejection<Path<i64>, Error>,
) -> RawResult<()> {
    call!(spawn, controller, drop_task(task_id))
}

pub async fn task_status(
    State(controller): State<Arc<Controller>>,
    WithRejection(Path(task_id), _): WithRejection<Path<i64>, Error>,
) -> JsonResult<ListTaskJobStatesResult> {
    call!(json, controller, task_status(task_id))
}
