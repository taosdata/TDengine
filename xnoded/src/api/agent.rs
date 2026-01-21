use std::{collections::HashMap, sync::Arc};

use axum::{
    Json,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use ha_core::activity::AgentStatus;

use crate::{
    api::{CancelledSnafu, Data, Error, JsonResult, RawResult, call},
    controller::Controller,
};

#[derive(Debug, serde::Deserialize)]
pub struct AddAgentParam {
    token: String,
}

pub async fn add_agent(
    State(controller): State<Arc<Controller>>,
    WithRejection(Json(param), _): WithRejection<Json<AddAgentParam>, Error>,
) -> RawResult<()> {
    call!(spawn, controller, add_agent(&param.token))
}

pub async fn del_agent(
    State(controller): State<Arc<Controller>>,
    WithRejection(Path(id), _): WithRejection<Path<i64>, Error>,
) -> RawResult<()> {
    call!(spawn, controller, del_agent(id))
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AgentStatusResult {
    pub xnode_id: i32,
    pub status: AgentStatus,
}

pub async fn get_agent(
    State(controller): State<Arc<Controller>>,
) -> JsonResult<HashMap<i64, Vec<AgentStatusResult>>> {
    call!(json, controller, agent_status())
}
