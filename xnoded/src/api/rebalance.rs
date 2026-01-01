use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;

use crate::{
    api::{Error, RawResult, call},
    controller::Controller,
};

pub async fn rebalance_manual(
    State(controller): State<Arc<Controller>>,
    WithRejection(Path((task_id, job_id, xnode_id)), _): WithRejection<
        Path<(i64, i64, i32)>,
        Error,
    >,
) -> RawResult<()> {
    call!(
        spawn,
        controller,
        rebalance_manually(task_id, job_id, xnode_id)
    )
}

#[derive(Debug, serde::Deserialize)]
pub struct AutoRebalanceParam {
    tid: i64,
    jid: Option<i64>,
}

/// 新节点加入后，迁移部分节点到新节点
pub async fn rebalance_auto(
    State(controller): State<Arc<Controller>>,
    WithRejection(Json(param), _): WithRejection<Json<Vec<AutoRebalanceParam>>, Error>,
) -> RawResult<()> {
    let param = param
        .into_iter()
        .map(|v| (v.tid, v.jid))
        .collect::<Vec<_>>();
    call!(spawn, controller, rebalance_auto(&param))
}
