use actix_web::web::{Json, Query};
use actix_web::{get, post, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use utoipa::*;

use crate::serve::task::Failed;

#[derive(Debug, Default, Serialize, ToSchema)]
pub struct PointsHeader {
    name: String,          // header name
    default_value: String, // default value
    r#type: String,        // data type
    description: String,   // description
}

#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct OpcPoint {
    point_id: String,
    stable: String,
    tbname: String,
    enable: bool,
    value_col: Option<String>,
    value_transform: Option<String>,
    r#type: Option<String>,
    quality_col: Option<String>,
    ts_col: Option<String>,
    ts_transform: Option<String>,
    received_ts_col: Option<String>,
    received_ts_transform: Option<String>,
    tags: Option<Vec<OpcPointTag>>,
}

#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct OpcPointTag {
    seq: u8,
    name: String,
    r#type: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetPointsHeaderReq {
    task_id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AddPointReq {
    task_id: i64,
    point: OpcPoint,
}

#[utoipa::path(
    tag = "data sources",
    params(
        ("req" = PointReq, description = "include task id")
    ),
    responses(
        (
            status = 200,
            description = "get the OPC-UA/OPC-DA point configuration header from the CSV config file",
            body = Vec<PointsHeader>
        ),
        (
            status = 500,
            description = "failed to get point configs",
            body = Failed
        ),
    )
)]
#[get("/ds/in/opc/csv/points/header")]
pub async fn get_point_header(req: Query<GetPointsHeaderReq>) -> impl Responder {
    dbg!(req);
    Ok::<HttpResponse, Failed>(HttpResponse::Ok().json(vec![PointsHeader::default()]))
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (
            status = 200,
            description = "add one OPC-UA/OPC-DA point and its configuration to the current CSV config file",
            body = PointConfig
        ),
        (
            status = 500,
            description = "failed to add point config",
            body = Failed
        ),
    ),
    params(
        ("req" = AddPointReq, description = "task id")
    )
)]
#[post("/ds/in/opc/csv/points")]
pub async fn append_point(req: Json<AddPointReq>) -> impl Responder {
    dbg!(req);
    Ok::<HttpResponse, Failed>(HttpResponse::Ok().json(OpcPoint::default()))
}
