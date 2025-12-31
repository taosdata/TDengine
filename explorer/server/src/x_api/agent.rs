use actix_web::{
    HttpRequest,
    web::{self, Json},
};

use crate::{
    Args,
    x_api::{JsonResult, types::Agent},
};

pub async fn get_agents(_args: web::Data<Args>, _req: HttpRequest) -> JsonResult<Vec<Agent>> {
    Ok(Json(vec![]))
}
