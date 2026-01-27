use actix_web::{
    HttpRequest,
    web::{self, Json},
};
use anyhow::Context;
use ha_core::types::GetSamplesParam;
use taosx_utils::dsn::{json_to_dsn, parse_simple_params};
use tokio_util::sync::CancellationToken;

use crate::{
    Args,
    x_api::{
        Error, get_dsn, get_one_client,
        types::{ApiCheckValidParam, ApiGetSampleParam},
    },
};

type JsonResult<T> = std::result::Result<Json<T>, Error>;

pub async fn validate(
    args: web::Data<Args>,
    Json(param): Json<ApiCheckValidParam>,
    req: HttpRequest,
) -> JsonResult<serde_json::Value> {
    let dsn = get_dsn(&args, &req).await?;
    let cancel = CancellationToken::new();
    let _guard = cancel.drop_guard_ref();
    let client = get_one_client(&dsn, param.via, cancel.clone())
        .await?
        .context("no available xnode found")?;

    match client.check_valid(&param.try_into()?).await {
        Ok(res) => Ok(Json(res)),
        Err(e) => Ok(Json(serde_json::json!({
            "valid": false,
            "support": false,
            "message": format!("{:#}", anyhow::Error::new(e))
        }))),
    }
}

pub async fn get_sample(
    args: web::Data<Args>,
    Json(param): Json<ApiGetSampleParam>,
    req: HttpRequest,
) -> JsonResult<serde_json::Value> {
    let from = json_to_dsn(&param.dsn)?;
    let via = parse_simple_params::<i64>(&from, "agent")?;
    let dsn = get_dsn(&args, &req).await?;
    let cancel = CancellationToken::new();
    let _guard = cancel.drop_guard_ref();
    let client = get_one_client(&dsn, via, cancel.clone())
        .await?
        .context("no available xnode found")?;
    let from = json_to_dsn(&param.dsn).context("invalid `from` param")?;
    let samples = client
        .get_samples(&GetSamplesParam {
            from: from.to_string(),
            via,
        })
        .await
        .context("get sample error")?;

    Ok(Json(samples))
}
