use actix_web::{
    HttpRequest,
    web::{self, Json},
};
use anyhow::Context;
use taosx_utils::dsn::json_to_dsn;
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
    let (client, _event_rx) = get_one_client(&dsn, cancel.clone())
        .await?
        .context("no available xnode found")?;

    match client.check_valid(&param.try_into()?).await {
        Ok(_) => Ok(Json(serde_json::json!({
            "valid": true,
            "support": true
        }))),
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
    let dsn = get_dsn(&args, &req).await?;
    let cancel = CancellationToken::new();
    let (client, _event_rx) = get_one_client(&dsn, cancel.clone())
        .await?
        .context("no available xnode found")?;
    let from = json_to_dsn(&param.dsn).context("invalid `from` param")?;
    let samples = client
        .get_samples(&from.to_string())
        .await
        .context("get sample error")?;
    let res = match from.driver.as_str() {
        "sparkplugb" => serde_json::json!({
            "samples": samples
        }),
        _ => serde_json::json!({
            "input": samples
        }),
    };
    Ok(Json(res))
}
