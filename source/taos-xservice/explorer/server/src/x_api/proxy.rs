use actix_web::{
    HttpRequest, HttpResponse,
    web::{self},
};
use anyhow::Context;

use crate::{
    Args, proxy,
    x_api::{Error, get_x_url},
};

type Result<T> = std::result::Result<T, Error>;

pub async fn x_proxy(
    args: web::Data<Args>,
    client: web::Data<reqwest::Client>,
    path: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
) -> Result<HttpResponse> {
    let url = get_x_url(&args, &req, &path.into_inner())
        .await?
        .context("no available x url found")?;

    let resp = proxy(req, payload, &client, &url, None)
        .await
        .map_err(|e| anyhow::anyhow!("x api proxy error: {e}"))?;
    Ok(resp)
}
