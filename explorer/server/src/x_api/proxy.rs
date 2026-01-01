use actix_web::{
    HttpRequest, HttpResponse,
    web::{self},
};
use anyhow::Context;

use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    Args, proxy,
    sql::query,
    x_api::{Error, get_dsn, get_one_client, types::Xnode},
};

type Result<T> = std::result::Result<T, Error>;

pub async fn x_proxy(
    args: web::Data<Args>,
    client: web::Data<reqwest::Client>,
    path: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
) -> Result<HttpResponse> {
    let url = get_x_url(&args, &path.into_inner(), &req)
        .await?
        .context("no available x url found")?;

    let resp = proxy(req, payload, &client, &url, None)
        .await
        .map_err(|e| anyhow::anyhow!("x api proxy error: {e}"))?;
    Ok(resp)
}

pub async fn get_x_url(args: &Args, api: &str, req: &HttpRequest) -> Result<Option<String>> {
    let dsn = get_dsn(args, req).await?;
    let cancel = CancellationToken::new();
    let (client, _rx) = get_one_client(&dsn, cancel)
        .await?
        .context("no available xnode found")?;

    let mut ports = client
        .get_x_http_port()
        .await
        .context("Failed to get x http port")?
        .context("x http port not set")?;
    let port = ports.pop().context("x http port not set")?;

    let mut xnodes = query::<Xnode>(&dsn, "SHOW XNODES")
        .await
        .context("show xnodes error")?;
    if let Some(xnode) = xnodes.pop() {
        let url = if xnode.url.starts_with("http") {
            xnode.url.to_string()
        } else {
            format!("http://{}", xnode.url)
        };
        let mut url = Url::parse(&url).context("x api not invalid url")?;
        url.set_port(Some(port))
            .map_err(|_| anyhow::anyhow!("set x url port error"))?;
        return Ok(Some(format!("{}{api}?{}", url, req.query_string())));
    }
    Ok(None)
}
